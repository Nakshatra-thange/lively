import logging
import random

import httpx
from redis.exceptions import RedisError

from .. import metrics
from ..config import USER_AGENT
from ..redisx import get_redis
from .breaker import Breaker
from .errors import CircuitOpen, PolitenessUnavailable, RobotsDisallowed
from .guard import ensure_public
from .limiter import Limiter
from .robots import Robots

log = logging.getLogger("lively.polite")


def _retry_after(response):
    try:
        return min(300.0, float(response.headers.get("retry-after")))
    except (TypeError, ValueError):
        return None


class PoliteTransport(httpx.AsyncBaseTransport):
    def __init__(self, inner, limiter=None, breaker=None, robots=None, guard=None):
        self.inner = inner
        self.guard = guard
        self.limiter = limiter or Limiter()
        self.breaker = breaker or Breaker()
        self.robots = robots or Robots()

    async def handle_async_request(self, request):
        host = request.url.host

        # SSRF guard first (also runs per redirect hop, since httpx sends each hop
        # through this transport), so nothing, not even robots.txt, goes to a
        # non-public address.
        await (self.guard or ensure_public)(host)

        if not await self.robots.allowed(request.url):
            await metrics.count_blocked("robots", host)
            raise RobotsDisallowed(f"robots.txt disallows {request.url.path}")

        try:
            await self.breaker.before(host)
        except CircuitOpen:
            await metrics.count_blocked("circuit", host)
            raise

        await self.limiter.acquire(host)

        try:
            response = await self.inner.handle_async_request(request)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.ProtocolError) as exc:
            await metrics.count_request(host, True)
            try:
                await self.breaker.failure(host)
            except RedisError as redis_exc:
                raise PolitenessUnavailable(
                    f"could not record failure for {host}: {redis_exc}"
                ) from exc
            raise

        status = response.status_code
        failed = status == 429 or status >= 500
        try:
            if status == 429:
                r = get_redis()
                count = await r.incr(f"pen:{host}:n")
                await r.expire(f"pen:{host}:n", 600)
                wait = _retry_after(response)
                if wait is None:
                    wait = min(60.0, 2.0 ** min(max(count, 1), 6)) + random.random()
                await self.limiter.penalize(host, wait)
            elif status >= 500:
                count = await self.breaker.failure(host)
                wait = _retry_after(response)
                if wait is None:
                    wait = min(60.0, 2.0 ** min(max(count, 1), 6)) + random.random()
                await self.limiter.penalize(host, wait)
            else:
                await self.breaker.success(host)
        except RedisError as exc:
            if failed:
                # Fail closed: the host asked us to back off and we cannot record it.
                await response.aclose()
                raise PolitenessUnavailable(f"could not record backoff for {host}: {exc}") from exc
            # A good response is kept; leaving breaker counters un-reset is conservative.
            log.warning("could not reset circuit breaker for %s: %r", host, exc)
        await metrics.count_request(host, failed)
        return response

    async def aclose(self):
        await self.inner.aclose()
        await self.robots.close()


def make_polite_client(concurrency):
    inner = httpx.AsyncHTTPTransport(
        limits=httpx.Limits(
            max_connections=concurrency * 2,
            max_keepalive_connections=concurrency,
        ),
        retries=0,
    )
    return httpx.AsyncClient(
        transport=PoliteTransport(inner),
        timeout=httpx.Timeout(connect=6.0, read=15.0, write=10.0, pool=30.0),
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.5",
            "Accept-Language": "en",
        },
        follow_redirects=True,
        max_redirects=6,
        trust_env=False,
    )