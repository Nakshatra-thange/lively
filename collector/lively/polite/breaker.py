from redis.exceptions import RedisError

from ..config import (
    BREAKER_MAX_OPEN_SECONDS,
    BREAKER_OPEN_SECONDS,
    BREAKER_THRESHOLD,
    BREAKER_WINDOW,
)
from ..redisx import get_redis
from .errors import CircuitOpen, PolitenessUnavailable


class Breaker:
    def __init__(
        self,
        threshold=BREAKER_THRESHOLD,
        window=BREAKER_WINDOW,
        base_open=BREAKER_OPEN_SECONDS,
        max_open=BREAKER_MAX_OPEN_SECONDS,
    ):
        self.threshold = threshold
        self.window = window
        self.base_open = base_open
        self.max_open = max_open

    async def before(self, host):
        r = get_redis()
        try:
            opened, tripped = await r.mget(f"cb:{host}:open", f"cb:{host}:trips")
            got = None
            if not opened and tripped:
                got = await r.set(f"cb:{host}:probe", "1", nx=True, px=15000)
        except RedisError as exc:
            # Fail closed: an unknown breaker state must not be treated as closed.
            raise PolitenessUnavailable(f"circuit breaker unavailable for {host}: {exc}") from exc
        if opened:
            raise CircuitOpen(f"circuit open for {host}")
        if tripped:
            if not got:
                raise CircuitOpen(f"circuit half-open for {host}")
            return True
        return False

    async def failure(self, host):
        r = get_redis()
        if await r.exists(f"cb:{host}:open"):
            return 0
        if await r.exists(f"cb:{host}:trips"):
            await self._trip(host)
            return 0
        n = await r.incr(f"cb:{host}:fails")
        if n == 1:
            await r.expire(f"cb:{host}:fails", self.window)
        if n >= self.threshold:
            await self._trip(host)
            return 0
        return n

    async def success(self, host):
        r = get_redis()
        trips, fails = await r.mget(f"cb:{host}:trips", f"cb:{host}:fails")
        if trips or fails:
            await r.delete(f"cb:{host}:trips", f"cb:{host}:fails", f"cb:{host}:probe")

    async def _trip(self, host):
        r = get_redis()
        trips = await r.incr(f"cb:{host}:trips")
        await r.expire(f"cb:{host}:trips", 1800)
        seconds = min(self.max_open, self.base_open * 2 ** (trips - 1))
        await r.set(f"cb:{host}:open", "1", px=int(seconds * 1000))
        await r.delete(f"cb:{host}:fails", f"cb:{host}:probe")
        await r.hincrby("m:trips", host, 1)