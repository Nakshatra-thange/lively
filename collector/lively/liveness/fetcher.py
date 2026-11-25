import asyncio
import time
from collections import defaultdict
from urllib.parse import urlparse

import httpx

from ..config import USER_AGENT
from .models import FetchResult

MAX_BYTES = 600_000
TOTAL_TIMEOUT = 25.0
HOST_LIMIT = 6

_host_sems = defaultdict(lambda: asyncio.Semaphore(HOST_LIMIT))


def make_client(concurrency):
    return httpx.AsyncClient(
        timeout=httpx.Timeout(connect=6.0, read=15.0, write=10.0, pool=30.0),
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.5",
            "Accept-Language": "en",
        },
        follow_redirects=True,
        max_redirects=6,
        limits=httpx.Limits(
            max_connections=concurrency * 2,
            max_keepalive_connections=concurrency,
        ),
    )


def _retry_after(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def fetch(client, url, max_bytes=MAX_BYTES):
    host = urlparse(url).netloc
    started = time.monotonic()
    chunks = []
    size = 0
    truncated = False
    partial = False
    status = None
    final_url = url
    redirects = []
    retry_after = None
    error = None

    async with _host_sems[host]:
        try:
            async with asyncio.timeout(TOTAL_TIMEOUT):
                async with client.stream("GET", url) as resp:
                    status = resp.status_code
                    final_url = str(resp.url)
                    redirects = [str(r.url) for r in resp.history]
                    retry_after = _retry_after(resp.headers.get("retry-after"))
                    try:
                        async for chunk in resp.aiter_bytes():
                            chunks.append(chunk)
                            size += len(chunk)
                            if size >= max_bytes:
                                truncated = True
                                break
                    except (httpx.ReadError, httpx.RemoteProtocolError, httpx.ReadTimeout):
                        partial = True
        except TimeoutError:
            error = "timeout"
        except httpx.TooManyRedirects:
            error = "too many redirects"
        except httpx.TimeoutException:
            error = "timeout"
        except httpx.HTTPError as exc:
            error = type(exc).__name__

    if error and status is not None:
        partial = True

    return FetchResult(
        url=url,
        final_url=final_url,
        status=status,
        redirects=redirects,
        body=b"".join(chunks).decode("utf-8", errors="replace"),
        truncated=truncated,
        partial=partial,
        latency_ms=int((time.monotonic() - started) * 1000),
        error=error,
        retry_after=retry_after,
    )
