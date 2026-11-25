import asyncio
import uuid

import httpx
import pytest

from lively.polite.breaker import Breaker
from lively.polite.errors import CircuitOpen
from lively.polite.limiter import Limiter
from lively.polite.robots import Robots
from lively.redisx import close_redis, get_redis


def run_with_redis(body):
    async def main():
        try:
            await get_redis().ping()
        except Exception:
            pytest.skip("redis not reachable")
        try:
            return await body()
        finally:
            await close_redis()

    return asyncio.run(main())


def test_limiter_spaces_out_a_burst():
    async def body():
        host = f"t-{uuid.uuid4().hex}"
        limiter = Limiter(rate=20, burst=2)
        waits = await asyncio.gather(*(limiter.acquire(host) for _ in range(6)))
        ordered = sorted(waits)
        assert ordered[:2] == [0, 0]
        assert ordered[-1] >= 100

    run_with_redis(body)


def test_breaker_trips_then_recovers():
    async def body():
        host = f"t-{uuid.uuid4().hex}"
        breaker = Breaker(threshold=3, window=30, base_open=1, max_open=2)
        for _ in range(3):
            await breaker.before(host)
            await breaker.failure(host)
        with pytest.raises(CircuitOpen):
            await breaker.before(host)
        await asyncio.sleep(1.2)
        assert await breaker.before(host) is True
        with pytest.raises(CircuitOpen):
            await breaker.before(host)
        await breaker.success(host)
        assert await breaker.before(host) is False

    run_with_redis(body)


def test_robots_rules_are_enforced():
    robots = Robots()

    async def fake_load(origin, host):
        return "User-agent: *\nDisallow: /private\n", 60

    robots._load = fake_load

    async def body():
        assert await robots.allowed(httpx.URL("https://example.com/jobs/1"))
        assert not await robots.allowed(httpx.URL("https://example.com/private/x"))
        assert await robots.allowed(httpx.URL("https://boards-api.greenhouse.io/private"))

    asyncio.run(body())