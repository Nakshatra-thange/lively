import asyncio
import random

from redis.exceptions import RedisError

from ..config import DOMAIN_BURST, DOMAIN_RPS, MAX_THROTTLE_WAIT
from ..redisx import get_redis, script
from .errors import PolitenessUnavailable, Throttled

LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = burst
  ts = now
end
tokens = math.min(burst, tokens + (now - ts) * rate / 1000)
tokens = tokens - 1
local wait = 0
if tokens < 0 then
  wait = math.ceil(-tokens * 1000 / rate)
end
local pen = redis.call('PTTL', KEYS[2])
if pen > wait then
  wait = pen
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], 120000)
return wait
"""


class Limiter:
    def __init__(self, rate=DOMAIN_RPS, burst=DOMAIN_BURST):
        self.rate = rate
        self.burst = burst

    async def acquire(self, host):
        try:
            run = script("limiter", LUA)
            wait = await run(keys=[f"rl:{host}", f"pen:{host}"], args=[self.rate, self.burst])
        except RedisError as exc:
            # Fail closed: without the shared bucket we cannot bound the request rate.
            raise PolitenessUnavailable(f"rate limiter unavailable for {host}: {exc}") from exc
        if wait <= 0:
            return 0
        seconds = wait / 1000.0
        if seconds > MAX_THROTTLE_WAIT:
            raise Throttled(f"{host} is throttled for {seconds:.0f}s")
        await asyncio.sleep(seconds + random.random() * 0.05)
        return wait

    async def penalize(self, host, seconds):
        ms = int(min(seconds, 300.0) * 1000)
        r = get_redis()
        current = await r.pttl(f"pen:{host}")
        if ms > current:
            await r.set(f"pen:{host}", "1", px=ms)