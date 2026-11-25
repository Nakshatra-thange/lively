import redis.asyncio as aioredis

from .config import REDIS_URL

_client = None
_scripts = {}


def get_redis():
    global _client
    if _client is None:
        _client = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _client


def script(name, source):
    found = _scripts.get(name)
    if found is None:
        found = get_redis().register_script(source)
        _scripts[name] = found
    return found


async def close_redis():
    global _client
    _scripts.clear()
    if _client is not None:
        await _client.aclose()
        _client = None