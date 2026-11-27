import redis.asyncio as aioredis

from .config import REDIS_TIMEOUT, REDIS_URL

_client = None
_scripts = {}


def get_redis():
    global _client
    if _client is None:
        # Bounded timeouts: a stalled Redis must surface as an error, not hang a
        # consumer forever while the heartbeat keeps renewing its leases.
        _client = aioredis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=REDIS_TIMEOUT,
            socket_timeout=REDIS_TIMEOUT,
        )
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