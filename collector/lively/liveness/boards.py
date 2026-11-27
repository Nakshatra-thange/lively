import asyncio
import time
from collections import defaultdict

from ..sources import SOURCES


class BoardCache:
    def __init__(self, ttl=600):
        self.ttl = ttl
        self._data = {}
        self._locks = defaultdict(asyncio.Lock)

    async def ids(self, client, source, slug):
        key = (source, slug)
        async with self._locks[key]:
            hit = self._data.get(key)
            if hit and time.monotonic() - hit[0] < self.ttl:
                return hit[1], hit[2]
            try:
                if source == "greenhouse":
                    raw = await SOURCES[source].fetch(client, slug, content=False)
                else:
                    raw = await SOURCES[source].fetch(client, slug)
            except Exception:
                return "error", set()
            if raw is None:
                entry = ("missing", set())
            else:
                entry = ("ok", {j.external_id for j in raw})
            self._data[key] = (time.monotonic(), entry[0], entry[1])
            return entry
