import time
from collections import defaultdict
from urllib.robotparser import RobotFileParser

import asyncio
import httpx

from ..config import ROBOTS_ENFORCE, ROBOTS_TTL, USER_AGENT
from ..redisx import get_redis

API_HOSTS = {"boards-api.greenhouse.io", "api.lever.co", "api.ashbyhq.com"}
AGENT = "LivelyBot"
DISALLOW_ALL = "User-agent: *\nDisallow: /\n"


def is_api(url):
    return url.host in API_HOSTS or url.path.startswith("/api/")


class Robots:
    def __init__(self):
        self._local = {}
        self._locks = defaultdict(asyncio.Lock)
        self._http_client = None

    def _http(self):
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=10.0,
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
                trust_env=False,
            )
        return self._http_client

    async def close(self):
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def allowed(self, url):
        if not ROBOTS_ENFORCE or is_api(url):
            return True
        origin = f"{url.scheme}://{url.host}"
        if url.port:
            origin += f":{url.port}"
        parser = await self._parser(origin, url.host)
        return parser.can_fetch(AGENT, str(url))

    async def _parser(self, origin, host):
        hit = self._local.get(host)
        if hit and hit[0] > time.monotonic():
            return hit[1]
        async with self._locks[host]:
            hit = self._local.get(host)
            if hit and hit[0] > time.monotonic():
                return hit[1]
            text, ttl = await self._load(origin, host)
            parser = RobotFileParser()
            parser.parse(text.splitlines())
            self._local[host] = (time.monotonic() + min(ttl, 3600), parser)
            return parser

    async def _load(self, origin, host):
        r = get_redis()
        cached = await r.get(f"robots:{host}")
        if cached is not None:
            return cached, 3600
        try:
            resp = await self._http().get(f"{origin}/robots.txt")
        except httpx.HTTPError:
            await r.set(f"robots:{host}", DISALLOW_ALL, ex=300)
            return DISALLOW_ALL, 300
        if resp.status_code == 200:
            text, ttl = resp.text[:500000], ROBOTS_TTL
        elif 400 <= resp.status_code < 500:
            text, ttl = "", ROBOTS_TTL
        else:
            text, ttl = DISALLOW_ALL, 300
        await r.set(f"robots:{host}", text, ex=ttl)
        return text, ttl