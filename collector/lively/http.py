import asyncio
import random

import httpx

from .polite.errors import Blocked


def backoff(attempt):
    return min(30.0, 2.0 ** attempt) + random.random()


async def get_json(client, url, retries=3):
    last = None
    for attempt in range(retries):
        try:
            resp = await client.get(url)
        except Blocked:
            raise
        except httpx.HTTPError as exc:
            last = exc
            await asyncio.sleep(backoff(attempt))
            continue
        if resp.status_code in (404, 410):
            return None
        if resp.status_code == 429 or resp.status_code >= 500:
            last = RuntimeError(f"http {resp.status_code}")
            await asyncio.sleep(backoff(attempt))
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"http {resp.status_code}")
        try:
            return resp.json()
        except ValueError:
            return None
    raise last or RuntimeError("request failed")