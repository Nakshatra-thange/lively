import asyncio
import logging
import re

import httpx

from .config import CONCURRENCY, HTTP_TIMEOUT, USER_AGENT
from .db import pool
from .sources import SOURCES

log = logging.getLogger("lively.discover")


def slug_variants(name):
    base = re.sub(r"[^a-z0-9 ]", "", name.lower()).strip()
    parts = base.split()
    if not parts:
        return []
    return list(dict.fromkeys(["".join(parts), "-".join(parts)]))


async def run_discover(path, concurrency=CONCURRENCY):
    with open(path, encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]

    sem = asyncio.Semaphore(concurrency)
    found = []

    async with httpx.AsyncClient(
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        limits=httpx.Limits(max_connections=concurrency * 2),
    ) as client:

        async def probe(name, slug, source):
            async with sem:
                try:
                    label = await source.probe(client, slug)
                except Exception:
                    return
            if label:
                display = label if source.name in ("greenhouse", "workable") else name
                found.append((source.name, slug, display, source.board_url(slug)))

        tasks = [
            probe(name, slug, source)
            for name in names
            for slug in slug_variants(name)
            for source in SOURCES.values()
        ]
        log.info("probing %d candidates", len(tasks))
        await asyncio.gather(*tasks)

    if found:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO companies (source, slug, name, board_url)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source, slug) DO NOTHING
                    """,
                    found,
                )
    log.info("discovered %d boards", len(found))
