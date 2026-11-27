import asyncio
import contextlib
import logging
import re
from collections import Counter

from .config import CONCURRENCY
from .db import pool
from .polite.client import make_polite_client
from .polite.errors import Blocked
from .sources import SOURCES

log = logging.getLogger("lively.discover")


def slug_variants(name):
    base = re.sub(r"[^a-z0-9 ]", "", name.lower()).strip()
    parts = base.split()
    if not parts:
        return []
    return list(dict.fromkeys(["".join(parts), "-".join(parts)]))


async def run_discover(path, concurrency=CONCURRENCY, client=None):
    with open(path, encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]

    sem = asyncio.Semaphore(concurrency)
    found = []
    blocked = Counter()

    # Same politeness controls (SSRF guard, robots, limiter, breaker) as the worker.
    session = make_polite_client(concurrency) if client is None else contextlib.nullcontext(client)
    async with session as client:

        async def probe(name, slug, source):
            async with sem:
                try:
                    label = await source.probe(client, slug)
                except Blocked as exc:
                    # Not "no board here": we could not ask. Never recorded as absent.
                    blocked[type(exc).__name__] += 1
                    return
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
    if blocked:
        log.warning(
            "%d probes were blocked by politeness controls %s; those candidates were not "
            "checked, rerun discover later", sum(blocked.values()), dict(blocked),
        )
    return len(found)
