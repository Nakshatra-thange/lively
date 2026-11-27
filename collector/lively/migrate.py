import asyncio
import logging
import time
from pathlib import Path

from .db import pool

ROOT = Path(__file__).resolve().parent.parent
DIR = ROOT / "migrations"
BASE = ROOT / "schema_base.sql"

log = logging.getLogger("lively.migrate")


async def run_migrations():
    async with pool.connection() as conn:
        await conn.execute("SELECT pg_advisory_xact_lock(727001)")
        cur = await conn.execute("SELECT to_regclass('public.jobs') IS NOT NULL")
        if not (await cur.fetchone())[0]:
            await conn.execute(BASE.read_text(encoding="utf-8"))
            print("applied base schema")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur = await conn.execute("SELECT name FROM schema_migrations")
        applied = {r[0] for r in await cur.fetchall()}
        for path in sorted(DIR.glob("*.sql")):
            if path.name in applied:
                continue
            await conn.execute(path.read_text(encoding="utf-8"))
            await conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,))
            print(f"applied {path.name}")
    print("migrations up to date")

async def pending_migrations():
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT to_regclass('public.schema_migrations') IS NOT NULL")
        applied = set()
        if (await cur.fetchone())[0]:
            cur = await conn.execute("SELECT name FROM schema_migrations")
            applied = {r[0] for r in await cur.fetchall()}
    return [p.name for p in sorted(DIR.glob("*.sql")) if p.name not in applied]


async def wait_for_migrations(timeout=120.0, every=5.0):
    """Block until every migration file is applied. Never applies them itself,
    so workers cannot race the one-shot `migrate` step."""
    deadline = time.monotonic() + timeout
    while True:
        missing = await pending_migrations()
        if not missing:
            return
        if time.monotonic() >= deadline:
            raise SystemExit(
                f"database is missing migrations {missing}; run `lively migrate` first"
            )
        log.warning("waiting for migrations %s to be applied", missing)
        await asyncio.sleep(every)
