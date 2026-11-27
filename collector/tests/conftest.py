"""Shared fixtures for tests that need PostgreSQL and/or Redis.

PostgreSQL tests need TEST_DATABASE_URL (a superuser URL on a disposable server);
each test gets its own freshly created `lively_test_*` database, dropped afterwards.
They are skipped when TEST_DATABASE_URL is not set.
"""

import asyncio
import os
import sys
import uuid

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool

import lively.cli  # noqa: F401  (imports nearly every lively module)
import lively.db
import lively.sched.leases  # noqa: F401
import lively.sched.worker  # noqa: F401
from lively import migrate, redisx
from lively.liveness.combine import combine
from lively.liveness.models import Signal
from lively.sched import worker as worker_mod
from lively.sched.worker import Worker

ADMIN_URL = os.getenv("TEST_DATABASE_URL")
COLLECTOR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_ORIGINAL_POOL = lively.db.pool
POOL_MODULES = [
    m for n, m in list(sys.modules.items())
    if n.startswith("lively") and getattr(m, "pool", None) is _ORIGINAL_POOL
]


class PG:
    def __init__(self, url, monkeypatch):
        self.url = url
        self.monkeypatch = monkeypatch

    def q(self, sql, params=None):
        with psycopg.connect(self.url) as conn:
            cur = conn.execute(sql, params)
            return cur.fetchall() if cur.description else None

    def run(self, fn):
        """Run zero-arg coroutine function `fn` with every lively module using this DB."""

        async def go():
            p = AsyncConnectionPool(self.url, min_size=1, max_size=12, open=False)
            await p.open()
            for m in POOL_MODULES:
                self.monkeypatch.setattr(m, "pool", p)
            try:
                return await fn()
            finally:
                await p.close()
                await redisx.close_redis()

        return asyncio.run(go())


def _create_db():
    name = f"lively_test_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    params = conninfo_to_dict(ADMIN_URL)
    params["dbname"] = name
    return name, make_conninfo(**params)


def _drop_db(name):
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture
def pg_empty(monkeypatch):
    """A brand-new database with no schema at all."""
    if not ADMIN_URL:
        pytest.skip("TEST_DATABASE_URL not set")
    name, url = _create_db()
    yield PG(url, monkeypatch)
    _drop_db(name)


@pytest.fixture
def pg(pg_empty):
    """A brand-new, fully migrated database."""
    pg_empty.run(migrate.run_migrations)
    return pg_empty


# --- worker harness -------------------------------------------------------------


class FakeEngine:
    def __init__(self, client, boards=None, fail_ids=()):
        self.fail_ids = set(fail_ids)
        self.checked = []

    async def check(self, job):
        self.checked.append(job.id)
        if job.id in self.fail_ids:
            raise RuntimeError(f"boom on job {job.id}")
        return combine([Signal("ats_api", "live", 0.95, "listed on ATS API")])


class DummyClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def run_worker_until(pg, monkeypatch, done_sql, engine=None, timeout=20, client=None,
                     before_stop=None, **kw):
    """Run the real Worker loop until `done_sql` returns true, then stop it.

    `before_stop(worker)` runs while the worker is still live (e.g. to inspect tasks)."""
    engine = engine or FakeEngine(None)
    monkeypatch.setattr(worker_mod, "Engine", lambda client, boards=None: engine)
    monkeypatch.setattr(worker_mod, "make_polite_client", lambda n: client or DummyClient())

    async def go():
        w = Worker(concurrency=kw.get("concurrency", 2), claim_size=kw.get("claim_size", 10),
                   lease=kw.get("lease", 30))
        task = asyncio.create_task(w.run())
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            while loop.time() < deadline:
                async with worker_mod.leases.pool.connection() as conn:
                    cur = await conn.execute(done_sql)
                    if (await cur.fetchone())[0]:
                        break
                assert not task.done(), task.exception()
                await asyncio.sleep(0.2)
            else:
                raise AssertionError(f"timed out waiting for: {done_sql}")
            if before_stop:
                await before_stop(w)
            return w
        finally:
            w.stop.set()
            await asyncio.wait_for(task, 40)

    return pg.run(go), engine


# --- redis ---------------------------------------------------------------------


@pytest.fixture
def redis_down(monkeypatch):
    """Point every Redis user at a closed port: real redis-py ConnectionErrors."""
    import redis.asyncio as aioredis

    def dead():
        return aioredis.from_url(
            "redis://127.0.0.1:1/0", decode_responses=True,
            socket_connect_timeout=0.5, socket_timeout=0.5,
        )

    monkeypatch.setattr(redisx, "_client", dead())
    redisx._scripts.clear()
    yield dead
    redisx._scripts.clear()


TEST_HOSTS = ("boards.greenhouse.io", "boards-api.greenhouse.io", "api.lever.co",
              "api.ashbyhq.com", "apply.workable.com")


def _redis_keys(host):
    return [f"cb:{host}:open", f"cb:{host}:trips", f"cb:{host}:fails", f"cb:{host}:probe",
            f"pen:{host}", f"pen:{host}:n", f"rl:{host}", f"robots:{host}"]


class RealRedis:
    """Synchronous helper around the (disposable) test Redis at REDIS_URL."""

    def cmd(self, *args):
        import redis.asyncio as aioredis
        from lively.config import REDIS_URL

        async def go():
            r = aioredis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=1)
            try:
                return await r.execute_command(*args)
            finally:
                await r.aclose()

        return asyncio.run(go())

    def clear(self):
        for h in TEST_HOSTS:
            self.cmd("DEL", *_redis_keys(h))


@pytest.fixture
def redis_up(monkeypatch):
    """A reachable Redis. Only keys for TEST_HOSTS are touched, never flushed."""
    r = RealRedis()
    try:
        r.cmd("PING")
    except Exception:
        pytest.skip("redis not reachable")
    monkeypatch.setattr(redisx, "_client", None)
    redisx._scripts.clear()
    r.clear()
    yield r
    r.clear()
    redisx._scripts.clear()
