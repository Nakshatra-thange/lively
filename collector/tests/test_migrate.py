"""Migration and first-run bootstrap tests.

Needs a disposable PostgreSQL server: set TEST_DATABASE_URL to a superuser URL
(e.g. postgresql://postgres:test@localhost:55432/postgres). Each test creates and
drops its own `lively_test_*` databases; nothing else on the server is touched.
"""

import asyncio
import os
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool

from lively import cli, migrate

ADMIN_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not ADMIN_URL, reason="TEST_DATABASE_URL not set")

COLLECTOR = Path(__file__).resolve().parents[1]
INIT_SQL = COLLECTOR.parent / "db" / "init.sql"
MIGRATIONS = sorted(p.name for p in migrate.DIR.glob("*.sql"))


@pytest.fixture
def new_db():
    created = []

    def make():
        name = f"lively_test_{uuid.uuid4().hex[:10]}"
        with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{name}"')
        created.append(name)
        params = conninfo_to_dict(ADMIN_URL)
        params["dbname"] = name
        return make_conninfo(**params)

    yield make
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        for name in created:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def with_pool(url, monkeypatch, fn):
    async def go():
        p = AsyncConnectionPool(url, min_size=1, max_size=4, open=False)
        await p.open()
        monkeypatch.setattr(migrate, "pool", p)
        monkeypatch.setattr(cli, "pool", p)
        try:
            return await fn()
        finally:
            await p.close()

    return asyncio.run(go())


def migrate_db(url, monkeypatch):
    with_pool(url, monkeypatch, migrate.run_migrations)


def q(url, sql, params=None):
    with psycopg.connect(url) as conn:
        return conn.execute(sql, params).fetchall()


def schema(url):
    """Everything that defines the schema, in a comparable form."""
    return {
        "columns": q(url, """
            SELECT table_name, column_name, data_type, udt_name, is_nullable, column_default
            FROM information_schema.columns WHERE table_schema = 'public'
            ORDER BY table_name, column_name
        """),
        "indexes": q(url, "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY 1"),
        "constraints": q(url, """
            SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid)
            FROM pg_constraint WHERE connamespace = 'public'::regnamespace ORDER BY 1, 2
        """),
        "enums": q(url, """
            SELECT t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder)
            FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid GROUP BY 1 ORDER BY 1
        """),
    }


def applied(url):
    return [r[0] for r in q(url, "SELECT name FROM schema_migrations ORDER BY name")]


def apply_init_sql(url):
    with psycopg.connect(url) as conn:
        conn.execute(INIT_SQL.read_text(encoding="utf-8"))


def insert_sample(url):
    with psycopg.connect(url) as conn:
        cid = conn.execute("""
            INSERT INTO companies (source, slug, name, board_url, last_synced_at)
            VALUES ('greenhouse', 'acme', 'Acme', 'https://boards.greenhouse.io/acme', now() - interval '1 day')
            RETURNING id
        """).fetchone()[0]
        conn.execute("""
            INSERT INTO jobs (company_id, source, external_id, url, title, content_hash, fingerprint, state)
            VALUES (%s, 'greenhouse', '1', 'https://example.com/1', 'Engineer', 'h', 'f', 'live')
        """, (cid,))


# --- migrations ---------------------------------------------------------------


def test_fresh_database_gets_full_schema(new_db, monkeypatch):
    url = new_db()
    assert q(url, "SELECT to_regclass('public.jobs')") == [(None,)]

    migrate_db(url, monkeypatch)

    assert applied(url) == MIGRATIONS
    cols = {(t, c) for t, c, *_ in schema(url)["columns"]}
    assert ("jobs", "lease_owner") in cols        # 002
    assert ("companies", "next_sync_at") in cols  # 002
    assert ("jobs", "ghost_score") in cols        # 003
    assert ("app_state", "key") in cols           # 004


def test_init_sql_database_is_migrated_and_data_kept(new_db, monkeypatch):
    url = new_db()
    apply_init_sql(url)
    insert_sample(url)

    migrate_db(url, monkeypatch)

    assert applied(url) == MIGRATIONS
    assert q(url, "SELECT slug FROM companies") == [("acme",)]
    rows = q(url, "SELECT title, state::text, lease_owner, check_count, ghost_reasons::text FROM jobs")
    assert rows == [("Engineer", "live", None, 0, "[]")]


def test_base_schema_matches_init_sql(new_db, monkeypatch):
    # collector/schema_base.sql must not drift from db/init.sql.
    fresh, from_init = new_db(), new_db()
    migrate_db(fresh, monkeypatch)
    apply_init_sql(from_init)
    migrate_db(from_init, monkeypatch)
    assert schema(fresh) == schema(from_init)


def test_base_schema_is_idempotent(new_db):
    url = new_db()
    sql = migrate.BASE.read_text(encoding="utf-8")
    with psycopg.connect(url) as conn:
        conn.execute(sql)
    with psycopg.connect(url) as conn:
        conn.execute(sql)


def test_repeated_migration_is_a_noop(new_db, monkeypatch):
    url = new_db()
    apply_init_sql(url)
    insert_sample(url)
    migrate_db(url, monkeypatch)
    before_schema = schema(url)
    before_rows = q(url, "SELECT id, next_sync_at FROM companies ORDER BY id")
    before_applied = q(url, "SELECT name, applied_at FROM schema_migrations ORDER BY name")

    migrate_db(url, monkeypatch)
    migrate_db(url, monkeypatch)

    assert schema(url) == before_schema
    assert q(url, "SELECT id, next_sync_at FROM companies ORDER BY id") == before_rows
    assert q(url, "SELECT name, applied_at FROM schema_migrations ORDER BY name") == before_applied


def test_concurrent_migrate_processes_apply_once(new_db):
    url = new_db()
    env = {**os.environ, "DATABASE_URL": url, "CONCURRENCY": "1"}
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "lively.cli", "migrate"],
            cwd=COLLECTOR, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        for _ in range(4)
    ]
    outputs = [p.communicate(timeout=60)[0] for p in procs]
    assert [p.returncode for p in procs] == [0, 0, 0, 0], outputs
    assert applied(url) == MIGRATIONS
    assert sum(o.count("applied base schema") for o in outputs) == 1


# --- bootstrap ----------------------------------------------------------------


@pytest.fixture
def fake_network(monkeypatch, tmp_path):
    """Replace discover/sync (which hit the internet) with recorders."""
    calls = []

    async def fake_discover(path, concurrency, client=None):
        calls.append("discover")
        return 0

    async def fake_sync(source=None, limit=None, stale_hours=0, concurrency=None, client=None):
        calls.append("sync")
        return {"boards": 1}

    monkeypatch.setattr(cli, "run_discover", fake_discover)
    monkeypatch.setattr(cli, "run_sync", fake_sync)
    boards = tmp_path / "boards.csv"
    boards.write_text("source,slug,name\ngreenhouse,acme,Acme\nlever,beta,Beta\n")
    candidates = tmp_path / "candidates.txt"
    candidates.write_text("Acme\n")
    return calls, str(boards), str(candidates)


def boot(url, monkeypatch, boards, candidates, force=False):
    return with_pool(url, monkeypatch, lambda: cli.bootstrap(boards, candidates, 2, force))


def test_bootstrap_fresh_runs_once(new_db, monkeypatch, fake_network):
    calls, boards, candidates = fake_network
    url = new_db()

    assert boot(url, monkeypatch, boards, candidates) == "fresh"
    assert calls == ["discover", "sync"]
    assert applied(url) == MIGRATIONS
    assert q(url, "SELECT count(*) FROM companies") == [(2,)]
    assert q(url, "SELECT value FROM app_state WHERE key = 'bootstrap_completed'") == [("fresh",)]

    # A restart / second invocation does nothing.
    assert boot(url, monkeypatch, boards, candidates) == "skipped"
    assert calls == ["discover", "sync"]
    assert q(url, "SELECT count(*) FROM companies") == [(2,)]


def test_bootstrap_existing_database_is_left_alone(new_db, monkeypatch, fake_network):
    calls, boards, candidates = fake_network
    url = new_db()
    apply_init_sql(url)
    insert_sample(url)

    assert boot(url, monkeypatch, boards, candidates) == "existing"
    assert calls == []
    assert q(url, "SELECT slug FROM companies") == [("acme",)]
    assert q(url, "SELECT count(*) FROM jobs") == [(1,)]


def test_bootstrap_force_is_additive(new_db, monkeypatch, fake_network):
    calls, boards, candidates = fake_network
    url = new_db()
    apply_init_sql(url)
    insert_sample(url)

    assert boot(url, monkeypatch, boards, candidates, force=True) == "fresh"
    assert calls == ["discover", "sync"]
    assert q(url, "SELECT slug FROM companies ORDER BY slug") == [("acme",), ("beta",)]
    assert q(url, "SELECT count(*) FROM jobs") == [(1,)]


def test_bootstrap_failure_is_resumed_on_retry(new_db, monkeypatch, fake_network):
    calls, boards, candidates = fake_network
    url = new_db()
    real_sync = cli.run_sync

    async def broken_sync(**kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(cli, "run_sync", broken_sync)
    with pytest.raises(RuntimeError):
        boot(url, monkeypatch, boards, candidates)
    assert q(url, "SELECT count(*) FROM app_state WHERE key = 'bootstrap_completed'") == [(0,)]
    assert q(url, "SELECT count(*) FROM companies") == [(2,)]

    # Seeded companies from the failed run must not make the retry think this
    # is a pre-existing install: it must run the steps again and complete.
    monkeypatch.setattr(cli, "run_sync", real_sync)
    calls.clear()
    assert boot(url, monkeypatch, boards, candidates) == "fresh"
    assert calls == ["discover", "sync"]
    assert q(url, "SELECT count(*) FROM companies") == [(2,)]


def test_bootstrap_refuses_to_run_concurrently(new_db, monkeypatch, fake_network):
    _, boards, candidates = fake_network
    url = new_db()
    migrate_db(url, monkeypatch)
    with psycopg.connect(url) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (cli.BOOTSTRAP_LOCK,))
        with pytest.raises(SystemExit):
            boot(url, monkeypatch, boards, candidates)
    assert q(url, "SELECT count(*) FROM companies") == [(0,)]
