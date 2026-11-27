"""Phase 2: first-run pipeline (bootstrap -> sync -> worker claims/checks/schedules).

No network: the four ATS sources are replaced by in-memory fakes and the liveness
engine by a fake. Everything else (bootstrap, seed, discover, sync, leases,
scheduling, worker loop) is the real code against an isolated database.
"""

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from lively import cli, migrate
from lively.polite.client import PoliteTransport
from lively.sources import SOURCES
from lively.sources.base import RawJob

from conftest import run_worker_until

REPO = Path(__file__).resolve().parents[2]


def raw(external_id, title="Engineer"):
    return RawJob(
        external_id=str(external_id), title=title, url=f"https://jobs.test/{external_id}",
        apply_url=None, location="Remote", department=None, employment_type=None,
        remote=True, description=f"{title} description {external_id}", posted_at=None,
    )


class FakeSource:
    def __init__(self, name, boards=None):
        self.name = name
        self.boards = boards or {}

    def board_url(self, slug):
        return f"https://{self.name}.test/{slug}"

    async def probe(self, client, slug):
        return slug.title() if slug in self.boards else None

    async def fetch(self, client, slug, content=True):
        value = self.boards.get(slug)
        if isinstance(value, Exception):
            raise value
        return value


@pytest.fixture
def ats(monkeypatch):
    """Replace every ATS source with a fake; returns the greenhouse fake's boards dict."""
    gh = FakeSource("greenhouse", {
        "acme": [raw(1), raw(2, "Designer")],
        "betalabs": [raw(3, "PM")],
    })
    for name in list(SOURCES):
        monkeypatch.setitem(SOURCES, name, gh if name == "greenhouse" else FakeSource(name))
    return gh.boards


@pytest.fixture
def seeds(tmp_path):
    boards = tmp_path / "boards.csv"
    boards.write_text("source,slug,name\ngreenhouse,acme,Acme\n")
    candidates = tmp_path / "candidates.txt"
    candidates.write_text("Beta Labs\nNobody Here\n")  # -> betalabs is discovered
    return str(boards), str(candidates)


def boot(pg, seeds, force=False):
    return pg.run(lambda: cli.bootstrap(*seeds, concurrency=2, force=force))


# --- first-time setup ---------------------------------------------------------


def test_fresh_install_end_to_end(pg_empty, monkeypatch, ats, seeds):
    pg = pg_empty
    assert boot(pg, seeds) == "fresh"

    # seeded + discovered companies, all active
    assert pg.q("SELECT slug, active FROM companies ORDER BY slug") == [("acme", True), ("betalabs", True)]
    # initial sync populated jobs; they are new, unleased and due now
    rows = pg.q("""SELECT external_id, state::text, lease_owner IS NULL, next_check_at <= now(), check_count
                   FROM jobs ORDER BY external_id""")
    assert rows == [("1", "new", True, True, 0), ("2", "new", True, True, 0), ("3", "new", True, True, 0)]
    assert pg.q("SELECT count(*) FROM status_events WHERE to_state = 'new'") == [(3,)]

    # the real worker claims, checks, saves and schedules them
    run_worker_until(pg, monkeypatch, "SELECT bool_and(check_count = 1) FROM jobs")
    rows = pg.q("""
        SELECT state::text, lease_owner, check_interval_seconds,
               extract(epoch FROM next_check_at - last_checked_at)
        FROM jobs ORDER BY id""")
    for state, owner, interval, delay in rows:
        assert state == "live" and owner is None
        assert interval == 172800                      # BASE 1d doubled after a stable first check
        assert 0.89 * interval <= delay <= 1.11 * interval   # +-10% jitter
    assert pg.q("SELECT count(*) FROM job_checks WHERE checker = 'combined' AND lease_token IS NOT NULL") == [(3,)]
    assert pg.q("SELECT count(*) FROM status_events WHERE from_state = 'new' AND to_state = 'live'") == [(3,)]


def test_company_sync_is_scheduled_independently(pg, monkeypatch, ats, seeds):
    boot(pg, seeds)
    run_worker_until(
        pg, monkeypatch,
        "SELECT bool_and(next_sync_at > now() AND lease_owner IS NULL) FROM companies",
    )
    # worker re-synced boards on their own schedule without duplicating jobs
    assert pg.q("SELECT count(*) FROM jobs") == [(3,)]
    rows = pg.q("SELECT extract(epoch FROM next_sync_at - now()) FROM companies")
    assert all(r[0] > 3000 for r in rows)  # >= MIN_SYNC (1h) minus jitter


def test_jobs_with_only_default_scheduling_values_are_claimed(pg, monkeypatch):
    # rows inserted with nothing but required columns (NULL lease/verdict/hash columns)
    pg.q("""
        WITH c AS (INSERT INTO companies (source, slug, name, board_url)
                   VALUES ('lever', 'x', 'X', 'https://jobs.lever.co/x') RETURNING id)
        INSERT INTO jobs (company_id, source, external_id, url, title, content_hash, fingerprint)
        SELECT id, 'lever', 'j1', 'https://jobs.lever.co/x/j1', 'T', 'h', 'f' FROM c
        RETURNING id
    """)
    pg.q("UPDATE companies SET next_sync_at = now() + interval '1 day'")
    run_worker_until(pg, monkeypatch, "SELECT bool_and(check_count = 1) FROM jobs")
    assert pg.q("SELECT state::text, last_verdict, seen_hash = content_hash FROM jobs") == [("live", "live", True)]


# --- repeated / partial / failed setup ---------------------------------------


def test_repeated_setup_does_not_duplicate_or_reset(pg, monkeypatch, ats, seeds):
    boot(pg, seeds)
    run_worker_until(pg, monkeypatch, "SELECT bool_and(check_count = 1) FROM jobs")
    snapshot = "SELECT id, state::text, next_check_at, check_count, check_interval_seconds FROM jobs ORDER BY id"
    before = pg.q(snapshot)
    companies = pg.q("SELECT id, slug, next_sync_at FROM companies ORDER BY id")

    assert boot(pg, seeds) == "skipped"
    assert pg.q(snapshot) == before

    # even a forced re-run only adds/updates content; it does not reset state or schedules
    assert boot(pg, seeds, force=True) == "fresh"
    assert pg.q(snapshot) == before
    assert pg.q("SELECT id, slug, next_sync_at FROM companies ORDER BY id") == companies


def test_seed_failure_is_reported_and_recoverable(pg, ats, seeds, tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("source,slug,name\nmonster,acme,Acme\n")
    with pytest.raises(cli.BootstrapError, match="during seed"):
        boot(pg, (str(bad), seeds[1]))
    assert pg.q("SELECT count(*) FROM app_state WHERE key = 'bootstrap_completed'") == [(0,)]

    assert boot(pg, seeds) == "fresh"
    assert pg.q("SELECT count(*) FROM jobs") == [(3,)]


def test_discover_failure_is_reported_and_recoverable(pg, ats, seeds, tmp_path):
    missing = str(tmp_path / "nope.txt")
    with pytest.raises(cli.BootstrapError, match="during discover"):
        boot(pg, (seeds[0], missing))
    # seed already ran, but the started marker means a retry resumes rather than skipping
    assert pg.q("SELECT count(*) FROM companies") == [(1,)]
    assert boot(pg, seeds) == "fresh"
    assert pg.q("SELECT count(*) FROM companies") == [(2,)]
    assert pg.q("SELECT count(*) FROM jobs") == [(3,)]


def test_total_sync_failure_is_not_marked_complete(pg, ats, seeds):
    good = dict(ats)
    for slug in ats:
        ats[slug] = RuntimeError("ATS down")
    with pytest.raises(cli.BootstrapError, match="0 of 2 boards synced"):
        boot(pg, seeds)
    assert pg.q("SELECT count(*) FROM app_state WHERE key = 'bootstrap_completed'") == [(0,)]
    # failures recorded, companies NOT deactivated after one failure
    assert pg.q("SELECT fail_count, active FROM companies ORDER BY slug") == [(1, True), (1, True)]

    ats.update(good)
    assert boot(pg, seeds) == "fresh"
    assert pg.q("SELECT count(*) FROM jobs") == [(3,)]
    assert pg.q("SELECT fail_count FROM companies ORDER BY slug") == [(0,), (0,)]


def test_partial_sync_failure_completes_with_warning(pg, ats, seeds, capsys):
    ats["betalabs"] = RuntimeError("ATS down")
    assert boot(pg, seeds) == "fresh"
    assert "1 boards failed initial sync" in capsys.readouterr().out
    assert pg.q("SELECT slug, fail_count, active FROM companies ORDER BY slug") == [
        ("acme", 0, True), ("betalabs", 1, True)]
    assert pg.q("SELECT count(*) FROM jobs") == [(2,)]


def test_bootstrap_network_steps_use_polite_client(pg, monkeypatch, seeds):
    seen = []

    async def spy_discover(path, concurrency, client=None):
        seen.append(client)
        return 0

    async def spy_sync(concurrency=None, client=None, **kw):
        seen.append(client)
        return {"boards": 1}

    monkeypatch.setattr(cli, "run_discover", spy_discover)
    monkeypatch.setattr(cli, "run_sync", spy_sync)
    boot(pg, seeds)
    assert len(seen) == 2
    assert all(isinstance(c._transport, PoliteTransport) for c in seen)


# --- migration-before-worker ordering ----------------------------------------


def test_worker_refuses_to_start_on_unmigrated_db(pg_empty, monkeypatch):
    async def go():
        with pytest.raises(SystemExit, match="missing migrations"):
            await migrate.wait_for_migrations(timeout=0.3, every=0.1)
        assert await migrate.pending_migrations() == sorted(p.name for p in migrate.DIR.glob("*.sql"))

    pg_empty.run(go)


def test_worker_waits_until_migrations_complete(pg_empty):
    async def go():
        async def later():
            await asyncio.sleep(0.5)
            await migrate.run_migrations()

        t = asyncio.create_task(later())
        await migrate.wait_for_migrations(timeout=10, every=0.1)
        await t
        assert await migrate.pending_migrations() == []

    pg_empty.run(go)


def compose_config():
    """Rendered compose config: from $COMPOSE_CONFIG_JSON (pre-rendered with
    `docker compose --profile init config --format json`) or the docker CLI."""
    path = os.getenv("COMPOSE_CONFIG_JSON")
    if path:
        return json.loads(Path(path).read_text())
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not available and COMPOSE_CONFIG_JSON not set")
    out = subprocess.run(
        ["docker", "compose", "--profile", "init", "config", "--format", "json"],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout
    return json.loads(out)


def test_compose_services_wait_for_migrate():
    services = compose_config()["services"]
    assert services["migrate"]["command"][-1] == "migrate"
    for name in ("worker", "api", "metrics", "ghost", "collector", "bootstrap"):
        dep = services[name]["depends_on"]["migrate"]
        assert dep["condition"] == "service_completed_successfully", name
    assert "migrate" not in services["migrate"].get("depends_on", {})
