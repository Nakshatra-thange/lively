"""Phase 4: every HTTP entry point (manual discover / sync / check) goes through the
polite client, fails closed, and never records a blocked request as evidence.

The polite client's network transport is replaced by httpx.MockTransport (`net`),
so request counts are exact and nothing leaves the machine. The SSRF guard is
recorded in `net.guarded` (real guard behaviour is tested separately below).
"""

import argparse
import asyncio
import os
import subprocess
import sys

import httpx
import pytest

from lively import cli
from lively.discover import run_discover
from lively.liveness.combine import combine
from lively.liveness.models import Signal
from lively.liveness.runner import check_batch
from lively.polite import client as polite_client
from lively.polite.client import PoliteTransport
from lively.polite.errors import UnsafeTarget, blocked_by
from lively.polite.robots import Robots
from lively.sched.worker import blocked_delay
from lively.sync import run_sync

from conftest import COLLECTOR_DIR

PAGE = "boards.greenhouse.io"
API = "boards-api.greenhouse.io"


class Net:
    def __init__(self):
        self.requests = []
        self.guarded = []
        self.routes = {}

    def handle(self, request):
        url = str(request.url)
        self.requests.append(url)
        for prefix, make in self.routes.items():
            if url.startswith(prefix):
                return make()
        return httpx.Response(404, json={})


@pytest.fixture
def net(monkeypatch):
    n = Net()
    monkeypatch.setattr(polite_client.httpx, "AsyncHTTPTransport",
                        lambda **kw: httpx.MockTransport(n.handle))

    async def guard(host):
        n.guarded.append(host)

    monkeypatch.setattr(polite_client, "ensure_public", guard)
    return n


def add_company(pg, slug="acme"):
    return pg.q("INSERT INTO companies (source, slug, name, board_url) VALUES "
                "('greenhouse', %s, 'Acme', %s) RETURNING id", (slug, f"https://{PAGE}/{slug}"))[0][0]


def add_job(pg, state="new"):
    cid = add_company(pg)
    return pg.q("""
        INSERT INTO jobs (company_id, source, external_id, url, title, content_hash, fingerprint, state)
        VALUES (%s, 'greenhouse', '101', %s, 'Engineer', 'h', 'f', %s) RETURNING id
    """, (cid, f"https://{PAGE}/acme/jobs/101", state))[0][0]


def job_snapshot(pg):
    return pg.q("SELECT state::text, next_check_at, last_checked_at, check_count FROM jobs")


# --- manual discovery ------------------------------------------------------------


def test_manual_discover_fails_closed_when_redis_is_down(pg, redis_down, net, tmp_path, caplog):
    path = tmp_path / "c.txt"
    path.write_text("Acme\n")
    with caplog.at_level("WARNING", logger="lively.discover"):
        found = pg.run(lambda: run_discover(str(path), 2))
    assert found == 0
    assert net.requests == []                    # nothing sent
    assert API in net.guarded                    # went through PoliteTransport
    assert pg.q("SELECT count(*) FROM companies") == [(0,)]
    msg = " ".join(r.getMessage() for r in caplog.records)
    assert "probes were blocked by politeness controls" in msg and "PolitenessUnavailable" in msg


def test_manual_discover_uses_rate_limiter(pg, redis_up, net, tmp_path):
    net.routes[f"https://{API}/v1/boards/acme"] = lambda: httpx.Response(200, json={"name": "Acme Inc"})
    path = tmp_path / "c.txt"
    path.write_text("Acme\n")
    assert pg.run(lambda: run_discover(str(path), 2)) == 1
    assert pg.q("SELECT source::text, slug, name FROM companies") == [("greenhouse", "acme", "Acme Inc")]
    assert redis_up.cmd("EXISTS", f"rl:{API}") == 1      # limiter bucket was consulted


# --- manual sync -----------------------------------------------------------------


def test_manual_sync_fails_closed_and_is_not_a_company_failure(pg, redis_down, net, caplog):
    add_company(pg)
    with caplog.at_level("WARNING", logger="lively.sync"):
        totals = pg.run(lambda: run_sync(concurrency=2))
    assert totals == {"boards": 1, "blocked": 1}
    assert net.requests == [] and API in net.guarded
    assert pg.q("SELECT active, fail_count, last_error FROM companies") == [(True, 0, None)]
    assert any("blocked by politeness controls" in r.getMessage() for r in caplog.records)


def test_manual_sync_circuit_open_is_not_a_company_failure(pg, redis_up, net):
    add_company(pg)
    redis_up.cmd("SET", f"cb:{API}:open", "1", "PX", 60000)
    assert pg.run(lambda: run_sync(concurrency=2)) == {"boards": 1, "blocked": 1}
    assert net.requests == []
    assert pg.q("SELECT active, fail_count FROM companies") == [(True, 0)]


def test_manual_sync_works_through_polite_client(pg, redis_up, net):
    add_company(pg)
    net.routes[f"https://{API}/v1/boards/acme/jobs"] = lambda: httpx.Response(200, json={"jobs": [
        {"id": 7, "title": "Engineer", "absolute_url": f"https://{PAGE}/acme/jobs/7",
         "location": {"name": "Remote"}, "content": "<p>Build</p>"}]})
    totals = pg.run(lambda: run_sync(concurrency=2))
    assert totals["inserted"] == 1 and "blocked" not in totals
    assert pg.q("SELECT external_id, state::text FROM jobs") == [("7", "new")]
    assert redis_up.cmd("EXISTS", f"rl:{API}") == 1


# --- manual checks ---------------------------------------------------------------


def test_check_batch_blocked_is_reported_and_not_saved(pg, redis_down, net):
    add_job(pg)
    before = job_snapshot(pg)
    counts = pg.run(lambda: check_batch(["new"], 10, 2))
    assert counts == {"blocked:PolitenessUnavailable": 1}
    assert net.requests == []
    assert pg.q("SELECT count(*) FROM job_checks") == [(0,)]
    assert job_snapshot(pg) == before            # state and schedule untouched


def test_check_batch_circuit_open_never_closes_a_job(pg, redis_up, net):
    add_job(pg, state="live")
    for h in (PAGE, API):
        redis_up.cmd("SET", f"cb:{h}:open", "1", "PX", 60000)
    before = job_snapshot(pg)
    assert pg.run(lambda: check_batch(["live"], 10, 2)) == {"blocked:CircuitOpen": 1}
    assert net.requests == []
    assert job_snapshot(pg) == before and before[0][0] == "live"
    assert pg.q("SELECT count(*) FROM status_events") == [(0,)]


def test_check_one_blocked_prints_and_saves_nothing(pg, redis_down, net, capsys):
    job_id = add_job(pg)
    before = job_snapshot(pg)
    args = argparse.Namespace(job_id=job_id, url=None)
    assert pg.run(lambda: cli.check_one(args)) == "PolitenessUnavailable"
    assert "BLOCKED by politeness controls (PolitenessUnavailable)" in capsys.readouterr().out
    assert net.requests == []
    assert job_snapshot(pg) == before
    assert pg.q("SELECT count(*) FROM job_checks") == [(0,)]


def test_check_one_still_works_and_saves(pg, redis_up, net):
    job_id = add_job(pg)
    redis_up.cmd("SET", f"robots:{PAGE}", "", "EX", 60)   # cached robots.txt: allow all
    args = argparse.Namespace(job_id=job_id, url=None)
    verdict = pg.run(lambda: cli.check_one(args))
    assert verdict in ("live", "closed", "uncertain")
    assert any(u.startswith(f"https://{PAGE}/acme/jobs/101") for u in net.requests)
    assert PAGE in net.guarded and API in net.guarded
    assert pg.q("SELECT count(*) FROM job_checks WHERE checker = 'combined'") == [(1,)]
    assert pg.q("SELECT check_count >= 0, last_checked_at IS NOT NULL FROM jobs") == [(True, True)]


# --- shared classification -------------------------------------------------------


def test_blocked_by_only_flags_inconclusive_results():
    blocked = Signal("http_status", "uncertain", 0.0, "x", {"error": "UnsafeTarget"})
    conclusive = Signal("ats_api", "closed", 0.95, "gone from API")
    assert blocked_by(combine([blocked])) == "UnsafeTarget"
    assert blocked_by(combine([blocked, conclusive])) is None      # real evidence wins
    assert blocked_by(combine([Signal("http_status", "uncertain", 0.0, "timeout",
                                      {"error": "timeout"})])) is None
    assert 21600 <= blocked_delay(combine([blocked])) <= 21600 * 1.2


# --- SSRF guard (real ensure_public, IP literals: no DNS needed) ----------------


class AllowAll:
    async def allowed(self, url):
        return True

    async def close(self):
        pass

    async def before(self, host):
        return False

    async def acquire(self, host):
        return 0

    async def success(self, host):
        pass


@pytest.mark.parametrize("url", ["http://127.0.0.1/", "http://169.254.169.254/latest/meta-data",
                                 "http://10.0.0.5:8080/x", "http://[::1]/"])
def test_transport_refuses_non_public_targets(url, redis_down):
    sent = []
    stub = AllowAll()
    t = PoliteTransport(httpx.MockTransport(lambda r: sent.append(r) or httpx.Response(200)),
                        limiter=stub, breaker=stub, robots=stub)

    async def go():
        async with httpx.AsyncClient(transport=t) as c:
            with pytest.raises(UnsafeTarget):
                await c.get(url)

    asyncio.run(go())
    assert sent == []


def test_redirect_to_internal_address_is_refused(redis_down):
    sent = []

    def handle(request):
        sent.append(str(request.url))
        return httpx.Response(302, headers={"location": "http://169.254.169.254/latest"})

    stub = AllowAll()
    t = PoliteTransport(httpx.MockTransport(handle), limiter=stub, breaker=stub, robots=stub)

    async def go():
        async with httpx.AsyncClient(transport=t, follow_redirects=True) as c:
            with pytest.raises(UnsafeTarget):
                await c.get("http://93.184.216.34/job")

    asyncio.run(go())
    assert sent == ["http://93.184.216.34/job"]   # the internal hop was never sent


def test_robots_fetch_is_guarded(monkeypatch):
    from lively import redisx

    class EmptyRedis:
        async def get(self, key):
            return None

        async def set(self, *a, **kw):
            return True

    monkeypatch.setattr(redisx, "_client", EmptyRedis())
    sent = []
    robots = Robots()
    robots._http()._transport = httpx.MockTransport(lambda r: sent.append(r) or httpx.Response(200, text=""))
    # an internal origin: robots.txt must not be fetched; rules default to disallow-all
    assert asyncio.run(robots.allowed(httpx.URL("http://127.0.0.1:8080/jobs/1"))) is False
    assert sent == []


# --- real CLI processes ----------------------------------------------------------


def run_cli(pg, *args):
    env = {**os.environ, "DATABASE_URL": pg.url, "REDIS_URL": "redis://127.0.0.1:1/0",
           "REDIS_TIMEOUT": "0.5", "CONCURRENCY": "2"}
    return subprocess.run([sys.executable, "-m", "lively.cli", *args], cwd=COLLECTOR_DIR,
                          env=env, capture_output=True, text=True, timeout=120)


def test_cli_commands_keep_their_arguments():
    for cmd, opts in {
        "sync": ["--source", "--limit", "--stale-hours", "--concurrency"],
        "discover": ["path", "--concurrency"],
        "check": ["--job-id", "--url", "--state", "--limit", "--concurrency"],
        "seed": ["path"],
    }.items():
        out = subprocess.run([sys.executable, "-m", "lively.cli", cmd, "--help"],
                             cwd=COLLECTOR_DIR, capture_output=True, text=True, check=True).stdout
        for opt in opts:
            assert opt in out, (cmd, opt)


def test_cli_with_redis_down_fails_closed_and_reports(pg, tmp_path):
    # Real processes, real network stack; Redis unreachable -> only DNS lookups
    # (SSRF guard) happen, no HTTP request can be sent.
    job_id = add_job(pg)
    before = job_snapshot(pg)

    r = run_cli(pg, "check", "--job-id", str(job_id))
    assert r.returncode == 0, r.stderr
    assert "BLOCKED by politeness controls (PolitenessUnavailable)" in r.stdout

    r = run_cli(pg, "check", "--state", "new")
    assert r.returncode == 0, r.stderr
    assert "blocked by politeness controls and not saved" in r.stderr

    r = run_cli(pg, "sync")
    assert r.returncode == 0, r.stderr
    assert "were blocked by politeness controls" in r.stderr

    cands = tmp_path / "c.txt"
    cands.write_text("Acme\n")
    r = run_cli(pg, "discover", str(cands))
    assert r.returncode == 0, r.stderr
    assert "probes were blocked by politeness controls" in r.stderr

    assert job_snapshot(pg) == before
    assert pg.q("SELECT count(*) FROM job_checks") == [(0,)]
    assert pg.q("SELECT count(*), sum(fail_count), bool_and(active) FROM companies") == [(1, 0, True)]
