"""Phase 3: Redis failure resilience and worker reliability.

Redis outages use a real redis-py client pointed at a closed port (`redis_down`),
so failures are genuine ConnectionErrors. HTTP goes through httpx.MockTransport so
we can assert that no request reached the network when politeness is unenforceable.
"""

import asyncio
import logging
import time

import httpx
import pytest
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError

from conftest import FakeEngine, run_worker_until
from lively import metrics, redisx
from lively.config import REDIS_URL
from lively.liveness.combine import combine
from lively.liveness.models import Signal
from lively.polite.breaker import Breaker
from lively.polite.client import PoliteTransport
from lively.polite.errors import Blocked, PolitenessUnavailable
from lively.polite.limiter import Limiter
from lively.polite.robots import Robots
from lively.sched import leases
from lively.sched.worker import blocked_delay
from lively.sync import sync_company

JOB_URL = httpx.URL("https://boards.greenhouse.io/acme/jobs/1")


class Counter:
    """Inner transport that records every request that would hit the network."""

    def __init__(self, status=200):
        self.requests = []
        self.status = status

    def transport(self):
        def handle(request):
            self.requests.append(request)
            return httpx.Response(self.status, json={"jobs": []}, text=None)
        return httpx.MockTransport(handle)


def seed_jobs(pg, n, company_due=False):
    pg.q(f"""
        WITH c AS (INSERT INTO companies (source, slug, name, board_url, next_sync_at)
                   VALUES ('greenhouse', 'acme', 'Acme', 'https://boards.greenhouse.io/acme',
                           now() + interval '{0 if company_due else 1} day') RETURNING id)
        INSERT INTO jobs (company_id, source, external_id, url, title, content_hash, fingerprint)
        SELECT c.id, 'greenhouse', g::text, 'https://boards.greenhouse.io/acme/jobs/' || g,
               'T', 'h', 'f' || g
        FROM c, generate_series(1, %s) g
    """, (n,))
    return [r[0] for r in pg.q("SELECT id FROM jobs ORDER BY id")]


# --- metrics fail open ----------------------------------------------------------


def test_metrics_failures_are_logged_not_raised(redis_down, caplog, monkeypatch):
    monkeypatch.setattr(metrics, "_failures", {})

    async def go():
        await metrics.record_check("live", True)
        await metrics.count_request("example.com", False)
        await metrics.count_blocked("robots", "example.com")
        await metrics.count_reclaimed(3)
        await metrics.count_lost_lease()
        await metrics.count_deferred()
        await metrics.count_worker_error("job")
        await metrics.count_sync({"inserted": 2})
        await metrics.record_check("live", False)  # second failure: rate-limited log

    with caplog.at_level(logging.WARNING, logger="lively.metrics"):
        asyncio.run(go())
    msgs = [r.getMessage() for r in caplog.records]
    assert any("metrics.record_check failed" in m and "ConnectionError" in m for m in msgs)
    assert sum("metrics.record_check" in m for m in msgs) == 1  # not one log line per failure
    assert len(msgs) == 8


def test_metrics_read_path_still_reports_errors(redis_down):
    # snapshot() is not best-effort: the metrics endpoint should show the outage.
    with pytest.raises(RedisConnectionError):
        asyncio.run(metrics.snapshot())


# --- politeness fails closed -----------------------------------------------------


def test_limiter_fails_closed(redis_down):
    with pytest.raises(PolitenessUnavailable, match="rate limiter unavailable"):
        asyncio.run(Limiter().acquire("example.com"))
    assert issubclass(PolitenessUnavailable, Blocked)


def test_breaker_fails_closed(redis_down):
    with pytest.raises(PolitenessUnavailable, match="circuit breaker unavailable"):
        asyncio.run(Breaker().before("example.com"))


def test_robots_cache_miss_fails_closed_without_fetching(redis_down):
    robots = Robots()

    def no_fetch():
        raise AssertionError("robots.txt must not be fetched while Redis is down")

    robots._http = no_fetch
    with pytest.raises(PolitenessUnavailable, match="robots cache unavailable"):
        asyncio.run(robots.allowed(JOB_URL))


def test_robots_fresh_local_cache_is_honoured_expired_is_not(redis_down):
    from urllib.robotparser import RobotFileParser

    parser = RobotFileParser()
    parser.parse("User-agent: *\nDisallow: /private\n".splitlines())
    robots = Robots()
    robots._local["boards.greenhouse.io"] = (time.monotonic() + 60, parser)

    async def go():
        assert await robots.allowed(JOB_URL)
        assert not await robots.allowed(httpx.URL("https://boards.greenhouse.io/private"))
        robots._local["boards.greenhouse.io"] = (time.monotonic() - 1, parser)  # expired
        with pytest.raises(PolitenessUnavailable):
            await robots.allowed(JOB_URL)

    asyncio.run(go())


def test_robots_cache_write_failure_still_enforces_fetched_rules(monkeypatch):
    class HalfRedis:  # reads work (cache miss), writes fail
        async def get(self, key):
            return None

        async def set(self, *a, **kw):
            raise RedisConnectionError("write failed")

    monkeypatch.setattr(redisx, "_client", HalfRedis())
    robots = Robots()

    class FakeHTTP:
        async def get(self, url):
            return httpx.Response(200, text="User-agent: *\nDisallow: /acme/jobs\n")

    robots._http = lambda: FakeHTTP()
    assert asyncio.run(robots.allowed(JOB_URL)) is False


def test_transport_sends_nothing_when_redis_is_down(redis_down):
    counter = Counter()

    async def go():
        async with httpx.AsyncClient(transport=PoliteTransport(counter.transport())) as c:
            for url in (JOB_URL, "https://boards-api.greenhouse.io/v1/boards/acme/jobs"):
                with pytest.raises(PolitenessUnavailable):
                    await c.get(url)

    asyncio.run(go())
    assert counter.requests == []


class StubLimiter:
    def __init__(self, penalize_fails=False):
        self.penalize_fails = penalize_fails

    async def acquire(self, host):
        return 0

    async def penalize(self, host, seconds):
        if self.penalize_fails:
            raise RedisConnectionError("down")


class StubBreaker:
    def __init__(self, fail=False):
        self.fail = fail

    async def before(self, host):
        return False

    async def failure(self, host):
        if self.fail:
            raise RedisConnectionError("down")
        return 1

    async def success(self, host):
        if self.fail:
            raise RedisConnectionError("down")


class StubRobots:
    async def allowed(self, url):
        return True

    async def close(self):
        pass


def test_redis_loss_after_good_response_keeps_the_response(redis_down):
    counter = Counter(200)
    t = PoliteTransport(counter.transport(), StubLimiter(), StubBreaker(fail=True), StubRobots())

    async def go():
        async with httpx.AsyncClient(transport=t) as c:
            return (await c.get(JOB_URL)).status_code

    assert asyncio.run(go()) == 200


@pytest.mark.parametrize("status", [429, 503])
def test_redis_loss_when_backoff_cannot_be_recorded_fails_closed(redis_down, status):
    counter = Counter(status)
    t = PoliteTransport(counter.transport(), StubLimiter(penalize_fails=True),
                        StubBreaker(fail=True), StubRobots())

    async def go():
        async with httpx.AsyncClient(transport=t) as c:
            with pytest.raises(PolitenessUnavailable, match="could not record backoff"):
                await c.get(JOB_URL)

    asyncio.run(go())


def test_politeness_unavailable_defers_instead_of_recording_a_check():
    sig = Signal("http_status", "uncertain", 0.0, "request failed", {"error": "PolitenessUnavailable"})
    delay = blocked_delay(combine([sig]))
    assert delay is not None and 60 <= delay <= 120


# --- recovery ---------------------------------------------------------------------


def _real_redis_or_skip():
    async def ping():
        r = aioredis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=1)
        try:
            await r.ping()
            return True
        except Exception:
            return False
        finally:
            await r.aclose()

    if not asyncio.run(ping()):
        pytest.skip("redis not reachable")


def test_normal_operation_resumes_after_redis_recovers(redis_down, monkeypatch):
    _real_redis_or_skip()
    host = f"recover-{time.time_ns()}"
    counter = Counter(200)
    transport = PoliteTransport(counter.transport(), robots=StubRobots())

    async def go():
        async with httpx.AsyncClient(transport=transport) as c:
            with pytest.raises(PolitenessUnavailable):
                await c.get(f"https://{host}/x")
            assert counter.requests == []
            # Redis comes back (same objects, no restart)
            monkeypatch.setattr(redisx, "_client", None)
            redisx._scripts.clear()
            assert (await c.get(f"https://{host}/x")).status_code == 200
            assert len(counter.requests) == 1
            assert await Breaker().before(host) is False
            assert await Limiter().acquire(host) == 0
        await redisx.close_redis()

    asyncio.run(go())


# --- worker reliability (DB) ------------------------------------------------------


def test_redis_outage_during_sync_does_not_deactivate_company(pg, redis_down):
    pg.q("INSERT INTO companies (source, slug, name, board_url, lease_owner, lease_until) "
         "VALUES ('greenhouse', 'acme', 'Acme', 'x', 'tok', now() + interval '1 minute')")
    cid = pg.q("SELECT id FROM companies")[0][0]
    counter = Counter()

    async def go():
        async with httpx.AsyncClient(transport=PoliteTransport(counter.transport())) as c:
            results = [await sync_company(c, (cid, "greenhouse", "acme")) for _ in range(5)]
        await leases.finish_company(cid, "tok", results[-1], True)
        return results

    assert pg.run(go) == [{"blocked": 1}] * 5
    assert counter.requests == []
    row = pg.q("""SELECT active, fail_count, lease_owner, extract(epoch FROM next_sync_at - now())
                  FROM companies""")[0]
    assert row[:3] == (True, 0, None)
    assert 500 < row[3] < 700  # blocked boards retry in ~10 min


def test_worker_with_redis_down_defers_jobs_and_sends_nothing(pg, monkeypatch, redis_down):
    from lively.liveness.engine import Engine
    from lively.liveness.boards import BoardCache
    import lively.sched.worker as worker_mod

    ids = seed_jobs(pg, 4)
    counter = Counter()
    client = httpx.AsyncClient(transport=PoliteTransport(counter.transport()))
    alive = {}

    # real Engine with all six real checkers, through the real polite transport
    monkeypatch.setattr(worker_mod, "Engine", lambda c, boards=None: Engine(c, boards=BoardCache(ttl=1)))
    monkeypatch.setattr(worker_mod, "make_polite_client", lambda n: client)

    async def go():
        wk = worker_mod.Worker(concurrency=2, claim_size=10, lease=30)
        task = asyncio.create_task(wk.run())
        try:
            for _ in range(100):
                async with leases.pool.connection() as conn:
                    cur = await conn.execute(
                        "SELECT bool_and(lease_owner IS NULL AND next_check_at > now() + interval '30 seconds') FROM jobs")
                    if (await cur.fetchone())[0]:
                        break
                await asyncio.sleep(0.2)
            alive["consumers"] = all(not t.done() for t in wk.consumers)
        finally:
            wk.stop.set()
            await asyncio.wait_for(task, 40)

    pg.run(go)
    assert alive["consumers"]
    assert counter.requests == []                       # nothing reached the network
    assert pg.q("SELECT count(*) FROM job_checks") == [(0,)]  # no bogus 'uncertain' results
    rows = pg.q("SELECT check_count, lease_owner, extract(epoch FROM next_check_at - now()) FROM jobs")
    assert len(rows) == len(ids)
    assert all(c == 0 and o is None and 30 < d <= 125 for c, o, d in rows)


def test_one_failing_job_does_not_stop_others_even_with_metrics_down(pg, monkeypatch, redis_down):
    ids = seed_jobs(pg, 6)
    bad = ids[0]
    engine = FakeEngine(None, fail_ids=[bad])
    alive = {}

    async def check_alive(w):
        alive["ok"] = all(not t.done() for t in w.consumers) and not w.beat.done()

    run_worker_until(
        pg, monkeypatch,
        f"SELECT bool_and(check_count = 1) FILTER (WHERE id <> {bad}) "
        f"AND bool_and(lease_owner IS NULL) FROM jobs",
        engine=engine, concurrency=2, before_stop=check_alive,
    )
    assert alive["ok"]
    good = pg.q(f"SELECT count(*) FROM jobs WHERE id <> {bad} AND state = 'live' AND check_count = 1")
    assert good == [(5,)]
    # failed job: not recorded as a check, lease released, retried later (not hot-looped)
    row = pg.q(f"SELECT check_count, lease_owner, extract(epoch FROM next_check_at - now()) FROM jobs WHERE id = {bad}")[0]
    assert row[0] == 0 and row[1] is None and row[2] > 250
    assert engine.checked.count(bad) == 1


def test_dead_consumer_is_detected_and_replaced(pg, monkeypatch, caplog):
    seed_jobs(pg, 1)
    state = {}

    async def kill_one(w):
        state["old"] = w.consumers[0]
        w.consumers[0].cancel()
        await asyncio.sleep(0.5)
        for _ in range(50):
            if w.consumers[0] is not state["old"]:
                break
            await asyncio.sleep(0.1)
        state["new_alive"] = w.consumers[0] is not state["old"] and not w.consumers[0].done()

    with caplog.at_level(logging.ERROR, logger="lively.worker"):
        run_worker_until(pg, monkeypatch, "SELECT bool_and(check_count = 1) FROM jobs",
                         before_stop=kill_one)
    assert state["new_alive"]
    assert any("consumer 0 exited unexpectedly" in r.getMessage() for r in caplog.records)


def test_heartbeat_failure_is_logged_and_heartbeat_survives(pg, monkeypatch, caplog):
    seed_jobs(pg, 1)

    async def broken_renew(*a, **kw):
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(leases, "renew", broken_renew)
    state = {}

    async def wait_beats(w):
        await asyncio.sleep(1.0)  # several heartbeat periods at lease=0.9
        state["beat_alive"] = not w.beat.done()

    with caplog.at_level(logging.WARNING, logger="lively.worker"):
        run_worker_until(pg, monkeypatch, "SELECT bool_and(check_count = 1) FROM jobs",
                         lease=0.9, before_stop=wait_beats)
    assert state["beat_alive"]
    assert sum("lease renewal failed" in r.getMessage() for r in caplog.records) >= 2


def test_lease_expiry_lets_another_worker_recover_and_fences_the_dead_one(pg):
    seed_jobs(pg, 3)
    ok = combine([Signal("ats_api", "live", 0.95, "listed")])

    async def go():
        dead, _ = await leases.claim_jobs("worker-A", 10, 1)  # A "crashes" holding 3 leases
        assert len(dead) == 3
        again, _ = await leases.claim_jobs("worker-B", 10, 1)
        assert again == []                                   # still leased: nobody steals it
        await asyncio.sleep(1.3)
        got, reclaimed = await leases.claim_jobs("worker-B", 10, 30)
        assert len(got) == 3 and reclaimed == 3
        assert await leases.complete_job(dead[0], "worker-A", ok) is None   # stale result discarded
        assert await leases.complete_job(got[0], "worker-B", ok) is not None

    pg.run(go)
    assert pg.q("SELECT count(*) FROM job_checks WHERE lease_token = 'worker-A'") == [(0,)]
    assert pg.q("SELECT count(*) FROM job_checks WHERE checker = 'combined'") == [(1,)]


class HangingEngine:
    def __init__(self):
        self.started = asyncio.Event()
        self.checked = []

    async def check(self, job):
        self.checked.append(job.id)
        self.started.set()
        await asyncio.Event().wait()  # never finishes


def test_shutdown_releases_queued_and_in_flight_leases(pg, monkeypatch):
    import lively.sched.worker as worker_mod
    from conftest import DummyClient

    seed_jobs(pg, 5)
    engine = HangingEngine()
    monkeypatch.setattr(worker_mod, "Engine", lambda c, boards=None: engine)
    monkeypatch.setattr(worker_mod, "make_polite_client", lambda n: DummyClient())

    async def go():
        engine.started = asyncio.Event()
        w = worker_mod.Worker(concurrency=1, claim_size=5, lease=30)
        w.shutdown_timeout = 0.5
        task = asyncio.create_task(w.run())
        await asyncio.wait_for(engine.started.wait(), 10)
        async with leases.pool.connection() as conn:
            cur = await conn.execute("SELECT count(*) FROM jobs WHERE lease_owner IS NOT NULL")
            held = (await cur.fetchone())[0]
        w.stop.set()
        await asyncio.wait_for(task, 10)
        return held

    assert pg.run(go) == 5          # 1 in flight + 4 queued were leased
    assert len(engine.checked) == 1
    assert pg.q("SELECT count(*) FROM jobs WHERE lease_owner IS NOT NULL OR lease_until IS NOT NULL") == [(0,)]
    assert pg.q("SELECT count(*) FROM jobs WHERE check_count = 0 AND next_check_at <= now()") == [(5,)]
