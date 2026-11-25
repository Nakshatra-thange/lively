import asyncio
import json
import time

from .config import TIME_SCALE
from .db import pool
from .redisx import get_redis

BUCKETS = ["<=1h", "<=6h", "<=1d", "<=3d", ">3d"]


def _minute():
    return int(time.time() // 60)


async def count_request(host, error):
    pipe = get_redis().pipeline()
    pipe.hincrby("m:req", host, 1)
    if error:
        pipe.hincrby("m:err", host, 1)
    await pipe.execute()


async def count_blocked(kind, host):
    await get_redis().hincrby(f"m:blocked:{kind}", host, 1)


async def record_check(verdict, closed_now):
    minute = _minute()
    pipe = get_redis().pipeline()
    pipe.incr(f"m:cpm:{minute}")
    pipe.expire(f"m:cpm:{minute}", 7200)
    pipe.incr("m:checks")
    pipe.hincrby("m:verdicts", verdict, 1)
    pipe.set("m:started", str(time.time()), nx=True)
    if closed_now:
        pipe.incr("m:closures")
    await pipe.execute()


async def count_reclaimed(n):
    await get_redis().incrby("m:reclaimed", n)


async def count_lost_lease():
    await get_redis().incr("m:lost")


async def count_deferred():
    await get_redis().incr("m:deferred")


async def count_worker_error(kind):
    await get_redis().hincrby("m:worker_errors", kind, 1)


async def count_sync(stats):
    pipe = get_redis().pipeline()
    pipe.incr("m:syncs")
    for key, value in stats.items():
        if value:
            pipe.hincrby("m:sync", key, value)
    await pipe.execute()


async def reset_metrics():
    r = get_redis()
    for pattern in ("m:*", "cb:*", "pen:*", "rl:*"):
        keys = [k async for k in r.scan_iter(match=pattern, count=500)]
        if keys:
            await r.delete(*keys)


async def _db_snapshot():
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT
                count(*) FILTER (WHERE j.next_check_at <= now()
                                 AND (j.lease_until IS NULL OR j.lease_until < now())),
                count(*) FILTER (WHERE j.lease_until >= now()),
                count(*) FILTER (WHERE j.lease_until < now()),
                count(*),
                COALESCE(sum(86400.0 / GREATEST(j.check_interval_seconds, 1)), 0)::float8
            FROM jobs j JOIN companies c ON c.id = j.company_id
            WHERE j.state IN ('new', 'live', 'suspect', 'reposted') AND c.active
            """
        )
        queue = await cur.fetchone()

        cur = await conn.execute(
            """
            SELECT
                count(*) FILTER (WHERE next_sync_at <= now()
                                 AND (lease_until IS NULL OR lease_until < now())),
                count(*) FILTER (WHERE change_rate > 0.05),
                count(*)
            FROM companies WHERE active
            """
        )
        companies = await cur.fetchone()

        cur = await conn.execute("SELECT state::text, count(*) FROM jobs GROUP BY 1")
        states = {r[0]: r[1] for r in await cur.fetchall()}

        cur = await conn.execute(
            """
            SELECT CASE
                WHEN check_interval_seconds <= 3600 THEN '<=1h'
                WHEN check_interval_seconds <= 21600 THEN '<=6h'
                WHEN check_interval_seconds <= 86400 THEN '<=1d'
                WHEN check_interval_seconds <= 259200 THEN '<=3d'
                ELSE '>3d' END AS bucket,
                count(*)
            FROM jobs WHERE state IN ('new', 'live', 'suspect', 'reposted')
            GROUP BY 1
            """
        )
        found = {r[0]: r[1] for r in await cur.fetchall()}
        intervals = {b: found.get(b, 0) for b in BUCKETS}

        cur = await conn.execute(
            """
            SELECT count(*) FILTER (WHERE to_state = 'closed'),
                   count(*) FILTER (WHERE to_state = 'suspect')
            FROM status_events WHERE created_at > now() - interval '24 hours'
            """
        )
        events = await cur.fetchone()
    return queue, companies, states, intervals, events


async def snapshot():
    r = get_redis()
    minute = _minute()
    pipe = r.pipeline()
    for i in range(6):
        pipe.get(f"m:cpm:{minute - i}")
    for key in ("m:checks", "m:closures", "m:started", "m:reclaimed", "m:lost", "m:deferred"):
        pipe.get(key)
    for key in (
        "m:verdicts",
        "m:req",
        "m:err",
        "m:trips",
        "m:blocked:robots",
        "m:blocked:circuit",
        "m:sync",
        "m:worker_errors",
    ):
        pipe.hgetall(key)
    out = await pipe.execute()

    cpm = [int(v or 0) for v in out[:6]]
    checks, closures, started, reclaimed, lost, deferred = out[6:12]
    verdicts, req, err, trips, robots, circuit, syncs, worker_errors = out[12:20]
    open_circuits = sorted(
        [k[3:-5] async for k in r.scan_iter(match="cb:*:open", count=100)]
    )

    queue, companies, states, intervals, events = await _db_snapshot()
    due, leased, orphaned, active, adaptive = queue
    naive = active
    actual = int(checks or 0)
    elapsed = time.time() - float(started) if started else 0.0
    naive_equivalent = active * (1.0 + elapsed * TIME_SCALE / 86400.0)

    hosts = set(req) | set(err) | set(trips) | set(robots) | set(circuit)
    domains = {
        h: {
            "requests": int(req.get(h, 0)),
            "errors": int(err.get(h, 0)),
            "trips": int(trips.get(h, 0)),
            "robots_blocked": int(robots.get(h, 0)),
            "circuit_blocked": int(circuit.get(h, 0)),
        }
        for h in hosts
    }
    top = sorted(
        domains.items(),
        key=lambda kv: kv[1]["errors"] + kv[1]["circuit_blocked"] + kv[1]["robots_blocked"],
        reverse=True,
    )[:20]

    return {
        "time_scale": TIME_SCALE,
        "queue": {
            "due": due,
            "leased": leased,
            "orphaned": orphaned,
            "active": active,
            "companies_due": companies[0],
            "companies_volatile": companies[1],
            "companies_total": companies[2],
        },
        "states": states,
        "intervals": intervals,
        "throughput": {
            "checks_total": actual,
            "this_minute": cpm[0],
            "checks_per_minute": cpm[1],
            "checks_per_minute_avg5": round(sum(cpm[1:6]) / 5.0, 1),
        },
        "caught": {
            "closures_total": int(closures or 0),
            "closed_24h": events[0],
            "suspect_24h": events[1],
        },
        "verdicts": {k: int(v) for k, v in verdicts.items()},
        "savings": {
            "adaptive_per_day": round(adaptive, 1),
            "naive_per_day": naive,
            "saved_pct_projected": round(100.0 * (1.0 - adaptive / naive), 1) if naive else 0.0,
            "actual_checks": actual,
            "naive_equivalent": round(naive_equivalent),
            "saved_pct_actual": (
                round(100.0 * (1.0 - actual / naive_equivalent), 1) if naive_equivalent else 0.0
            ),
        },
        "reliability": {
            "reclaimed": int(reclaimed or 0),
            "lost_leases": int(lost or 0),
            "deferred": int(deferred or 0),
            "worker_errors": {k: int(v) for k, v in worker_errors.items()},
        },
        "syncs": {k: int(v) for k, v in syncs.items()},
        "open_circuits": open_circuits,
        "domains": dict(top),
    }


def format_snapshot(s):
    q = s["queue"]
    t = s["throughput"]
    c = s["caught"]
    v = s["savings"]
    rel = s["reliability"]
    lines = [
        f"time scale          {s['time_scale']}",
        f"jobs by state       {s['states']}",
        f"queue               due {q['due']} | leased {q['leased']} | orphaned {q['orphaned']} | active {q['active']}",
        f"boards              due {q['companies_due']} | volatile {q['companies_volatile']} | total {q['companies_total']}",
        f"intervals           {s['intervals']}",
        f"throughput          {t['checks_per_minute']}/min last | {t['checks_per_minute_avg5']}/min avg5 | {t['checks_total']} total",
        f"verdicts            {s['verdicts']}",
        f"closures caught     {c['closures_total']} since start | {c['closed_24h']} closed and {c['suspect_24h']} suspect in 24h",
        f"saved vs naive      projected {v['saved_pct_projected']}% ({v['adaptive_per_day']}/day vs {v['naive_per_day']}/day)",
        f"                    actual {v['saved_pct_actual']}% ({v['actual_checks']} checks vs {v['naive_equivalent']} naive)",
        f"reliability         reclaimed {rel['reclaimed']} | lost leases {rel['lost_leases']} | deferred {rel['deferred']} | errors {rel['worker_errors']}",
        f"syncs               {s['syncs']}",
        f"open circuits       {s['open_circuits']}",
        "domains",
    ]
    for host, d in s["domains"].items():
        lines.append(
            f"  {host:<34} req {d['requests']:<7} err {d['errors']:<5} trips {d['trips']:<3} "
            f"robots {d['robots_blocked']:<4} circuit {d['circuit_blocked']}"
        )
    return "\n".join(lines)


def render_prometheus(s):
    q = s["queue"]
    t = s["throughput"]
    c = s["caught"]
    v = s["savings"]
    rel = s["reliability"]
    lines = [
        f"lively_queue_due {q['due']}",
        f"lively_queue_leased {q['leased']}",
        f"lively_queue_orphaned {q['orphaned']}",
        f"lively_jobs_active {q['active']}",
        f"lively_boards_due {q['companies_due']}",
        f"lively_checks_total {t['checks_total']}",
        f"lively_checks_per_minute {t['checks_per_minute']}",
        f"lively_closures_total {c['closures_total']}",
        f"lively_closed_24h {c['closed_24h']}",
        f"lively_adaptive_checks_per_day {v['adaptive_per_day']}",
        f"lively_naive_checks_per_day {v['naive_per_day']}",
        f"lively_saved_percent_projected {v['saved_pct_projected']}",
        f"lively_saved_percent_actual {v['saved_pct_actual']}",
        f"lively_reclaimed_total {rel['reclaimed']}",
        f"lively_lost_leases_total {rel['lost_leases']}",
        f"lively_deferred_total {rel['deferred']}",
    ]
    for state, n in s["states"].items():
        lines.append(f'lively_jobs{{state="{state}"}} {n}')
    for bucket, n in s["intervals"].items():
        lines.append(f'lively_interval_bucket{{bucket="{bucket}"}} {n}')
    for host, d in s["domains"].items():
        lines.append(f'lively_domain_requests_total{{domain="{host}"}} {d["requests"]}')
        lines.append(f'lively_domain_errors_total{{domain="{host}"}} {d["errors"]}')
        lines.append(f'lively_domain_trips_total{{domain="{host}"}} {d["trips"]}')
    for host in s["open_circuits"]:
        lines.append(f'lively_circuit_open{{domain="{host}"}} 1')
    return "\n".join(lines) + "\n"


async def _handle(reader, writer):
    status = "200 OK"
    ctype = "text/plain"
    try:
        line = await asyncio.wait_for(reader.readline(), 5)
        while True:
            header = await asyncio.wait_for(reader.readline(), 5)
            if header in (b"\r\n", b"\n", b""):
                break
        parts = line.decode("latin-1").split()
        path = parts[1].split("?")[0] if len(parts) > 1 else "/"
        if path == "/metrics":
            body = render_prometheus(await snapshot()).encode()
            ctype = "text/plain; version=0.0.4"
        elif path == "/metrics.json":
            body = json.dumps(await snapshot(), default=str).encode()
            ctype = "application/json"
        elif path == "/health":
            body = b"ok"
        else:
            status = "404 Not Found"
            body = b"not found"
        head = (
            f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
            "Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n"
        ).encode()
        writer.write(head + body)
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def serve(host, port):
    server = await asyncio.start_server(_handle, host, port)
    async with server:
        await server.serve_forever()