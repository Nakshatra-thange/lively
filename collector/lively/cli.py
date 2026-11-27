import argparse
import asyncio
import csv
import logging
from pathlib import Path

from .config import CONCURRENCY
from .db import pool
from .discover import run_discover
from .liveness import Engine, store
from .liveness.resolve import resolve
from .liveness.runner import check_batch
from .metrics import format_snapshot, serve, snapshot
from .migrate import run_migrations
from .polite.client import make_polite_client
from .polite.errors import blocked_by
from .redisx import close_redis
from .sched.worker import Worker
from .sources import SOURCES
from .sync import run_sync

STATES = ["new", "live", "suspect", "closed", "reposted"]

SEEDS = Path(__file__).resolve().parent.parent / "seeds"
BOOTSTRAP_KEY = "bootstrap_completed"
BOOTSTRAP_LOCK = 727002


async def seed(path):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    values = []
    for r in rows:
        source = r["source"].strip()
        slug = r["slug"].strip()
        values.append((source, slug, r["name"].strip(), SOURCES[source].board_url(slug)))
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO companies (source, slug, name, board_url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source, slug) DO NOTHING
                """,
                values,
            )
    print(f"seeded {len(values)} rows")


class BootstrapError(RuntimeError):
    pass


async def bootstrap_step(name, coro):
    try:
        return await coro
    except BootstrapError:
        raise
    except Exception as exc:
        raise BootstrapError(
            f"bootstrap failed during {name}: {exc!r}; rerun `lively bootstrap` to resume"
        ) from exc


async def set_state(conn, key, value):
    await conn.execute(
        """
        INSERT INTO app_state (key, value) VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
        """,
        (key, value),
    )
    await conn.commit()


async def bootstrap(boards, candidates, concurrency=CONCURRENCY, force=False):
    """One-time first-run setup: migrate, seed, discover, initial sync.

    Skips if it already completed, or if the database already has companies
    (an existing installation). Every step is additive (ON CONFLICT DO NOTHING
    inserts, normal sync), so --force re-runs them without deleting anything.
    """
    await run_migrations()
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT pg_try_advisory_lock(%s)", (BOOTSTRAP_LOCK,))
        got_lock = (await cur.fetchone())[0]
        await conn.commit()
        if not got_lock:
            raise SystemExit("another bootstrap is already running")
        try:
            cur = await conn.execute("SELECT key, value FROM app_state WHERE key LIKE 'bootstrap_%'")
            state = dict(await cur.fetchall())
            cur = await conn.execute("SELECT count(*) FROM companies")
            companies = (await cur.fetchone())[0]
            await conn.commit()

            if not force and BOOTSTRAP_KEY in state:
                print(f"bootstrap already completed ({state[BOOTSTRAP_KEY]}); use --force to run it again")
                return "skipped"
            if not force and companies and "bootstrap_started" not in state:
                mark = "existing"
                print(f"database already has {companies} companies; recording it as bootstrapped without changes")
            else:
                mark = "fresh"
                await set_state(conn, "bootstrap_started", "1")
                # Same politeness controls (robots, per-domain limiter, breaker) as the worker.
                async with make_polite_client(concurrency) as client:
                    await bootstrap_step("seed", seed(boards))
                    found = await bootstrap_step(
                        "discover", run_discover(candidates, concurrency, client=client)
                    )
                    totals = await bootstrap_step(
                        "sync", run_sync(concurrency=concurrency, client=client)
                    )
                synced = totals["boards"] - totals.get("errors", 0) - totals.get("blocked", 0)
                print(f"discovered {found} boards | initial sync {totals}")
                if synced <= 0:
                    raise BootstrapError(
                        f"bootstrap failed during sync: 0 of {totals['boards']} boards synced "
                        f"(errors {totals.get('errors', 0)}, blocked {totals.get('blocked', 0)}); "
                        "check network/Redis, then rerun `lively bootstrap` to resume"
                    )
                if synced < totals["boards"]:
                    print(f"warning: {totals['boards'] - synced} boards failed initial sync; "
                          "workers will retry them on their sync schedule")

            await set_state(conn, BOOTSTRAP_KEY, mark)
            print(f"bootstrap complete ({mark})")
            return mark
        finally:
            await conn.rollback()
            await conn.execute("SELECT pg_advisory_unlock(%s)", (BOOTSTRAP_LOCK,))
            await conn.commit()


async def stats():
    async with pool.connection() as conn:
        queries = [
            ("boards by source", "SELECT source::text, count(*) FROM companies WHERE active GROUP BY 1 ORDER BY 1"),
            ("jobs by source", "SELECT source::text, count(*) FROM jobs GROUP BY 1 ORDER BY 1"),
            ("jobs by state", "SELECT state::text, count(*) FROM jobs GROUP BY 1 ORDER BY 1"),
            ("total jobs", "SELECT count(*) FROM jobs"),
            ("failing boards", "SELECT count(*) FROM companies WHERE fail_count > 0"),
            ("combined checks by verdict", "SELECT verdict::text, count(*) FROM job_checks WHERE checker = 'combined' GROUP BY 1 ORDER BY 1"),
            ("signals by checker and verdict", "SELECT checker, verdict::text, count(*) FROM job_checks WHERE checker <> 'combined' GROUP BY 1, 2 ORDER BY 1, 2"),
            ("status events by transition", "SELECT coalesce(from_state::text, '-') || ' -> ' || to_state::text, count(*) FROM status_events GROUP BY 1 ORDER BY 2 DESC"),
        ]
        for title, query in queries:
            cur = await conn.execute(query)
            print(f"\n{title}")
            for row in await cur.fetchall():
                print("  ", *row)


def show(job, result, new_state):
    print(f"\n{job.title or job.url}")
    print(f"  url         {job.url}")
    print(f"  verdict     {result.verdict} (confidence {result.confidence:.2f})")
    if new_state is not None:
        print(f"  state       {job.state} -> {new_state}")
    print("  reasons")
    for reason in result.reasons:
        print(f"    - {reason}")
    print("  signals")
    for s in result.signals:
        print(f"    {s.checker:<14} {s.verdict:<10} {s.confidence:.2f}  {s.reason}")


async def check_one(args):
    if args.job_id:
        job = await store.get_job(args.job_id)
    else:
        job = await resolve(args.url)
    if job is None:
        print("job not found or link not supported")
        return
    async with make_polite_client(1) as client:
        result = await Engine(client).check(job)
    blocked = blocked_by(result)
    if blocked:
        show(job, result, None)
        print(f"\n  BLOCKED by politeness controls ({blocked}); nothing was saved, retry later")
        return blocked
    new_state = await store.save_result(job, result)
    show(job, result, new_state)
    return result.verdict


async def dispatch(args):
    await pool.open()
    try:
        if args.command == "seed":
            await seed(args.path)
        elif args.command == "discover":
            await run_discover(args.path, args.concurrency)
        elif args.command == "sync":
            await run_sync(args.source, args.limit, args.stale_hours, args.concurrency)
        elif args.command == "check":
            if args.job_id or args.url:
                await check_one(args)
            else:
                await check_batch(args.state or ["new", "live", "suspect"], args.limit, args.concurrency)
        elif args.command == "stats":
            await stats()
        elif args.command == "migrate":
            await run_migrations()
        elif args.command == "bootstrap":
            await bootstrap(args.boards, args.candidates, args.concurrency, args.force)
        elif args.command == "worker":
            await Worker(args.concurrency).run()
        elif args.command == "metrics":
            print(format_snapshot(await snapshot()))
        elif args.command == "serve-metrics":
            await serve(args.host, args.port)
    finally:
        await pool.close()
        await close_redis()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="lively")
    sub = parser.add_subparsers(dest="command", required=True)

    p_seed = sub.add_parser("seed")
    p_seed.add_argument("path")

    p_discover = sub.add_parser("discover")
    p_discover.add_argument("path")
    p_discover.add_argument("--concurrency", type=int, default=CONCURRENCY)

    p_sync = sub.add_parser("sync")
    p_sync.add_argument("--source", choices=list(SOURCES))
    p_sync.add_argument("--limit", type=int)
    p_sync.add_argument("--stale-hours", type=int, default=0)
    p_sync.add_argument("--concurrency", type=int, default=CONCURRENCY)

    p_check = sub.add_parser("check")
    p_check.add_argument("--job-id", type=int)
    p_check.add_argument("--url")
    p_check.add_argument("--state", action="append", choices=STATES)
    p_check.add_argument("--limit", type=int, default=100)
    p_check.add_argument("--concurrency", type=int, default=CONCURRENCY)

    sub.add_parser("stats")
    sub.add_parser("migrate")
    sub.add_parser("metrics")

    p_boot = sub.add_parser("bootstrap")
    p_boot.add_argument("--boards", default=str(SEEDS / "boards.csv"))
    p_boot.add_argument("--candidates", default=str(SEEDS / "candidates.txt"))
    p_boot.add_argument("--concurrency", type=int, default=CONCURRENCY)
    p_boot.add_argument("--force", action="store_true")

    p_worker = sub.add_parser("worker")
    p_worker.add_argument("--concurrency", type=int, default=CONCURRENCY)

    p_serve = sub.add_parser("serve-metrics")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=9100)

    try:
        asyncio.run(dispatch(parser.parse_args()))
    except BootstrapError as exc:
        logging.getLogger("lively.bootstrap").error("%s", exc, exc_info=exc.__cause__)
        raise SystemExit(1)


if __name__ == "__main__":
    main()