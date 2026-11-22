import argparse
import asyncio
import csv
import logging

from .config import CONCURRENCY
from .db import pool
from .discover import run_discover
from .sources import SOURCES
from .sync import run_sync


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


async def stats():
    async with pool.connection() as conn:
        queries = [
            ("boards by source", "SELECT source::text, count(*) FROM companies WHERE active GROUP BY 1 ORDER BY 1"),
            ("jobs by source", "SELECT source::text, count(*) FROM jobs GROUP BY 1 ORDER BY 1"),
            ("jobs by state", "SELECT state::text, count(*) FROM jobs GROUP BY 1 ORDER BY 1"),
            ("total jobs", "SELECT count(*) FROM jobs"),
            ("failing boards", "SELECT count(*) FROM companies WHERE fail_count > 0"),
        ]
        for title, query in queries:
            cur = await conn.execute(query)
            print(f"\n{title}")
            for row in await cur.fetchall():
                print("  ", *row)


async def dispatch(args):
    await pool.open()
    try:
        if args.command == "seed":
            await seed(args.path)
        elif args.command == "discover":
            await run_discover(args.path, args.concurrency)
        elif args.command == "sync":
            await run_sync(args.source, args.limit, args.stale_hours, args.concurrency)
        elif args.command == "stats":
            await stats()
    finally:
        await pool.close()


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

    sub.add_parser("stats")

    asyncio.run(dispatch(parser.parse_args()))


if __name__ == "__main__":
    main()
