import asyncio
import logging
from collections import Counter

import httpx

from .config import CONCURRENCY, FAIL_DEACTIVATE_AFTER, HTTP_TIMEOUT, USER_AGENT
from .db import pool
from .normalize import content_hash, fingerprint
from .sources import SOURCES

log = logging.getLogger("lively.sync")

INSERT_JOB = """
INSERT INTO jobs (
    company_id, source, external_id, url, apply_url, title, location, department,
    employment_type, remote, description, content_hash, fingerprint, posted_at,
    state, repost_of, repost_count
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING id
"""

UPDATE_JOB = """
UPDATE jobs SET
    title = %s, url = %s, apply_url = %s, location = %s, department = %s,
    employment_type = %s, remote = %s, description = %s, content_hash = %s,
    fingerprint = %s, posted_at = COALESCE(%s, posted_at),
    last_seen_at = now(), updated_at = now()
WHERE id = %s
"""

INSERT_EVENT = """
INSERT INTO status_events (job_id, from_state, to_state, reason) VALUES (%s, %s, %s, %s)
"""


async def record_failure(company_id, message):
    async with pool.connection() as conn:
        await conn.execute(
            """
            UPDATE companies SET
                fail_count = fail_count + 1,
                last_error = %s,
                last_synced_at = now(),
                active = (fail_count + 1) < %s
            WHERE id = %s
            """,
            (message[:300], FAIL_DEACTIVATE_AFTER, company_id),
        )


async def sync_company(client, company):
    company_id, source_name, slug = company
    source = SOURCES[source_name]
    try:
        raw = await source.fetch(client, slug)
    except Exception as exc:
        await record_failure(company_id, str(exc))
        return {"errors": 1}
    if raw is None:
        await record_failure(company_id, "board not found")
        return {"errors": 1}

    stats = Counter()
    feed_ids = {j.external_id for j in raw}

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, external_id, state::text, content_hash, fingerprint, repost_count
                FROM jobs WHERE company_id = %s
                """,
                (company_id,),
            )
            existing = {r[1]: r for r in await cur.fetchall()}

            gone_by_fp = {}
            for r in existing.values():
                if r[1] in feed_ids:
                    continue
                prev = gone_by_fp.get(r[4])
                if prev is None or r[5] > prev[5]:
                    gone_by_fp[r[4]] = r

            updates = []
            reappeared = []
            events = []

            for j in raw:
                ch = content_hash(j.title, j.description)
                fp = fingerprint(company_id, j.title, j.location, j.description)
                row = existing.get(j.external_id)
                if row:
                    updates.append(
                        (
                            j.title, j.url, j.apply_url, j.location, j.department,
                            j.employment_type, j.remote, j.description, ch, fp,
                            j.posted_at, row[0],
                        )
                    )
                    if row[2] in ("suspect", "closed"):
                        reappeared.append((row[0], row[2]))
                    continue

                root = gone_by_fp.get(fp)
                if root:
                    state, repost_of, repost_count = "reposted", root[0], root[5] + 1
                    reason = f"same content as earlier listing {root[1]}"
                    stats["reposted"] += 1
                else:
                    state, repost_of, repost_count = "new", None, 0
                    reason = "first seen on company board"

                await cur.execute(
                    INSERT_JOB,
                    (
                        company_id, source_name, j.external_id, j.url, j.apply_url, j.title,
                        j.location, j.department, j.employment_type, j.remote,
                        j.description, ch, fp, j.posted_at, state, repost_of, repost_count,
                    ),
                )
                new_id = (await cur.fetchone())[0]
                events.append((new_id, None, state, reason))
                stats["inserted"] += 1

            missing = [
                r for r in existing.values()
                if r[1] not in feed_ids and r[2] in ("new", "live", "reposted")
            ]

            if updates:
                await cur.executemany(UPDATE_JOB, updates)
                stats["updated"] += len(updates)

            if reappeared:
                ids = [r[0] for r in reappeared]
                await cur.execute(
                    """
                    UPDATE jobs SET state = 'live', closed_at = NULL, updated_at = now()
                    WHERE id = ANY(%s::bigint[])
                    """,
                    (ids,),
                )
                events.extend(
                    (jid, prev, "live", "reappeared on company board") for jid, prev in reappeared
                )
                stats["reappeared"] += len(reappeared)

            if missing:
                ids = [r[0] for r in missing]
                await cur.execute(
                    """
                    UPDATE jobs SET state = 'suspect', next_check_at = now(), updated_at = now()
                    WHERE id = ANY(%s::bigint[])
                    """,
                    (ids,),
                )
                events.extend(
                    (r[0], r[2], "suspect", "missing from company board listing") for r in missing
                )
                stats["suspect"] += len(missing)

            if events:
                await cur.executemany(INSERT_EVENT, events)

            await cur.execute(
                """
                UPDATE companies SET
                    last_synced_at = now(), job_count = %s, fail_count = 0, last_error = NULL
                WHERE id = %s
                """,
                (len(raw), company_id),
            )

    return dict(stats)


async def run_sync(source=None, limit=None, stale_hours=0, concurrency=CONCURRENCY):
    query = "SELECT id, source::text, slug FROM companies WHERE active"
    params = []
    if source:
        query += " AND source = %s"
        params.append(source)
    if stale_hours:
        query += " AND (last_synced_at IS NULL OR last_synced_at < now() - make_interval(hours => %s))"
        params.append(stale_hours)
    query += " ORDER BY last_synced_at NULLS FIRST, id"
    if limit:
        query += " LIMIT %s"
        params.append(limit)

    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        companies = await cur.fetchall()

    log.info("syncing %d boards", len(companies))
    totals = Counter()
    done = 0
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        limits=httpx.Limits(max_connections=concurrency * 2),
    ) as client:

        async def worker(company):
            nonlocal done
            async with sem:
                result = await sync_company(client, company)
            totals.update(result)
            done += 1
            if done % 25 == 0 or done == len(companies):
                log.info("%d/%d boards | %s", done, len(companies), dict(totals))

        await asyncio.gather(*(worker(c) for c in companies))

    log.info("finished | %s", dict(totals))
