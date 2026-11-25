from dataclasses import dataclass

from psycopg.types.json import Jsonb

from ..config import BASE_INTERVAL, TIME_SCALE
from ..db import pool
from ..liveness.models import JobRef
from ..liveness.store import next_state
from .policy import ewma, jitter, job_interval, sync_interval

CLAIM_JOBS = """
WITH picked AS (
    SELECT j.id, (j.lease_owner IS NOT NULL) AS reclaimed
    FROM jobs j
    JOIN companies c ON c.id = j.company_id
    WHERE j.state IN ('new', 'live', 'suspect', 'reposted')
      AND j.next_check_at <= now()
      AND (j.lease_until IS NULL OR j.lease_until < now())
      AND c.active
    ORDER BY CASE j.state::text WHEN 'suspect' THEN 0 WHEN 'new' THEN 1 ELSE 2 END,
             j.next_check_at
    LIMIT %s
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE jobs j SET
    lease_owner = %s,
    lease_until = now() + make_interval(secs => %s::float8)
FROM picked p, companies c
WHERE j.id = p.id AND c.id = j.company_id
RETURNING j.id, j.company_id, j.source::text, c.slug, j.external_id, j.url, j.apply_url,
          j.title, j.state::text, p.reclaimed
"""

CLAIM_COMPANIES = """
WITH picked AS (
    SELECT id FROM companies
    WHERE active
      AND next_sync_at <= now()
      AND (lease_until IS NULL OR lease_until < now())
    ORDER BY next_sync_at
    LIMIT %s
    FOR UPDATE SKIP LOCKED
)
UPDATE companies c SET
    lease_owner = %s,
    lease_until = now() + make_interval(secs => %s::float8)
FROM picked
WHERE c.id = picked.id
RETURNING c.id, c.source::text, c.slug, c.last_synced_at IS NULL, c.change_rate, c.fail_count
"""

RENEW_JOBS = """
UPDATE jobs SET lease_until = now() + make_interval(secs => %s::float8)
WHERE (id, lease_owner) IN (SELECT a, b FROM unnest(%s::bigint[], %s::text[]) AS t(a, b))
"""

RENEW_COMPANIES = """
UPDATE companies SET lease_until = now() + make_interval(secs => %s::float8)
WHERE (id, lease_owner) IN (SELECT a, b FROM unnest(%s::bigint[], %s::text[]) AS t(a, b))
"""

RELEASE_JOBS = """
UPDATE jobs SET lease_owner = NULL, lease_until = NULL
WHERE (id, lease_owner) IN (SELECT a, b FROM unnest(%s::bigint[], %s::text[]) AS t(a, b))
"""

RELEASE_COMPANIES = """
UPDATE companies SET lease_owner = NULL, lease_until = NULL
WHERE (id, lease_owner) IN (SELECT a, b FROM unnest(%s::bigint[], %s::text[]) AS t(a, b))
"""

LOCK_JOB = """
SELECT state::text, content_hash, seen_hash, check_count, check_interval_seconds,
       stable_streak, last_verdict,
       (SELECT change_rate FROM companies WHERE id = jobs.company_id)
FROM jobs
WHERE id = %s AND lease_owner = %s
FOR UPDATE
"""

INSERT_CHECK = """
INSERT INTO job_checks
    (job_id, checker, verdict, confidence, http_status, latency_ms, reason, details, lease_token)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

UPDATE_JOB = """
UPDATE jobs SET
    state = %s,
    last_checked_at = now(),
    next_check_at = now() + make_interval(secs => %s::float8),
    check_interval_seconds = %s,
    check_count = check_count + 1,
    change_count = change_count + %s,
    stable_streak = %s,
    last_changed_at = CASE WHEN %s THEN now() ELSE last_changed_at END,
    last_verdict = %s,
    seen_hash = content_hash,
    closed_at = CASE WHEN %s THEN COALESCE(closed_at, now()) ELSE NULL END,
    lease_owner = NULL,
    lease_until = NULL,
    updated_at = now()
WHERE id = %s
"""

INSERT_EVENT = """
INSERT INTO status_events (job_id, from_state, to_state, reason) VALUES (%s, %s, %s, %s)
"""


@dataclass
class Outcome:
    old_state: str
    new_state: str
    changed: bool
    interval: int


async def claim_jobs(token, limit, lease):
    async with pool.connection() as conn:
        cur = await conn.execute(CLAIM_JOBS, (limit, token, lease))
        rows = await cur.fetchall()
    return [JobRef(*r[:9]) for r in rows], sum(1 for r in rows if r[9])


async def claim_companies(token, limit, lease):
    async with pool.connection() as conn:
        cur = await conn.execute(CLAIM_COMPANIES, (limit, token, lease))
        return await cur.fetchall()


async def renew(lease, jobs, companies):
    async with pool.connection() as conn:
        if jobs:
            await conn.execute(RENEW_JOBS, (lease, [i for i, _ in jobs], [t for _, t in jobs]))
        if companies:
            await conn.execute(
                RENEW_COMPANIES, (lease, [i for i, _ in companies], [t for _, t in companies])
            )


async def release(jobs, companies):
    async with pool.connection() as conn:
        if jobs:
            await conn.execute(RELEASE_JOBS, ([i for i, _ in jobs], [t for _, t in jobs]))
        if companies:
            await conn.execute(
                RELEASE_COMPANIES, ([i for i, _ in companies], [t for _, t in companies])
            )


async def defer_job(job_id, token, seconds):
    async with pool.connection() as conn:
        await conn.execute(
            """
            UPDATE jobs SET
                next_check_at = now() + make_interval(secs => %s::float8),
                lease_owner = NULL,
                lease_until = NULL
            WHERE id = %s AND lease_owner = %s
            """,
            (seconds, job_id, token),
        )


async def complete_job(job, token, result):
    async with pool.connection() as conn:
        cur = await conn.execute(LOCK_JOB, (job.id, token))
        row = await cur.fetchone()
        if row is None:
            return None
        state, chash, seen, count, interval, streak, last_verdict, rate = row

        new_state = next_state(state, result)
        conclusive = result.verdict in ("live", "closed")
        content_changed = seen is not None and seen != chash
        verdict_flip = (
            conclusive and last_verdict in ("live", "closed") and last_verdict != result.verdict
        )
        moved = new_state != state and state != "new"
        changed = content_changed or verdict_flip or moved
        streak = 0 if changed else streak + 1

        current = BASE_INTERVAL if count == 0 else interval
        new_interval = job_interval(
            new_state, result.verdict, changed, current, rate or 0.0, streak
        )
        delay = max(5.0, jitter(new_interval) / TIME_SCALE)
        last = result.verdict if conclusive else last_verdict

        rows = [
            (
                job.id,
                s.checker,
                s.verdict,
                s.confidence,
                s.http_status,
                s.latency_ms,
                s.reason[:300],
                Jsonb(s.details),
                token,
            )
            for s in result.signals
        ]
        rows.append(
            (
                job.id,
                "combined",
                result.verdict,
                result.confidence,
                result.http_status,
                result.latency_ms,
                "; ".join(result.reasons)[:300],
                Jsonb(result.to_dict()),
                token,
            )
        )
        async with conn.cursor() as c:
            await c.executemany(INSERT_CHECK, rows)
            await c.execute(
                UPDATE_JOB,
                (
                    new_state,
                    delay,
                    new_interval,
                    1 if changed else 0,
                    streak,
                    changed,
                    last,
                    new_state == "closed",
                    job.id,
                ),
            )
            if new_state != state:
                reason = f"{'; '.join(result.reasons[:3])} (confidence {result.confidence:.2f})"
                await c.execute(INSERT_EVENT, (job.id, state, new_state, reason[:500]))
    return Outcome(state, new_state, changed, new_interval)


async def finish_company(company_id, token, stats, first):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT job_count, fail_count, change_rate FROM companies
            WHERE id = %s AND lease_owner = %s
            FOR UPDATE
            """,
            (company_id, token),
        )
        row = await cur.fetchone()
        if row is None:
            return False
        job_count, fails, rate = row

        if stats.get("blocked"):
            new_rate, interval = rate, 600
        elif stats.get("errors"):
            new_rate, interval = rate, sync_interval(rate, fails)
        else:
            if first:
                new_rate = rate
            else:
                changes = (
                    stats.get("inserted", 0) + stats.get("suspect", 0) + stats.get("reappeared", 0)
                )
                new_rate = ewma(rate, min(1.0, changes / max(job_count, 10)))
            interval = sync_interval(new_rate, 0)

        delay = max(5.0, jitter(interval) / TIME_SCALE)
        await conn.execute(
            """
            UPDATE companies SET
                change_rate = %s,
                sync_interval_seconds = %s,
                next_sync_at = now() + make_interval(secs => %s::float8),
                lease_owner = NULL,
                lease_until = NULL
            WHERE id = %s
            """,
            (new_rate, interval, delay, company_id),
        )
    return True