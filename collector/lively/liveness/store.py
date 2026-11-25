from psycopg.types.json import Jsonb

from ..db import pool
from .models import JobRef

CLOSE_CONFIDENCE = 0.75

INTERVALS = {
    "live": 86400,
    "new": 86400,
    "reposted": 86400,
    "suspect": 7200,
    "closed": 604800,
}
UNCERTAIN_INTERVAL = 3600

JOB_SELECT = """
SELECT j.id, j.company_id, j.source::text, c.slug, j.external_id, j.url, j.apply_url,
       j.title, j.state::text
FROM jobs j JOIN companies c ON c.id = j.company_id
"""

INSERT_CHECK = """
INSERT INTO job_checks (job_id, checker, verdict, confidence, http_status, latency_ms, reason, details)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

UPDATE_JOB = """
UPDATE jobs SET
    state = %s,
    last_checked_at = now(),
    next_check_at = now() + make_interval(secs => %s::float8),
    closed_at = CASE WHEN %s THEN COALESCE(closed_at, now()) ELSE NULL END,
    updated_at = now()
WHERE id = %s
"""

INSERT_EVENT = """
INSERT INTO status_events (job_id, from_state, to_state, reason) VALUES (%s, %s, %s, %s)
"""


def next_state(current, result):
    if current == "closed":
        return "live" if result.verdict == "live" else "closed"
    if result.verdict == "closed":
        return "closed" if result.confidence >= CLOSE_CONFIDENCE else "suspect"
    if result.verdict == "live":
        return current if current in ("live", "reposted") else "live"
    return current


def next_interval(state, result):
    if result.verdict == "uncertain":
        return UNCERTAIN_INTERVAL
    return INTERVALS.get(state, 86400)


async def get_job(job_id):
    async with pool.connection() as conn:
        cur = await conn.execute(JOB_SELECT + " WHERE j.id = %s", (job_id,))
        row = await cur.fetchone()
    return JobRef(*row) if row else None


async def find_job(url, source, slug, external_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            JOB_SELECT
            + " WHERE j.url = %s OR (j.source::text = %s AND c.slug = %s AND j.external_id = %s) LIMIT 1",
            (url, source, slug, external_id),
        )
        row = await cur.fetchone()
    return JobRef(*row) if row else None


async def due_jobs(states, limit):
    async with pool.connection() as conn:
        cur = await conn.execute(
            JOB_SELECT
            + """
            WHERE j.state::text = ANY(%s) AND j.next_check_at <= now() AND c.active
            ORDER BY (j.state = 'suspect') DESC, random()
            LIMIT %s
            """,
            (list(states), limit),
        )
        rows = await cur.fetchall()
    return [JobRef(*r) for r in rows]


async def save_result(job, result):
    if job.id is None:
        return None
    new_state = next_state(job.state, result)
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
        )
    )
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(INSERT_CHECK, rows)
            await cur.execute(
                UPDATE_JOB,
                (
                    new_state,
                    next_interval(new_state, result),
                    new_state == "closed",
                    job.id,
                ),
            )
            if new_state != job.state:
                reason = f"{'; '.join(result.reasons[:3])} (confidence {result.confidence:.2f})"
                await cur.execute(INSERT_EVENT, (job.id, job.state, new_state, reason[:500]))
    return new_state
