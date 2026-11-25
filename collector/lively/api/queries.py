from ..db import pool

CORE = """
SELECT j.id, j.title, c.name AS company, j.source::text AS source, j.url, j.apply_url,
       j.location, j.state::text AS state, j.posted_at, j.first_seen_at, j.last_seen_at,
       j.last_checked_at, j.next_check_at, j.check_interval_seconds, j.check_count,
       j.closed_at, j.ghost_score, j.ghost_reasons, j.predicted_label::text AS predicted_label,
       j.repost_count, j.family_size, j.repost_of
FROM jobs j JOIN companies c ON c.id = j.company_id
WHERE j.id = %s
"""

LATEST = """
SELECT checked_at, verdict::text AS verdict, confidence, reason, details
FROM job_checks WHERE job_id = %s AND checker = 'combined'
ORDER BY checked_at DESC LIMIT 1
"""

EVENTS = """
SELECT from_state::text AS from_state, to_state::text AS to_state, reason, created_at
FROM status_events WHERE job_id = %s ORDER BY created_at DESC LIMIT 12
"""

SEARCH = """
SELECT j.id, j.title, c.name AS company, j.source::text AS source, j.location,
       j.state::text AS state, j.url, j.predicted_label::text AS label, j.ghost_score
FROM jobs j JOIN companies c ON c.id = j.company_id
WHERE j.title ILIKE %s OR c.name ILIKE %s
ORDER BY (j.state = 'closed'), j.last_seen_at DESC
LIMIT %s
"""

SAMPLE = """
SELECT j.id, j.title, c.name AS company, j.source::text AS source, j.location,
       j.state::text AS state, j.url, j.predicted_label::text AS label, j.ghost_score
FROM jobs j JOIN companies c ON c.id = j.company_id
ORDER BY random() LIMIT %s
"""

CLOSED_FEED = """
SELECT e.job_id AS id, e.created_at AS at, e.reason, j.title, j.url, c.name AS company
FROM status_events e
JOIN jobs j ON j.id = e.job_id
JOIN companies c ON c.id = j.company_id
WHERE e.to_state = 'closed' AND e.created_at > now() - make_interval(hours => %s)
ORDER BY e.created_at DESC LIMIT %s
"""

SCORED_FEED = """
SELECT j.id, j.ghost_scored_at AS at, j.title, j.url, c.name AS company,
       j.ghost_score AS score, j.ghost_reasons AS reasons
FROM jobs j JOIN companies c ON c.id = j.company_id
WHERE j.predicted_label = %s::ghost_label
  AND j.ghost_scored_at > now() - make_interval(hours => %s)
ORDER BY j.ghost_score DESC, j.ghost_scored_at DESC LIMIT %s
"""

START = """
SELECT coalesce(min(id) - 1, (SELECT coalesce(max(id), 0) FROM status_events)) AS start
FROM (SELECT id FROM status_events WHERE to_state = %s::job_state ORDER BY id DESC LIMIT %s) t
"""

NEW_EVENTS = """
SELECT e.id, e.job_id, e.from_state::text AS from_state, e.to_state::text AS to_state,
       e.reason, e.created_at, j.title, j.url, c.name AS company
FROM status_events e
JOIN jobs j ON j.id = e.job_id
JOIN companies c ON c.id = j.company_id
WHERE e.id > %s AND e.to_state = %s::job_state
ORDER BY e.id LIMIT 100
"""

LABELS = """
SELECT coalesce(predicted_label::text, 'unscored') AS label, count(*) AS n
FROM jobs GROUP BY 1
"""

EVAL = """
SELECT l.label::text AS label, j.predicted_label::text AS predicted
FROM labels l JOIN jobs j ON j.id = l.job_id
WHERE j.predicted_label IS NOT NULL
"""


async def fetch_all(query, params=()):
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in await cur.fetchall()]


async def fetch_one(query, params=()):
    rows = await fetch_all(query, params)
    return rows[0] if rows else None


def ghost_block(row):
    return {
        "score": round(row["ghost_score"], 2) if row["ghost_score"] is not None else None,
        "label": row["predicted_label"],
        "reasons": row["ghost_reasons"] or [],
        "repost_count": row["repost_count"],
        "family_size": row["family_size"],
        "repost_of": row["repost_of"],
    }


async def job_status(job_id):
    row = await fetch_one(CORE, (job_id,))
    if not row:
        return None
    check = await fetch_one(LATEST, (job_id,))
    events = await fetch_all(EVENTS, (job_id,))
    details = (check or {}).get("details") or {}
    if check:
        reasons = details.get("reasons") or ([check["reason"]] if check["reason"] else [])
    else:
        reasons = ["this job has not been checked yet"]
    return {
        "known": True,
        "id": row["id"],
        "title": row["title"],
        "company": row["company"],
        "url": row["url"],
        "source": row["source"],
        "location": row["location"],
        "state": row["state"],
        "verdict": check["verdict"] if check else "unchecked",
        "confidence": round(check["confidence"], 2) if check else 0.0,
        "reasons": reasons,
        "signals": details.get("signals", []),
        "checked_at": check["checked_at"] if check else None,
        "ghost": ghost_block(row),
        "schedule": {
            "last_checked_at": row["last_checked_at"],
            "next_check_at": row["next_check_at"],
            "interval_seconds": row["check_interval_seconds"],
            "checks": row["check_count"],
            "posted_at": row["posted_at"],
            "first_seen_at": row["first_seen_at"],
            "closed_at": row["closed_at"],
        },
        "events": events,
    }