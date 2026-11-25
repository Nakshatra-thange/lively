import asyncio
import logging
import signal
from collections import Counter
from datetime import datetime, timezone

from psycopg.types.json import Jsonb

from ..db import pool
from .cluster import Member, analyze
from .score import EVERGREEN, predict_label, score_job
from .simhash import simhash, to_signed, to_unsigned

log = logging.getLogger("lively.ghost")

SELECT = """
SELECT id, external_id, title, COALESCE(location, ''), state::text, fingerprint, simhash,
       posted_at, first_seen_at, last_seen_at,
       CASE WHEN simhash IS NULL THEN description END,
       length(description),
       (left(description, 600) ~* %s OR title ~* %s),
       family_id, family_size, repost_of, repost_count, ghost_score, ghost_reasons,
       predicted_label::text
FROM jobs WHERE company_id = %s
"""

FILL = "UPDATE jobs SET simhash = %s WHERE id = %s"

UPDATE = """
UPDATE jobs SET
    family_id = %s, family_size = %s, repost_of = %s, repost_count = %s,
    ghost_score = %s, ghost_reasons = %s, predicted_label = %s, ghost_scored_at = now()
WHERE id = %s
"""

MOVE = """
WITH old AS (
    SELECT id, state::text AS prev FROM jobs
    WHERE id = ANY(%s::bigint[]) AND state IN ('new', 'live')
    FOR UPDATE
)
UPDATE jobs j SET state = 'reposted', updated_at = now()
FROM old WHERE j.id = old.id
RETURNING j.id, old.prev
"""

EVENT = "INSERT INTO status_events (job_id, from_state, to_state, reason) VALUES (%s, %s, %s, %s)"


async def scan_company(company_id, now=None):
    now = now or datetime.now(timezone.utc)
    stats = Counter()
    async with pool.connection() as conn:
        await conn.execute("SET LOCAL lock_timeout = '500ms'")
        async with conn.cursor() as cur:
            await cur.execute(SELECT, (EVERGREEN, EVERGREEN, company_id))
            rows = await cur.fetchall()
            if not rows:
                return stats

            members = []
            known = {}
            fills = []
            for r in rows:
                if r[6] is None:
                    value = simhash(r[10] or "")
                    fills.append((to_signed(value), r[0]))
                else:
                    value = to_unsigned(r[6])
                members.append(
                    Member(
                        id=r[0],
                        external_id=r[1],
                        title=r[2],
                        location=r[3],
                        state=r[4],
                        fingerprint=r[5],
                        simhash=value,
                        posted_at=r[7],
                        first_seen_at=r[8],
                        last_seen_at=r[9],
                        dlen=r[11] or 0,
                        evergreen=bool(r[12]),
                    )
                )
                known[r[0]] = r[13:]

            if fills:
                fills.sort(key=lambda f: f[1])
                await cur.executemany(FILL, fills)

            infos = analyze(members, now)
            fam_flags = {i.family_id: i.total_reposts > 0 for i in infos.values()}
            rate = sum(fam_flags.values()) / len(fam_flags) if len(fam_flags) >= 10 else 0.0
            stats["families"] += len(fam_flags)
            stats["repost_families"] += sum(fam_flags.values())

            updates = []
            for m in sorted(members, key=lambda x: x.id):
                info = infos[m.id]
                score, reasons = score_job(m, info, rate, now)
                label = predict_label(m.state, score, info.is_repost)
                stats["jobs"] += 1
                stats[f"predicted_{label}"] += 1
                prev = known[m.id]
                unchanged = (
                    prev[0] == info.family_id
                    and prev[1] == info.size
                    and prev[2] == info.repost_of
                    and prev[3] == info.repost_count
                    and prev[4] is not None
                    and abs(prev[4] - score) < 0.005
                    and prev[5] == reasons
                    and prev[6] == label
                )
                if unchanged:
                    continue
                updates.append(
                    (
                        info.family_id,
                        info.size,
                        info.repost_of,
                        info.repost_count,
                        score,
                        Jsonb(reasons),
                        label,
                        m.id,
                    )
                )
            if updates:
                await cur.executemany(UPDATE, updates)
                stats["written"] += len(updates)

            by_id = {m.id: m for m in members}
            move = sorted(m.id for m in members if infos[m.id].is_repost and m.state in ("new", "live"))
            if move:
                await cur.execute(MOVE, (move,))
                moved = await cur.fetchall()
                events = [
                    (
                        jid,
                        prev_state,
                        "reposted",
                        f"same role as earlier listing {by_id[infos[jid].repost_of].external_id}",
                    )
                    for jid, prev_state in moved
                ]
                if events:
                    await cur.executemany(EVENT, events)
                stats["moved_to_reposted"] += len(moved)
    return stats


async def run_scan(company_id=None):
    if company_id:
        ids = [company_id]
    else:
        async with pool.connection() as conn:
            cur = await conn.execute("SELECT DISTINCT company_id FROM jobs ORDER BY 1")
            ids = [r[0] for r in await cur.fetchall()]
    total = Counter()
    for cid in ids:
        try:
            total.update(await scan_company(cid))
        except Exception as exc:
            log.warning("scan of company %s skipped: %s", cid, exc)
            total["skipped_boards"] += 1
    log.info("scan finished | %s", dict(total))
    return total


async def run_loop(every):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    while not stop.is_set():
        await run_scan()
        try:
            await asyncio.wait_for(stop.wait(), timeout=every)
        except asyncio.TimeoutError:
            pass