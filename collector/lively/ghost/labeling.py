import asyncio
import csv
import random

from ..db import pool

QUOTAS = {"random": 50, "closed": 45, "ghost": 45, "repost": 30, "borderline": 30}

CONDITIONS = {
    "random": "TRUE",
    "closed": "j.predicted_label = 'closed'",
    "ghost": "j.predicted_label = 'ghost'",
    "repost": "j.predicted_label = 'repost'",
    "borderline": "j.predicted_label IN ('live', 'repost') AND j.ghost_score >= 0.3 AND j.ghost_score < 0.6",
}

KEYS = {"l": "live", "c": "closed", "g": "ghost", "r": "repost"}

PICK = """
SELECT j.id FROM jobs j
WHERE j.ghost_scored_at IS NOT NULL
  AND {cond}
  AND NOT EXISTS (SELECT 1 FROM labels l WHERE l.job_id = j.id)
  AND j.id <> ALL(%s::bigint[])
ORDER BY random()
LIMIT %s
"""

DETAILS = """
SELECT j.id, j.title, c.name, j.source::text, COALESCE(j.location, ''), j.url,
       (EXTRACT(EPOCH FROM now() - COALESCE(j.posted_at, j.first_seen_at)) / 86400)::int,
       left(j.description, 240)
FROM jobs j JOIN companies c ON c.id = j.company_id
WHERE j.id = ANY(%s::bigint[])
"""

SAVE = """
INSERT INTO labels (job_id, label, note, stratum) VALUES (%s, %s, %s, %s)
ON CONFLICT (job_id) DO UPDATE SET
    label = EXCLUDED.label, note = EXCLUDED.note,
    stratum = EXCLUDED.stratum, labeled_at = now()
"""

GUIDE = """
labels
  l  live    real, open, looks like a genuine active hire
  c  closed  page is dead, filled, expired or redirects away
  g  ghost   still open but probably not a real active hire (evergreen, very old, relisted again and again)
  r  repost  open copy of an older listing under a new ID, otherwise looks real
  s  skip    q  quit and save
add a note after the key, for example: g open since last year, same text
open each URL in a browser and judge the page, not the title
"""


async def build_queue(count):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT coalesce(stratum, ''), count(*) FROM labels GROUP BY 1")
        done = dict(await cur.fetchall())
        total_done = sum(done.values())
        chosen = []
        picked = {}
        for name, quota in QUOTAS.items():
            want = max(0, round(quota * count / 200) - done.get(name, 0))
            if not want:
                continue
            cur = await conn.execute(PICK.format(cond=CONDITIONS[name]), (chosen, want))
            ids = [r[0] for r in await cur.fetchall()]
            chosen.extend(ids)
            for i in ids:
                picked[i] = name
        shortfall = count - total_done - len(chosen)
        if shortfall > 0:
            cur = await conn.execute(PICK.format(cond="TRUE"), (chosen, shortfall))
            for r in await cur.fetchall():
                chosen.append(r[0])
                picked[r[0]] = "random"
    ids = list(picked)
    random.shuffle(ids)
    return ids, picked


async def run_label(count):
    ids, picked = await build_queue(count)
    if not ids:
        print("nothing left to label")
        return
    async with pool.connection() as conn:
        cur = await conn.execute(DETAILS, (ids,))
        details = {r[0]: r for r in await cur.fetchall()}

    print(GUIDE)
    saved = 0
    for n, jid in enumerate(ids, 1):
        _, title, company, source, location, url, age, snippet = details[jid]
        print(f"\n[{n}/{len(ids)}] {title}  @ {company}  ({source})")
        print(f"  location  {location or '-'}")
        print(f"  age       {age} days since posted")
        print(f"  url       {url}")
        print(f"  text      {snippet}")
        while True:
            try:
                raw = await asyncio.to_thread(input, "label [l/c/g/r] s=skip q=quit > ")
            except EOFError:
                raw = "q"
            parts = raw.strip().split(None, 1)
            key = parts[0].lower() if parts else ""
            if key == "q":
                print(f"\nsaved {saved} labels this session")
                return
            if key == "s":
                break
            if key in KEYS:
                note = parts[1] if len(parts) > 1 else None
                async with pool.connection() as conn:
                    await conn.execute(SAVE, (jid, KEYS[key], note, picked[jid]))
                saved += 1
                break
    print(f"\nsaved {saved} labels this session")


async def export_labels(path):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT j.url, l.label::text, coalesce(l.note, ''), coalesce(l.stratum, '')
            FROM labels l JOIN jobs j ON j.id = l.job_id ORDER BY l.job_id
            """
        )
        rows = await cur.fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["url", "label", "note", "stratum"])
        writer.writerows(rows)
    print(f"exported {len(rows)} labels to {path}")


async def import_labels(path):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    values = [(r["label"], r["note"], r["stratum"], r["url"]) for r in rows]
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO labels (job_id, label, note, stratum)
                SELECT id, %s::ghost_label, NULLIF(%s, ''), NULLIF(%s, '')
                FROM jobs WHERE url = %s LIMIT 1
                ON CONFLICT (job_id) DO UPDATE SET
                    label = EXCLUDED.label, note = EXCLUDED.note,
                    stratum = EXCLUDED.stratum, labeled_at = now()
                """,
                values,
            )
    print(f"imported {len(values)} rows from {path}")