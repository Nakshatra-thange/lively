from ..db import pool


async def summary():
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT coalesce(predicted_label::text, 'unscored'), count(*)
            FROM jobs GROUP BY 1 ORDER BY 2 DESC
            """
        )
        print("\npredicted labels")
        for row in await cur.fetchall():
            print("  ", *row)

        cur = await conn.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE reposted)
            FROM (
                SELECT family_id, max(repost_count) > 0 AS reposted
                FROM jobs WHERE family_id IS NOT NULL GROUP BY 1
            ) t
            """
        )
        total, reposted = await cur.fetchone()
        print(f"\nrole families {total} | with reposts {reposted}")

        cur = await conn.execute(
            """
            SELECT c.name, count(*) AS jobs,
                   count(*) FILTER (WHERE j.predicted_label = 'ghost') AS ghosts,
                   count(*) FILTER (WHERE j.predicted_label = 'repost') AS reposts
            FROM jobs j JOIN companies c ON c.id = j.company_id
            WHERE j.ghost_scored_at IS NOT NULL
            GROUP BY c.name ORDER BY ghosts DESC, reposts DESC LIMIT 10
            """
        )
        print("\ncompanies with the most ghost jobs")
        print(f"   {'company':<28} {'jobs':>6} {'ghost':>6} {'repost':>7}")
        for name, jobs, ghosts, reposts in await cur.fetchall():
            print(f"   {name:<28} {jobs:>6} {ghosts:>6} {reposts:>7}")


async def top(limit, minimum):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT c.name, j.title, j.state::text, j.ghost_score, j.predicted_label::text,
                   j.ghost_reasons, j.url
            FROM jobs j JOIN companies c ON c.id = j.company_id
            WHERE j.ghost_scored_at IS NOT NULL AND j.state <> 'closed' AND j.ghost_score >= %s
            ORDER BY j.ghost_score DESC, j.id
            LIMIT %s
            """,
            (minimum, limit),
        )
        rows = await cur.fetchall()
    if not rows:
        print("no jobs at or above that score")
        return
    for name, title, state, score, label, reasons, url in rows:
        print(f"\n{score:.2f} {label:<7} {name} | {title}  [{state}]")
        print(f"      {url}")
        for reason in reasons:
            print(f"      - {reason}")