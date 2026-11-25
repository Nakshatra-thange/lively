GHOST_MIN = 0.6

EVERGREEN = (
    r"talent (community|network|pool)|general application|future opportunit|evergreen"
    r"|open application|expression of interest|talent pipeline"
)

AGE_TIERS = [(365, 0.65), (180, 0.5), (120, 0.38), (90, 0.3), (60, 0.18), (30, 0.08)]


def _plural(n, one, many):
    return f"{n} {one if n == 1 else many}"


def score_job(member, info, company_rate, now):
    pts = []

    age = max(0, (now - member.start).days)
    for days, points in AGE_TIERS:
        if age >= days:
            pts.append((points, f"open for {age} days"))
            break

    recent = info.reposts_90d
    if recent >= 5:
        points = 0.5
    elif recent >= 3:
        points = 0.38
    elif recent >= 1:
        points = 0.18
    else:
        points = 0.0
    if points:
        pts.append((points, f"reposted {_plural(recent, 'time', 'times')} in 90 days"))

    older = info.total_reposts - recent
    if older >= 1:
        pts.append(
            (
                min(0.2, 0.04 + 0.08 * older),
                f"{_plural(older, 'earlier relisting', 'earlier relistings')} of the same role",
            )
        )

    family_age = (now - info.first_start).days
    if info.total_reposts and family_age - age >= 30:
        pts.append((0.1, f"same role first listed {family_age} days ago"))

    if member.evergreen:
        pts.append((0.6, "reads like an evergreen or talent pool listing"))

    if 0 <= member.dlen < 200:
        pts.append((0.05, "description is very short"))

    if company_rate >= 0.5:
        pts.append((0.1, f"company relists {round(company_rate * 100)}% of its roles"))
    elif company_rate >= 0.25:
        pts.append((0.05, f"company relists {round(company_rate * 100)}% of its roles"))

    total = min(1.0, sum(p for p, _ in pts))
    pts.sort(key=lambda item: -item[0])
    return round(total, 2), [text for _, text in pts]


def predict_label(state, score, is_repost):
    if state in ("closed", "suspect"):
        return "closed"
    if score >= GHOST_MIN:
        return "ghost"
    if is_repost:
        return "repost"
    return "live"