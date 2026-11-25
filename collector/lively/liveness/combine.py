import math

from .models import Result

WEIGHTS = {
    "ats_api": 0.95,
    "http_status": 0.9,
    "board_listing": 0.85,
    "valid_through": 0.9,
    "soft404": 0.8,
    "apply_button": 0.5,
}

LIVE_MIN = 0.66
CLOSED_MIN = 0.6


def strength(signal):
    return min(0.97, max(0.0, signal.confidence) * WEIGHTS.get(signal.checker, 0.5))


def combine(signals):
    net = 0.0
    evidence = False
    for s in signals:
        if s.verdict == "uncertain":
            continue
        evidence = True
        weight = -math.log(1.0 - strength(s))
        net += weight if s.verdict == "live" else -weight

    p_live = 1.0 / (1.0 + math.exp(-net))
    p_closed = 1.0 - p_live

    if not evidence:
        verdict, confidence = "uncertain", 0.0
    elif p_closed >= CLOSED_MIN:
        verdict, confidence = "closed", p_closed
    elif p_live >= LIVE_MIN:
        verdict, confidence = "live", p_live
    else:
        verdict, confidence = "uncertain", max(p_live, p_closed)

    lead = None if verdict == "uncertain" else verdict
    reasons = []
    for s in sorted(signals, key=strength, reverse=True):
        if s.verdict == "uncertain":
            if s.reason:
                reasons.append(f"inconclusive: {s.reason}")
        elif lead is None:
            reasons.append(f"{s.verdict} signal: {s.reason}")
        elif s.verdict == lead:
            reasons.append(s.reason)
        elif strength(s) >= 0.3:
            reasons.append(f"but {s.reason}")
    reasons = list(dict.fromkeys(reasons))

    return Result(
        verdict=verdict,
        confidence=round(confidence, 2),
        reasons=reasons,
        signals=list(signals),
        p_live=p_live,
        p_closed=p_closed,
    )
