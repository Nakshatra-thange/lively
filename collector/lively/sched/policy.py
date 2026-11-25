import random

from ..config import BASE_INTERVAL, MAX_INTERVAL, MIN_INTERVAL

SUSPECT_INTERVAL = 7200
UNCERTAIN_INTERVAL = 3600
CLOSED_INTERVAL = MAX_INTERVAL
GROW = 2.0
SHRINK = 0.5
MIN_SYNC = 3600
MAX_SYNC = 86400


def company_cap(rate):
    cap = MAX_INTERVAL / (1.0 + 20.0 * max(0.0, rate))
    return int(min(MAX_INTERVAL, max(6 * 3600, cap)))


def job_interval(state, verdict, changed, current, rate, streak):
    if state == "closed":
        return CLOSED_INTERVAL
    if state == "suspect":
        return int(min(86400, SUSPECT_INTERVAL * 2 ** min(streak, 3)))
    if verdict == "uncertain":
        return UNCERTAIN_INTERVAL
    current = current or BASE_INTERVAL
    nxt = current * (SHRINK if changed else GROW)
    return int(min(company_cap(rate), max(MIN_INTERVAL, nxt)))


def sync_interval(rate, failures):
    if failures:
        return int(min(7 * 86400, 3600 * 2 ** min(failures, 8)))
    return int(min(MAX_SYNC, max(MIN_SYNC, MAX_SYNC / (1.0 + 30.0 * max(0.0, rate)))))


def ewma(old, observed, alpha=0.3):
    return (1.0 - alpha) * (old or 0.0) + alpha * observed


def jitter(seconds, spread=0.1):
    return seconds * random.uniform(1.0 - spread, 1.0 + spread)