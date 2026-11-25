from datetime import datetime, timedelta, timezone

import pytest

from lively.ghost.cluster import Member, analyze
from lively.ghost.evaluate import metrics
from lively.ghost.score import GHOST_MIN, predict_label, score_job
from lively.ghost.simhash import distance, simhash

NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)
BASE = " ".join(f"w{i}" for i in range(300))
OTHER = " ".join(f"x{i}" for i in range(300))
SIG = simhash(BASE)


def member(i, **kw):
    base = dict(
        id=i,
        external_id=str(i),
        title="Backend Engineer",
        location="Remote",
        state="live",
        fingerprint=f"fp{i}",
        simhash=SIG,
        posted_at=None,
        first_seen_at=NOW - timedelta(days=1),
        last_seen_at=NOW,
        dlen=1500,
        evergreen=False,
    )
    base.update(kw)
    return Member(**base)


def posted(i, days, **kw):
    when = NOW - timedelta(days=days)
    return member(i, posted_at=when, first_seen_at=when, **kw)


def test_simhash_identical_different_and_near():
    assert distance(simhash(BASE), simhash(BASE)) == 0
    assert distance(simhash(BASE), simhash(OTHER)) > 16
    assert distance(simhash(BASE), simhash(BASE.replace("w150", "zzz"))) <= 14


def test_sequential_repost_is_linked():
    old = posted(
        1, 100, state="closed", last_seen_at=NOW - timedelta(days=60)
    )
    new = posted(2, 30)
    info = analyze([old, new], NOW)
    assert info[1].is_repost is False
    assert info[2].is_repost is True
    assert info[2].repost_of == 1
    assert info[2].family_id == 1


def test_concurrent_same_day_listings_are_not_reposts():
    info = analyze([member(1), member(2)], NOW)
    assert info[1].is_repost is False
    assert info[2].is_repost is False
    assert info[1].family_id == info[2].family_id


def test_different_titles_stay_separate():
    info = analyze([member(1), member(2, title="Data Scientist")], NOW)
    assert info[1].family_id != info[2].family_id


def test_repost_counts_and_plain_reasons():
    members = [posted(i + 1, d) for i, d in enumerate([80, 60, 40, 20, 5])]
    info = analyze(members, NOW)
    assert info[5].total_reposts == 4
    assert info[5].reposts_90d == 4
    assert info[5].repost_of == 4
    score, reasons = score_job(members[4], info[5], 0.0, NOW)
    assert "reposted 4 times in 90 days" in reasons
    assert score < GHOST_MIN


def test_very_old_listing_is_ghost():
    m = posted(1, 400)
    info = analyze([m], NOW)[1]
    score, reasons = score_job(m, info, 0.0, NOW)
    assert score >= GHOST_MIN
    assert reasons[0] == "open for 400 days"
    assert predict_label("live", score, False) == "ghost"


def test_evergreen_listing_is_ghost():
    m = member(1, evergreen=True)
    score, reasons = score_job(m, analyze([m], NOW)[1], 0.0, NOW)
    assert score >= GHOST_MIN
    assert any("evergreen" in r for r in reasons)


def test_fresh_unique_listing_is_clean():
    m = member(1)
    score, reasons = score_job(m, analyze([m], NOW)[1], 0.0, NOW)
    assert score == 0.0
    assert reasons == []


def test_predict_label_priority():
    assert predict_label("closed", 0.1, False) == "closed"
    assert predict_label("suspect", 0.9, False) == "closed"
    assert predict_label("live", 0.7, True) == "ghost"
    assert predict_label("live", 0.3, True) == "repost"
    assert predict_label("live", 0.1, False) == "live"


def test_metrics_per_class_and_bad_filter():
    pairs = [
        ("live", "live"),
        ("live", "ghost"),
        ("ghost", "ghost"),
        ("closed", "closed"),
        ("closed", "live"),
    ]
    result = metrics(pairs)
    assert result["per_class"]["ghost"]["precision"] == pytest.approx(0.5)
    assert result["per_class"]["ghost"]["recall"] == pytest.approx(1.0)
    assert result["per_class"]["closed"]["precision"] == pytest.approx(1.0)
    assert result["per_class"]["closed"]["recall"] == pytest.approx(0.5)
    assert result["bad_filter"]["precision"] == pytest.approx(2 / 3)
    assert result["bad_filter"]["recall"] == pytest.approx(2 / 3)
    assert result["accuracy"] == pytest.approx(0.6)