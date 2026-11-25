from lively.sched.policy import company_cap, ewma, job_interval, sync_interval


def test_stable_job_backs_off_to_the_cap():
    interval = 3600
    seen = []
    for _ in range(10):
        interval = job_interval("live", "live", False, interval, 0.0, 1)
        seen.append(interval)
    assert seen == sorted(seen)
    assert seen[-1] == 604800


def test_change_halves_interval():
    assert job_interval("live", "live", True, 86400, 0.0, 0) == 43200


def test_interval_never_drops_below_floor():
    assert job_interval("live", "live", True, 1800, 0.0, 0) == 1800


def test_volatile_company_caps_interval():
    assert company_cap(0.5) < company_cap(0.0)
    assert job_interval("live", "live", False, 604800, 0.5, 5) == company_cap(0.5)


def test_suspect_backs_off_but_stays_short():
    assert job_interval("suspect", "closed", False, 86400, 0.0, 0) == 7200
    assert job_interval("suspect", "closed", False, 86400, 0.0, 9) == 57600


def test_uncertain_retries_in_an_hour():
    assert job_interval("live", "uncertain", False, 604800, 0.0, 3) == 3600


def test_closed_rechecks_weekly():
    assert job_interval("closed", "closed", False, 3600, 0.0, 0) == 604800


def test_busy_boards_sync_more_often():
    assert sync_interval(0.4, 0) < sync_interval(0.0, 0)
    assert sync_interval(0.0, 0) == 86400


def test_failing_boards_back_off():
    assert sync_interval(0.0, 1) < sync_interval(0.0, 3)


def test_ewma_moves_toward_observation():
    assert 0.0 < ewma(0.0, 1.0) < 1.0