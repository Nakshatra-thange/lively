import asyncio

from lively.liveness.checkers.apply_button import ApplyButton
from lively.liveness.checkers.http_status import HttpStatus
from lively.liveness.checkers.soft404 import Soft404
from lively.liveness.checkers.valid_through import ValidThrough
from lively.liveness.combine import combine
from lively.liveness.models import FetchResult, JobRef, Signal
from lively.liveness.store import CLOSE_CONFIDENCE


class Ctx:
    def __init__(self, job, page):
        self.job = job
        self._page = page
        self.client = None
        self.boards = None

    async def page(self):
        return self._page


def make_job(**kw):
    base = dict(
        id=1,
        company_id=1,
        source="greenhouse",
        slug="acme",
        external_id="123",
        url="https://boards.greenhouse.io/acme/jobs/123",
        apply_url=None,
        title="Backend Engineer",
        state="new",
    )
    base.update(kw)
    return JobRef(**base)


def run(checker, page, **kw):
    return asyncio.run(checker.run(Ctx(make_job(**kw), page)))


def page(**kw):
    base = dict(url="https://boards.greenhouse.io/acme/jobs/123", status=200)
    base.update(kw)
    base.setdefault("final_url", base["url"])
    return FetchResult(**base)


def test_http_404_is_closed():
    sig = run(HttpStatus(), page(status=404))
    assert sig.verdict == "closed" and sig.confidence >= 0.95


def test_http_429_is_uncertain():
    sig = run(HttpStatus(), page(status=429, retry_after=30))
    assert sig.verdict == "uncertain"


def test_timeout_is_uncertain():
    sig = run(HttpStatus(), FetchResult(url="x", error="timeout"))
    assert sig.verdict == "uncertain"


def test_redirect_away_is_closed():
    sig = run(
        HttpStatus(),
        page(final_url="https://boards.greenhouse.io/acme", redirects=["https://boards.greenhouse.io/acme/jobs/123"]),
    )
    assert sig.verdict == "closed"


def test_login_redirect_is_uncertain():
    sig = run(
        HttpStatus(),
        page(final_url="https://acme.okta.com/login/", redirects=["https://boards.greenhouse.io/acme/jobs/123"]),
    )
    assert sig.verdict == "uncertain"


def test_soft404_phrase():
    body = "<html><title>Acme</title><body><h1>Sorry</h1><p>This job is no longer available.</p></body></html>"
    sig = run(Soft404(), page(body=body))
    assert sig.verdict == "closed" and sig.confidence >= 0.9


def test_soft404_title_present_is_live():
    body = "<html><title>Backend Engineer at Acme</title><body>" + "<p>build things</p>" * 20 + "</body></html>"
    sig = run(Soft404(), page(body=body))
    assert sig.verdict == "live"


def test_valid_through_past_and_future():
    past = '<script type="application/ld+json">{"@type":"JobPosting","validThrough":"2020-01-01"}</script>'
    future = '<script type="application/ld+json">{"@type":"JobPosting","validThrough":"2999-01-01"}</script>'
    assert run(ValidThrough(), page(body=past)).verdict == "closed"
    assert run(ValidThrough(), page(body=future)).verdict == "live"


def test_apply_button():
    with_apply = "<html><body><a href='#app'>Apply for this job</a></body></html>"
    without = "<html><body>" + "<p>lots of words here</p>" * 80 + "</body></html>"
    assert run(ApplyButton(), page(body=with_apply)).verdict == "live"
    assert run(ApplyButton(), page(body=without)).verdict == "closed"


def test_combine_ats_and_http_404():
    result = combine(
        [
            Signal("ats_api", "closed", 0.97, "ATS gone"),
            Signal("http_status", "closed", 0.95, "page 404"),
        ]
    )
    assert result.verdict == "closed" and result.confidence >= 0.95


def test_combine_bare_200_is_uncertain():
    result = combine([Signal("http_status", "live", 0.5, "page loads")])
    assert result.verdict == "uncertain"


def test_combine_removed_from_board_but_page_loads_is_suspect():
    result = combine(
        [
            Signal("board_listing", "closed", 0.9, "not on board"),
            Signal("http_status", "live", 0.5, "page loads"),
        ]
    )
    assert result.verdict == "closed" and result.confidence < CLOSE_CONFIDENCE


def test_combine_confirmed_live():
    result = combine(
        [
            Signal("ats_api", "live", 0.95, "ATS serves it"),
            Signal("board_listing", "live", 0.9, "on board"),
            Signal("http_status", "live", 0.5, "page loads"),
        ]
    )
    assert result.verdict == "live" and result.confidence >= 0.9


def test_inconclusive_reasons_are_surfaced():
    result = combine([Signal("http_status", "uncertain", 0.0, "rate limited by host (429)")])
    assert any("rate limited" in r for r in result.reasons)
