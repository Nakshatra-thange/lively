from ..htmlutil import is_login_url, looks_like_login, visible_text
from ..models import Signal
from .base import Checker


class HttpStatus(Checker):
    name = "http_status"

    async def run(self, ctx):
        page = await ctx.page()
        job = ctx.job

        def sig(verdict, confidence, reason, **details):
            return Signal(
                self.name, verdict, confidence, reason, details, page.status, page.latency_ms
            )

        if page.status is None:
            return sig("uncertain", 0.0, f"request failed: {page.error}", error=page.error)

        status = page.status
        if status in (404, 410):
            return sig("closed", 0.98 if status == 410 else 0.95, f"page returned {status}")
        if status == 429:
            return sig(
                "uncertain", 0.0, "rate limited by host (429)", retry_after=page.retry_after
            )
        if status == 401:
            return sig("uncertain", 0.0, "login required (401)")
        if status == 403:
            return sig("uncertain", 0.0, "blocked by host (403)")
        if status >= 500:
            return sig("uncertain", 0.0, f"host error ({status})")
        if status != 200:
            return sig("uncertain", 0.0, f"unexpected status {status}")

        final = page.final_url.lower()
        details = {"final_url": page.final_url, "redirects": len(page.redirects)}

        if is_login_url(final):
            return sig("uncertain", 0.0, "redirected to a login page", **details)
        if "error=true" in final:
            return sig("closed", 0.9, "redirected to the board with an error flag", **details)
        if page.redirects and job.external_id and job.external_id.lower() not in final:
            return sig("closed", 0.8, "redirected away from the job page", **details)
        if looks_like_login(page.body, visible_text(page.body)):
            return sig("uncertain", 0.0, "login wall instead of job content", **details)
        if page.partial:
            return sig("uncertain", 0.1, "page loaded only partially", **details)
        return sig("live", 0.5, "page loads normally", **details)
