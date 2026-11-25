from urllib.parse import quote

from ..fetcher import fetch
from ..models import Signal
from .base import Checker

ENDPOINTS = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{id}",
    "lever": "https://api.lever.co/v0/postings/{slug}/{id}",
}


class AtsApi(Checker):
    name = "ats_api"

    async def run(self, ctx):
        job = ctx.job
        template = ENDPOINTS.get(job.source)
        if not template or not job.slug or not job.external_id:
            return None
        url = template.format(slug=quote(job.slug), id=quote(job.external_id))
        res = await fetch(ctx.client, url, max_bytes=50_000)

        def sig(verdict, confidence, reason):
            return Signal(
                self.name, verdict, confidence, reason, {"url": url}, res.status, res.latency_ms
            )

        if res.status is None:
            return sig("uncertain", 0.0, f"ATS API request failed: {res.error}")
        if res.status == 404:
            return sig("closed", 0.97, "ATS API no longer serves this posting")
        if res.status == 200:
            return sig("live", 0.95, "ATS API still serves this posting")
        if res.status == 429:
            return sig("uncertain", 0.0, "ATS API rate limited (429)")
        return sig("uncertain", 0.0, f"ATS API returned {res.status}")
