from ..http import get_json
from ..normalize import clean_text, parse_dt
from .base import RawJob, Source


class Ashby(Source):
    name = "ashby"

    def board_url(self, slug):
        return f"https://jobs.ashbyhq.com/{slug}"

    async def probe(self, client, slug):
        data = await get_json(
            client, f"https://api.ashbyhq.com/posting-api/job-board/{slug}", retries=2
        )
        if isinstance(data, dict) and data.get("jobs"):
            return slug.replace("-", " ").title()
        return None

    async def fetch(self, client, slug):
        data = await get_json(client, f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
        if data is None:
            return None
        out = []
        for j in data.get("jobs", []):
            if j.get("isListed") is False:
                continue
            url = j.get("jobUrl")
            if not url or not j.get("id"):
                continue
            out.append(
                RawJob(
                    external_id=str(j["id"]),
                    title=j.get("title") or "",
                    url=url,
                    apply_url=j.get("applyUrl"),
                    location=j.get("location"),
                    department=j.get("department") or j.get("team"),
                    employment_type=j.get("employmentType"),
                    remote=j.get("isRemote"),
                    description=j.get("descriptionPlain") or clean_text(j.get("descriptionHtml")),
                    posted_at=parse_dt(j.get("publishedAt")),
                )
            )
        return out
