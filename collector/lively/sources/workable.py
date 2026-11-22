from ..http import get_json
from ..normalize import clean_text, parse_dt
from .base import RawJob, Source


class Workable(Source):
    name = "workable"

    def board_url(self, slug):
        return f"https://apply.workable.com/{slug}"

    async def probe(self, client, slug):
        data = await get_json(
            client,
            f"https://apply.workable.com/api/v1/widget/accounts/{slug}",
            retries=2,
        )
        if isinstance(data, dict) and data.get("jobs"):
            return data.get("name") or slug.replace("-", " ").title()
        return None

    async def fetch(self, client, slug):
        data = await get_json(
            client,
            f"https://apply.workable.com/api/v1/widget/accounts/{slug}?details=true",
        )
        if data is None:
            return None
        out = []
        for j in data.get("jobs", []):
            ext = j.get("shortcode") or j.get("id")
            url = j.get("url") or j.get("shortlink")
            if not ext or not url:
                continue
            parts = [p for p in (j.get("city"), j.get("state"), j.get("country")) if p]
            out.append(
                RawJob(
                    external_id=str(ext),
                    title=j.get("title") or "",
                    url=url,
                    apply_url=j.get("application_url"),
                    location=", ".join(parts) or None,
                    department=j.get("department"),
                    employment_type=j.get("employment_type"),
                    remote=j.get("telecommuting"),
                    description=clean_text(j.get("description")),
                    posted_at=parse_dt(j.get("published_on") or j.get("created_at")),
                )
            )
        return out
