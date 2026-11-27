from ..http import get_json
from ..normalize import clean_text, parse_dt
from .base import RawJob, Source


class Greenhouse(Source):
    name = "greenhouse"

    def board_url(self, slug):
        return f"https://boards.greenhouse.io/{slug}"

    async def probe(self, client, slug):
        data = await get_json(client, f"https://boards-api.greenhouse.io/v1/boards/{slug}", retries=2)
        if isinstance(data, dict) and data.get("name"):
            return data["name"]
        return None

    async def fetch(self, client, slug, content=True):
        suffix = "?content=true" if content else ""
        data = await get_json(client, f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs{suffix}")
        if data is None:
            return None
        out = []
        for j in data.get("jobs", []):
            url = j.get("absolute_url")
            if not url or j.get("id") is None:
                continue
            depts = j.get("departments") or []
            out.append(
                RawJob(
                    external_id=str(j["id"]),
                    title=j.get("title") or "",
                    url=url,
                    apply_url=url,
                    location=(j.get("location") or {}).get("name"),
                    department=depts[0].get("name") if depts else None,
                    employment_type=None,
                    remote=None,
                    description=clean_text(j.get("content")),
                    posted_at=parse_dt(j.get("first_published") or j.get("updated_at")),
                )
            )
        return out
