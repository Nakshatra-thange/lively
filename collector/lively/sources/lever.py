from ..http import get_json
from ..normalize import clean_text, parse_dt
from .base import RawJob, Source


class Lever(Source):
    name = "lever"

    def board_url(self, slug):
        return f"https://jobs.lever.co/{slug}"

    async def probe(self, client, slug):
        data = await get_json(
            client, f"https://api.lever.co/v0/postings/{slug}?mode=json&limit=1", retries=2
        )
        if isinstance(data, list) and data:
            return slug.replace("-", " ").title()
        return None

    async def fetch(self, client, slug):
        data = await get_json(client, f"https://api.lever.co/v0/postings/{slug}?mode=json")
        if data is None:
            return None
        if not isinstance(data, list):
            return []
        out = []
        for j in data:
            url = j.get("hostedUrl")
            if not url or not j.get("id"):
                continue
            cats = j.get("categories") or {}
            workplace = j.get("workplaceType")
            description = j.get("descriptionPlain") or clean_text(j.get("description"))
            extra = j.get("additionalPlain") or ""
            out.append(
                RawJob(
                    external_id=str(j["id"]),
                    title=j.get("text") or "",
                    url=url,
                    apply_url=j.get("applyUrl"),
                    location=cats.get("location"),
                    department=cats.get("team"),
                    employment_type=cats.get("commitment"),
                    remote=(workplace == "remote") if workplace else None,
                    description=clean_text(f"{description} {extra}"),
                    posted_at=parse_dt(j.get("createdAt")),
                )
            )
        return out
