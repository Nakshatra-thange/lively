from datetime import datetime, timezone

from ...normalize import parse_dt
from ..htmlutil import find_job_postings
from ..models import Signal
from .base import Checker


class ValidThrough(Checker):
    name = "valid_through"

    async def run(self, ctx):
        page = await ctx.page()
        if page.status != 200 or not page.body:
            return None
        for posting in find_job_postings(page.body):
            valid = parse_dt(posting.get("validThrough"))
            if valid is None:
                continue
            day = valid.date().isoformat()
            if valid < datetime.now(timezone.utc):
                return Signal(
                    self.name, "closed", 0.9, f"validThrough {day} has passed", {"valid_through": day}
                )
            return Signal(
                self.name, "live", 0.7, f"validThrough {day} is in the future", {"valid_through": day}
            )
        return None
