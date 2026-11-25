import re

from ...normalize import norm
from ..htmlutil import looks_like_login, page_title, visible_text
from ..models import Signal
from .base import Checker

CLOSED = [
    re.compile(p, re.I)
    for p in (
        r"no longer (?:accepting|available|open|active|taking|hiring|recruiting)",
        r"(?:position|job|role|posting|vacancy|opening) (?:has been|is|was|has) (?:filled|closed|expired|removed|cancell?ed|archived)",
        r"(?:job|position|posting|role) (?:is )?(?:no longer|not) (?:available|open|active)",
        r"this (?:job|position|posting|role|opportunity) (?:has )?(?:expired|closed|ended)",
        r"(?:job|posting|page|position) (?:could not|couldn'?t|can'?t|cannot) be found",
        r"we couldn'?t find (?:that|this|the) (?:job|page|position)",
        r"sorry, we couldn'?t find anything here",
        r"(?:job|position|posting) not found",
        r"applications? (?:are|is|have been) (?:now )?closed",
        r"has been (?:filled|closed)",
    )
]


class Soft404(Checker):
    name = "soft404"

    async def run(self, ctx):
        page = await ctx.page()
        if page.status != 200 or not page.body:
            return None
        text = visible_text(page.body)[:50000]
        if looks_like_login(page.body, text):
            return None
        title = page_title(page.body)

        head = f"{title} {text[:1200]}"
        for rx in CLOSED:
            m = rx.search(head)
            if m:
                return Signal(
                    self.name,
                    "closed",
                    0.9,
                    f'page says "{m.group(0).lower()}"',
                    {"match": m.group(0), "where": "top"},
                )

        for rx in CLOSED:
            m = rx.search(text[1200:])
            if m:
                return Signal(
                    self.name,
                    "closed",
                    0.3,
                    f'page body mentions "{m.group(0).lower()}"',
                    {"match": m.group(0), "where": "body"},
                )

        if page.partial:
            return None
        want = norm(ctx.job.title)
        if not want:
            return None
        if want in norm(f"{title} {text}"):
            return Signal(self.name, "live", 0.55, "job title appears on the page")
        if len(text) >= 300:
            return Signal(self.name, "closed", 0.3, "job title not found on the page")
        return None
