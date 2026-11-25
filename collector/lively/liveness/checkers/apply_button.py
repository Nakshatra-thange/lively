import re

from ..htmlutil import TAG, looks_like_login, visible_text
from ..models import Signal
from .base import Checker

APPLY_WORD = re.compile(r"\bapply\b|submit (?:your )?application", re.I)
ELEMENT = re.compile(r"<(a|button)\b([^>]*)>(.*?)</\1>", re.I | re.S)
FORM_ID = re.compile(
    r"id=[\"'](?:application_form|app_body|application-form|apply-form)[\"']", re.I
)
ATTR_HINT = re.compile(r"apply", re.I)


def has_apply(raw):
    raw = raw[:400000]
    if FORM_ID.search(raw):
        return True
    for m in ELEMENT.finditer(raw):
        attrs = m.group(2)
        inner = TAG.sub(" ", m.group(3))
        if len(inner) < 200 and APPLY_WORD.search(inner):
            return True
        if ATTR_HINT.search(attrs):
            return True
    return False


class ApplyButton(Checker):
    name = "apply_button"

    async def run(self, ctx):
        page = await ctx.page()
        if page.status != 200 or not page.body:
            return None
        text = visible_text(page.body)
        if looks_like_login(page.body, text):
            return None
        if has_apply(page.body):
            return Signal(self.name, "live", 0.65, "apply button or form is present")
        if len(text) >= 600 and not page.truncated and not page.partial:
            return Signal(self.name, "closed", 0.35, "no apply button found on the page")
        return None
