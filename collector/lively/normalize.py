import hashlib
import html
import re
from datetime import datetime, timezone

TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"\s+")
NON_ALNUM = re.compile(r"[^a-z0-9 ]+")


def clean_text(value):
    if not value:
        return ""
    text = html.unescape(str(value))
    text = TAG.sub(" ", text)
    text = html.unescape(text)
    return WS.sub(" ", text).strip()[:20000]


def norm(value):
    return WS.sub(" ", NON_ALNUM.sub(" ", (value or "").lower())).strip()


def _sha1(value):
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def content_hash(title, description):
    return _sha1(f"{norm(title)}|{norm(description)}")


def fingerprint(company_id, title, location, description):
    return _sha1(f"{company_id}|{norm(title)}|{norm(location)}|{norm(description)[:1500]}")


def parse_dt(value):
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (ValueError, OverflowError, OSError):
        return None
