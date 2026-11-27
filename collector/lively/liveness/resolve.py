import re
from urllib.parse import parse_qs, urlsplit

from . import store
from .models import JobRef

PATTERNS = [
    (
        "greenhouse",
        re.compile(
            r"^https?://(?:boards|job-boards)(?:\.eu)?\.greenhouse\.io/(?P<slug>[^/?#]+)/jobs/(?P<id>\d+)",
            re.I,
        ),
    ),
    (
        "lever",
        re.compile(
            r"^https?://jobs(?:\.eu)?\.lever\.co/(?P<slug>[^/?#]+)/(?P<id>[0-9a-f-]{36})", re.I
        ),
    ),
    (
        "ashby",
        re.compile(r"^https?://jobs\.ashbyhq\.com/(?P<slug>[^/?#]+)/(?P<id>[0-9a-f-]{36})", re.I),
    ),
    (
        "workable",
        re.compile(r"^https?://apply\.workable\.com/(?P<slug>[^/?#]+)/j/(?P<id>[A-Za-z0-9]+)", re.I),
    ),
]


def parse(url):
    for source, rx in PATTERNS:
        m = rx.match(url)
        if m:
            return source, m.group("slug"), m.group("id")
    return None


def loose_id(url):
    parts = urlsplit(url)
    gh = parse_qs(parts.query).get("gh_jid")
    if gh and gh[0].isdigit():
        return gh[0]
    segments = [s for s in parts.path.split("/") if s]
    if segments and segments[-1].isdigit():
        return segments[-1]
    return None


async def resolve(url):
    url = url.strip()
    if not url.lower().startswith(("http://", "https://")):
        return None
    parsed = parse(url)
    source, slug, external_id = parsed if parsed else ("", "", "")
    job = await store.find_job(url, source, slug, external_id)
    if job:
        return job
    if not parsed:
        external_id = loose_id(url)
        host = (urlsplit(url).hostname or "").lower()
        if external_id and host:
            job = await store.find_by_external_host(external_id, host)
            if job:
                return job
    if parsed:
        return JobRef(None, None, source, slug, external_id, url, None, "", "new")
    return JobRef(None, None, "unknown", "", "", url, None, "", "new")
