import html
import json
import re
from urllib.parse import urlparse

SCRIPT_BLOCK = re.compile(r"<(script|style|noscript|svg|template)\b[^>]*>.*?</\1>", re.I | re.S)
COMMENT = re.compile(r"<!--.*?-->", re.S)
TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"\s+")
TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
LD_JSON = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", re.I | re.S
)
PASSWORD_INPUT = re.compile(r"<input[^>]+type=[\"']password[\"']", re.I)
LOGIN_PATH = re.compile(r"/(log-?in|sign-?in|sso|saml|oauth2?|auth|authenticate)/", re.I)


def visible_text(raw):
    text = COMMENT.sub(" ", raw)
    text = SCRIPT_BLOCK.sub(" ", text)
    text = TAG.sub(" ", text)
    return WS.sub(" ", html.unescape(text)).strip()


def page_title(raw):
    m = TITLE.search(raw)
    if not m:
        return ""
    return WS.sub(" ", html.unescape(TAG.sub(" ", m.group(1)))).strip()


def _walk(node):
    if isinstance(node, list):
        for item in node:
            yield from _walk(item)
    elif isinstance(node, dict):
        kind = node.get("@type")
        kinds = kind if isinstance(kind, list) else [kind]
        if "JobPosting" in kinds:
            yield node
        for value in node.values():
            if isinstance(value, (list, dict)):
                yield from _walk(value)


def find_job_postings(raw):
    for m in LD_JSON.finditer(raw):
        try:
            data = json.loads(m.group(1).strip())
        except ValueError:
            continue
        yield from _walk(data)


def is_login_url(url):
    parsed = urlparse(url)
    if LOGIN_PATH.search(parsed.path.rstrip("/") + "/"):
        return True
    return parsed.netloc.lower().startswith(("login.", "sso.", "auth.", "accounts."))


def looks_like_login(raw, text):
    return bool(PASSWORD_INPUT.search(raw)) and len(text) < 1500
