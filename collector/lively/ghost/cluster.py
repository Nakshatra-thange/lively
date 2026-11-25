from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..normalize import norm
from .simhash import distance

MAX_DISTANCE = 10
MIN_GAP = timedelta(days=1)
WINDOW = timedelta(days=90)
OPEN_STATES = ("new", "live", "reposted")


@dataclass
class Member:
    id: int
    external_id: str
    title: str
    location: str
    state: str
    fingerprint: str
    simhash: int
    posted_at: datetime | None
    first_seen_at: datetime
    last_seen_at: datetime
    dlen: int
    evergreen: bool

    @property
    def start(self):
        if self.posted_at is not None and self.posted_at < self.first_seen_at:
            return self.posted_at
        return self.first_seen_at


@dataclass
class Info:
    family_id: int
    size: int
    repost_of: int | None
    repost_count: int
    is_repost: bool
    reposts_90d: int
    total_reposts: int
    first_start: datetime
    open_count: int


class Groups:
    def __init__(self, n):
        self.parent = list(range(n))

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def families(members):
    groups = Groups(len(members))
    by_fp = {}
    by_key = defaultdict(list)
    for i, m in enumerate(members):
        first = by_fp.setdefault(m.fingerprint, i)
        if first != i:
            groups.union(first, i)
        by_key[(norm(m.title), norm(m.location))].append(i)
    for idxs in by_key.values():
        reps = []
        for i in idxs:
            for r in reps:
                if distance(members[i].simhash, members[r].simhash) <= MAX_DISTANCE:
                    groups.union(r, i)
                    break
            else:
                reps.append(i)
    out = defaultdict(list)
    for i, m in enumerate(members):
        out[groups.find(i)].append(m)
    return list(out.values())


def _ended(earlier, later):
    return earlier.state in ("closed", "suspect") or earlier.last_seen_at < later.first_seen_at


def analyze(members, now):
    result = {}
    for fam in families(members):
        ordered = sorted(fam, key=lambda m: (m.start, m.first_seen_at, m.id))
        links = []
        for i, m in enumerate(ordered):
            link = None
            for j in range(i - 1, -1, -1):
                prev = ordered[j]
                if m.start - prev.start >= MIN_GAP or _ended(prev, m):
                    link = prev
                    break
            links.append(link)
        flags = [link is not None for link in links]
        total = sum(flags)
        recent = sum(1 for m, f in zip(ordered, flags) if f and now - m.start <= WINDOW)
        opened = sum(1 for m in ordered if m.state in OPEN_STATES)
        root = ordered[0]
        running = 0
        for m, flag, link in zip(ordered, flags, links):
            running += 1 if flag else 0
            result[m.id] = Info(
                family_id=root.id,
                size=len(ordered),
                repost_of=link.id if link else None,
                repost_count=running,
                is_repost=flag,
                reposts_90d=recent,
                total_reposts=total,
                first_start=root.start,
                open_count=opened,
            )
    return result