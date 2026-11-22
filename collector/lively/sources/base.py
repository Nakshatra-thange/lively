from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RawJob:
    external_id: str
    title: str
    url: str
    apply_url: Optional[str]
    location: Optional[str]
    department: Optional[str]
    employment_type: Optional[str]
    remote: Optional[bool]
    description: str
    posted_at: Optional[datetime]


class Source:
    name = ""

    def board_url(self, slug):
        raise NotImplementedError

    async def probe(self, client, slug):
        raise NotImplementedError

    async def fetch(self, client, slug):
        raise NotImplementedError
