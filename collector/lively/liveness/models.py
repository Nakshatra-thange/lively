from dataclasses import dataclass, field
from typing import Optional


@dataclass
class JobRef:
    id: Optional[int]
    company_id: Optional[int]
    source: str
    slug: str
    external_id: str
    url: str
    apply_url: Optional[str]
    title: str
    state: str


@dataclass
class FetchResult:
    url: str
    final_url: str = ""
    status: Optional[int] = None
    redirects: list = field(default_factory=list)
    body: str = ""
    truncated: bool = False
    partial: bool = False
    latency_ms: int = 0
    error: Optional[str] = None
    retry_after: Optional[int] = None


@dataclass
class Signal:
    checker: str
    verdict: str
    confidence: float
    reason: str
    details: dict = field(default_factory=dict)
    http_status: Optional[int] = None
    latency_ms: Optional[int] = None

    def to_dict(self):
        return {
            "checker": self.checker,
            "verdict": self.verdict,
            "confidence": round(self.confidence, 2),
            "reason": self.reason,
            "details": self.details,
        }


@dataclass
class Result:
    verdict: str
    confidence: float
    reasons: list
    signals: list
    p_live: float
    p_closed: float
    http_status: Optional[int] = None
    latency_ms: Optional[int] = None

    def to_dict(self):
        return {
            "verdict": self.verdict,
            "confidence": self.confidence,
            "reasons": self.reasons,
            "p_live": round(self.p_live, 3),
            "p_closed": round(self.p_closed, 3),
            "signals": [s.to_dict() for s in self.signals],
        }
