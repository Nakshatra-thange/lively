import httpx


class Blocked(httpx.RequestError):
    pass


class CircuitOpen(Blocked):
    pass


class RobotsDisallowed(Blocked):
    pass


class Throttled(Blocked):
    pass


class PolitenessUnavailable(Blocked):
    """A politeness control (limiter, breaker, robots) could not be enforced,
    usually because Redis is unreachable. The request is not sent."""



class UnsafeTarget(Blocked):
    pass


BLOCKING_ERRORS = (
    "CircuitOpen", "Throttled", "PolitenessUnavailable", "RobotsDisallowed", "UnsafeTarget",
)


def blocked_by(result):
    """Name of the politeness control that made a check inconclusive, or None.

    Only inconclusive results count: a check with conclusive evidence from an
    unblocked signal (e.g. the ATS API) is a real result even if the page was blocked."""
    if result.verdict != "uncertain":
        return None
    for s in result.signals:
        err = (s.details or {}).get("error")
        if err in BLOCKING_ERRORS:
            return err
    return None
