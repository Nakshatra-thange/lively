import httpx


class Blocked(httpx.RequestError):
    pass


class CircuitOpen(Blocked):
    pass


class RobotsDisallowed(Blocked):
    pass


class Throttled(Blocked):
    pass



class UnsafeTarget(Blocked):
    pass