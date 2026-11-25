import asyncio

from .boards import BoardCache
from .checkers import default_checkers
from .combine import combine
from .context import Context
from .models import Signal


class Engine:
    def __init__(self, client, checkers=None, boards=None):
        self.client = client
        self.checkers = checkers if checkers is not None else default_checkers()
        self.boards = boards or BoardCache()

    async def _run(self, checker, ctx):
        try:
            return await checker.run(ctx)
        except Exception as exc:
            return Signal(
                checker.name,
                "uncertain",
                0.0,
                f"{checker.name} crashed: {type(exc).__name__}",
                {"error": str(exc)[:200]},
            )

    async def check(self, job):
        ctx = Context(job, self.client, self.boards)
        found = await asyncio.gather(*(self._run(c, ctx) for c in self.checkers))
        result = combine([s for s in found if s is not None])
        if ctx.page_result is not None:
            result.http_status = ctx.page_result.status
            result.latency_ms = ctx.page_result.latency_ms
        return result
