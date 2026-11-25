from ...sources import SOURCES
from ..models import Signal
from .base import Checker


class BoardListing(Checker):
    name = "board_listing"

    async def run(self, ctx):
        job = ctx.job
        if job.source not in SOURCES or not job.slug or not job.external_id:
            return None
        status, ids = await ctx.boards.ids(ctx.client, job.source, job.slug)
        details = {"board_size": len(ids)}

        if status == "error":
            return Signal(
                self.name, "uncertain", 0.0, "company board could not be fetched", details
            )
        if status == "missing":
            return Signal(self.name, "uncertain", 0.2, "company board not found", details)
        if job.external_id in ids:
            return Signal(self.name, "live", 0.9, "listed on the company's job board", details)
        if not ids:
            return Signal(self.name, "closed", 0.6, "company job board is empty", details)
        return Signal(
            self.name, "closed", 0.9, "no longer listed on the company's job board", details
        )
