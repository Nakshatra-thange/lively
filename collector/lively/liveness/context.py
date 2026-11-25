import asyncio

from .fetcher import fetch


class Context:
    def __init__(self, job, client, boards):
        self.job = job
        self.client = client
        self.boards = boards
        self.page_result = None
        self._page_task = None

    async def page(self):
        if self._page_task is None:
            self._page_task = asyncio.ensure_future(fetch(self.client, self.job.url))
        result = await self._page_task
        self.page_result = result
        return result
