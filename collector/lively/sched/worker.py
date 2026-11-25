import asyncio
import logging
import os
import random
import signal
import socket
import uuid

from .. import metrics
from ..config import CLAIM_SIZE, CONCURRENCY, LEASE_SECONDS
from ..liveness.boards import BoardCache
from ..liveness.engine import Engine
from ..polite.client import make_polite_client
from ..sync import record_failure, sync_company
from . import leases

log = logging.getLogger("lively.worker")


def blocked_delay(result):
    if result.verdict != "uncertain":
        return None
    for s in result.signals:
        err = (s.details or {}).get("error")
        if err in ("CircuitOpen", "Throttled"):
            return 300.0 * random.uniform(1.0, 1.5)
        if err == "RobotsDisallowed":
            return 21600.0 * random.uniform(1.0, 1.2)
    return None


class Worker:
    def __init__(self, concurrency=CONCURRENCY, claim_size=CLAIM_SIZE, lease=LEASE_SECONDS):
        self.concurrency = concurrency
        self.claim_size = claim_size
        self.lease = lease
        self.name = f"{socket.gethostname()}-{os.getpid()}"
        self.stop = asyncio.Event()
        self.work = asyncio.Queue()
        self.jobs = {}
        self.companies = {}
        self.processed = 0
        self.client = None
        self.engine = None

    async def sleep(self, seconds):
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def run(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop.set)
        async with make_polite_client(self.concurrency) as client:
            self.client = client
            self.engine = Engine(client, boards=BoardCache(ttl=300))
            consumers = [asyncio.create_task(self.consume()) for _ in range(self.concurrency)]
            beat = asyncio.create_task(self.heartbeat())
            log.info("worker %s started | concurrency %d", self.name, self.concurrency)
            try:
                await self.produce()
            finally:
                await self.shutdown(consumers, beat)
        log.info("worker %s stopped | processed %d", self.name, self.processed)

    async def produce(self):
        while not self.stop.is_set():
            if self.work.qsize() >= self.concurrency:
                await self.sleep(0.2)
                continue
            token = f"{self.name}-{uuid.uuid4().hex[:10]}"
            try:
                companies = await leases.claim_companies(token, 4, self.lease)
                jobs, reclaimed = await leases.claim_jobs(token, self.claim_size, self.lease)
            except Exception as exc:
                log.warning("claim failed: %s", exc)
                await self.sleep(2.0 + random.random())
                continue
            for c in companies:
                self.companies[c[0]] = token
                self.work.put_nowait(("company", c, token))
            for j in jobs:
                self.jobs[j.id] = token
                self.work.put_nowait(("job", j, token))
            if reclaimed:
                log.info("reclaimed %d jobs from expired leases", reclaimed)
                await metrics.count_reclaimed(reclaimed)
            if not companies and not jobs:
                await self.sleep(3.0 + random.random() * 2.0)

    async def consume(self):
        while True:
            item = await self.work.get()
            if item is None:
                self.work.task_done()
                return
            kind, payload, token = item
            try:
                if kind == "job":
                    await self.do_job(payload, token)
                else:
                    await self.do_company(payload, token)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("%s task failed: %s", kind, exc)
                await metrics.count_worker_error(kind)
            finally:
                if kind == "job":
                    self.jobs.pop(payload.id, None)
                else:
                    self.companies.pop(payload[0], None)
                self.processed += 1
                self.work.task_done()

    async def do_job(self, job, token):
        result = await self.engine.check(job)
        delay = blocked_delay(result)
        if delay:
            await leases.defer_job(job.id, token, delay)
            await metrics.count_deferred()
            return
        outcome = await leases.complete_job(job, token, result)
        if outcome is None:
            await metrics.count_lost_lease()
            return
        closed_now = outcome.new_state == "closed" and outcome.old_state != "closed"
        await metrics.record_check(result.verdict, closed_now)

    async def do_company(self, company, token):
        company_id, source, slug, first, _rate, _fails = company
        try:
            stats = await sync_company(self.client, (company_id, source, slug))
        except Exception as exc:
            await record_failure(company_id, repr(exc))
            stats = {"errors": 1}
        await leases.finish_company(company_id, token, stats, first)
        await metrics.count_sync(stats)

    async def heartbeat(self):
        while True:
            await asyncio.sleep(self.lease / 3.0)
            try:
                await leases.renew(self.lease, list(self.jobs.items()), list(self.companies.items()))
            except Exception as exc:
                log.warning("lease renewal failed: %s", exc)
            log.info(
                "processed %d | held %d | queued %d",
                self.processed,
                len(self.jobs) + len(self.companies),
                self.work.qsize(),
            )

    async def shutdown(self, consumers, beat):
        released_jobs = []
        released_companies = []
        while True:
            try:
                kind, payload, token = self.work.get_nowait()
            except asyncio.QueueEmpty:
                break
            if kind == "job":
                released_jobs.append((payload.id, token))
                self.jobs.pop(payload.id, None)
            else:
                released_companies.append((payload[0], token))
                self.companies.pop(payload[0], None)
            self.work.task_done()
        try:
            await leases.release(released_jobs, released_companies)
        except Exception as exc:
            log.warning("release failed: %s", exc)
        for _ in consumers:
            self.work.put_nowait(None)
        _, pending = await asyncio.wait(consumers, timeout=30)
        for task in pending:
            task.cancel()
        beat.cancel()