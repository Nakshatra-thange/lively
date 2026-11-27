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
from ..migrate import wait_for_migrations
from ..polite.client import make_polite_client
from ..polite.errors import blocked_by
from ..sync import record_failure, sync_company
from . import leases

log = logging.getLogger("lively.worker")


def blocked_delay(result):
    err = blocked_by(result)
    if err in ("CircuitOpen", "Throttled"):
        return 300.0 * random.uniform(1.0, 1.5)
    if err == "PolitenessUnavailable":
        # Politeness controls (Redis) were unavailable, nothing was fetched: retry soon.
        return 60.0 * random.uniform(1.0, 2.0)
    if err in ("RobotsDisallowed", "UnsafeTarget"):
        return 21600.0 * random.uniform(1.0, 1.2)
    return None


ERROR_RETRY = 300.0
SHUTDOWN_TIMEOUT = 30.0


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
        self.consumers = []
        self.beat = None
        self.shutdown_timeout = SHUTDOWN_TIMEOUT

    async def sleep(self, seconds):
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def run(self):
        await wait_for_migrations()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop.set)
        async with make_polite_client(self.concurrency) as client:
            self.client = client
            self.engine = Engine(client, boards=BoardCache(ttl=300))
            self.consumers = [asyncio.create_task(self.consume()) for _ in range(self.concurrency)]
            self.beat = asyncio.create_task(self.heartbeat())
            log.info("worker %s started | concurrency %d", self.name, self.concurrency)
            try:
                await self.produce()
            finally:
                await self.shutdown()
        log.info("worker %s stopped | processed %d", self.name, self.processed)

    def supervise(self):
        """Make unexpected consumer/heartbeat exits visible and replace the task.
        Work held by a dead task is dropped from self.jobs/companies by its
        finally block, so its lease is no longer renewed and simply expires."""
        for i, task in enumerate(self.consumers):
            if task.done():
                exc = None if task.cancelled() else task.exception()
                log.error("consumer %d exited unexpectedly (%r); restarting it", i, exc or "cancelled")
                self.consumers[i] = asyncio.create_task(self.consume())
        if self.beat is not None and self.beat.done():
            exc = None if self.beat.cancelled() else self.beat.exception()
            log.error("heartbeat exited unexpectedly (%r); restarting it", exc or "cancelled")
            self.beat = asyncio.create_task(self.heartbeat())

    async def produce(self):
        while not self.stop.is_set():
            self.supervise()
            if self.work.qsize() >= self.concurrency:
                await self.sleep(0.2)
                continue
            token = f"{self.name}-{uuid.uuid4().hex[:10]}"
            # Enqueue each claim as soon as it succeeds, so a failure in the second
            # claim never strands leases from the first.
            companies, jobs, reclaimed = [], [], 0
            try:
                companies = await leases.claim_companies(token, 4, self.lease)
                for c in companies:
                    self.companies[c[0]] = token
                    self.work.put_nowait(("company", c, token))
                jobs, reclaimed = await leases.claim_jobs(token, self.claim_size, self.lease)
                for j in jobs:
                    self.jobs[j.id] = token
                    self.work.put_nowait(("job", j, token))
            except Exception as exc:
                log.warning("claim failed: %r", exc)
                await self.sleep(2.0 + random.random())
                continue
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
                await self.on_failure(kind, payload, token, exc)
            finally:
                if kind == "job":
                    self.jobs.pop(payload.id, None)
                else:
                    self.companies.pop(payload[0], None)
                self.processed += 1
                self.work.task_done()

    async def on_failure(self, kind, payload, token, exc):
        """One failed item must not affect others. Never raises."""
        ident = payload.id if kind == "job" else payload[0]
        log.warning("%s %s failed: %r", kind, ident, exc, exc_info=exc)
        await metrics.count_worker_error(kind)
        if kind != "job":
            # Company: lease expires and it is reclaimed (sync failures inside
            # do_company are already recorded and rescheduled by finish_company).
            return
        try:
            # Release the lease with a backoff so a poison job cannot hot-loop.
            # Token-guarded: a no-op if the lease was already lost to another worker.
            await leases.defer_job(payload.id, token, ERROR_RETRY * random.uniform(1.0, 1.5))
        except Exception as defer_exc:
            log.warning("could not defer job %s; its lease will expire instead: %r",
                        payload.id, defer_exc)

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
                # Not fatal: if renewal keeps failing, leases expire and the work is
                # reclaimed; complete_job then discards this worker's stale result.
                log.warning("lease renewal failed: %r", exc)
            log.info(
                "processed %d | held %d | queued %d",
                self.processed,
                len(self.jobs) + len(self.companies),
                self.work.qsize(),
            )

    async def shutdown(self):
        released_jobs = []
        released_companies = []
        while True:
            try:
                item = self.work.get_nowait()
            except asyncio.QueueEmpty:
                break
            self.work.task_done()
            if item is None:
                continue
            kind, payload, token = item
            if kind == "job":
                released_jobs.append((payload.id, token))
                self.jobs.pop(payload.id, None)
            else:
                released_companies.append((payload[0], token))
                self.companies.pop(payload[0], None)
        await self.release(released_jobs, released_companies, "queued")

        for _ in self.consumers:
            self.work.put_nowait(None)
        _, pending = await asyncio.wait(self.consumers, timeout=self.shutdown_timeout)
        # Work still in flight after the grace period: snapshot its leases before
        # cancelling (the consumers' finally blocks drop them from the maps).
        active_jobs = list(self.jobs.items())
        active_companies = list(self.companies.items())
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if pending:
            # Token-guarded release: no-op for anything that completed meanwhile.
            await self.release(active_jobs, active_companies, "in-flight")
        if self.beat is not None:
            self.beat.cancel()
            await asyncio.gather(self.beat, return_exceptions=True)

    async def release(self, jobs, companies, what):
        if not jobs and not companies:
            return
        try:
            await leases.release(jobs, companies)
            log.info("released %d %s job and %d company leases", len(jobs), what, len(companies))
        except Exception as exc:
            log.warning("release of %s leases failed; they will expire: %r", what, exc)
