import asyncio
import logging
from collections import Counter

from . import store
from .engine import Engine
from .fetcher import make_client

log = logging.getLogger("lively.check")


async def check_batch(states, limit, concurrency):
    jobs = await store.due_jobs(states, limit)
    log.info("checking %d jobs", len(jobs))
    verdicts = Counter()
    transitions = Counter()
    done = 0
    sem = asyncio.Semaphore(concurrency)

    async with make_client(concurrency) as client:
        engine = Engine(client)

        async def work(job):
            nonlocal done
            async with sem:
                try:
                    result = await engine.check(job)
                    new_state = await store.save_result(job, result)
                except Exception as exc:
                    log.warning("job %s failed: %s", job.id, exc)
                    verdicts["error"] += 1
                    done += 1
                    return
            verdicts[result.verdict] += 1
            if new_state and new_state != job.state:
                transitions[f"{job.state}->{new_state}"] += 1
            done += 1
            if done % 25 == 0 or done == len(jobs):
                log.info("%d/%d | %s | %s", done, len(jobs), dict(verdicts), dict(transitions))

        await asyncio.gather(*(work(j) for j in jobs))

    log.info("finished | verdicts %s | transitions %s", dict(verdicts), dict(transitions))
