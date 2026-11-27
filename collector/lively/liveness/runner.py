import asyncio
import logging
from collections import Counter

from ..polite.client import make_polite_client
from ..polite.errors import blocked_by
from . import store
from .engine import Engine

log = logging.getLogger("lively.check")


async def check_batch(states, limit, concurrency):
    jobs = await store.due_jobs(states, limit)
    log.info("checking %d jobs", len(jobs))
    verdicts = Counter()
    transitions = Counter()
    done = 0
    sem = asyncio.Semaphore(concurrency)

    async with make_polite_client(concurrency) as client:
        engine = Engine(client)

        async def work(job):
            nonlocal done
            async with sem:
                try:
                    result = await engine.check(job)
                    blocked = blocked_by(result)
                    # A blocked check is not evidence: save nothing (no check row,
                    # no state change, schedule untouched) and report it.
                    new_state = None if blocked else await store.save_result(job, result)
                except Exception as exc:
                    log.warning("job %s failed: %s", job.id, exc)
                    verdicts["error"] += 1
                    done += 1
                    return
            if blocked:
                verdicts[f"blocked:{blocked}"] += 1
                log.info("job %s not checked: blocked by %s", job.id, blocked)
            else:
                verdicts[result.verdict] += 1
            if new_state and new_state != job.state:
                transitions[f"{job.state}->{new_state}"] += 1
            done += 1
            if done % 25 == 0 or done == len(jobs):
                log.info("%d/%d | %s | %s", done, len(jobs), dict(verdicts), dict(transitions))

        await asyncio.gather(*(work(j) for j in jobs))

    log.info("finished | verdicts %s | transitions %s", dict(verdicts), dict(transitions))
    n_blocked = sum(v for k, v in verdicts.items() if k.startswith("blocked:"))
    if n_blocked:
        log.warning("%d jobs were blocked by politeness controls and not saved; rerun later", n_blocked)
    return dict(verdicts)
