import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

from .. import metrics
from ..db import pool
from ..ghost.evaluate import metrics as eval_metrics
from ..liveness import Engine
from ..liveness.boards import BoardCache
from ..liveness.resolve import resolve
from ..polite.client import make_polite_client
from ..redisx import close_redis, get_redis
from . import queries

WEB_DIR = Path(os.getenv("WEB_DIR", "/web"))
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
CHECK_PER_MIN = int(os.getenv("CHECK_PER_MIN", "10"))
STATES = {"closed", "suspect", "reposted", "live", "new"}

_cache = {}


async def cached(key, ttl, fn):
    now = time.monotonic()
    hit = _cache.get(key)
    if hit and hit[0] > now:
        return hit[1]
    value = await fn()
    _cache[key] = (now + ttl, value)
    return value


async def limited(request, bucket, limit):
    ip = request.client.host if request.client else "unknown"
    if os.getenv("TRUST_PROXY") == "1":
        ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or ip
    r = get_redis()
    key = f"api:{bucket}:{ip}:{int(time.time() // 60)}"
    n = await r.incr(key)
    if n == 1:
        await r.expire(key, 90)
    return n > limit


@asynccontextmanager
async def lifespan(app):
    await pool.open()
    client = make_polite_client(8)
    app.state.engine = Engine(client, boards=BoardCache(ttl=300))
    try:
        yield
    finally:
        await client.aclose()
        await close_redis()
        await pool.close()


app = FastAPI(title="Lively", version="1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


class CheckRequest(BaseModel):
    url: str


@app.get("/")
async def index():
    page = WEB_DIR / "index.html"
    if page.exists():
        return FileResponse(page)
    return JSONResponse({"service": "lively", "docs": "/docs"})


@app.get("/config.js")
async def config_js():
    return Response("window.LIVELY_API = '';\n", media_type="application/javascript")


@app.get("/health")
async def health():
    row = await queries.fetch_one("SELECT 1 AS ok")
    return {"ok": bool(row)}


@app.get("/jobs/{job_id}/status")
async def job_status(job_id: int):
    data = await queries.job_status(job_id)
    if data is None:
        raise HTTPException(404, "job not found")
    return data


@app.post("/check")
async def check(body: CheckRequest, request: Request):
    if await limited(request, "check", CHECK_PER_MIN):
        raise HTTPException(429, "too many checks, try again in a minute")
    url = body.url.strip()
    if len(url) > 2000 or not url.lower().startswith(("http://", "https://")):
        raise HTTPException(400, "paste a full http or https job link")
    job = await resolve(url)
    if job is None:
        raise HTTPException(400, "that link could not be read")
    try:
        result = await asyncio.wait_for(request.app.state.engine.check(job), 45)
    except asyncio.TimeoutError:
        raise HTTPException(504, "the job page took too long to answer")

    payload = {
        "known": job.id is not None,
        "id": job.id,
        "title": job.title or None,
        "company": None,
        "url": job.url,
        "source": job.source,
        "location": None,
        "state": job.state,
        "ghost": None,
        "schedule": None,
        "events": [],
    }
    if job.id is not None:
        base = await queries.job_status(job.id)
        if base:
            payload.update(base)
    payload.update(
        verdict=result.verdict,
        confidence=round(result.confidence, 2),
        reasons=result.reasons or ["no signal was conclusive"],
        signals=[s.to_dict() for s in result.signals],
        checked_at=datetime.now(timezone.utc),
        fresh=True,
    )
    return payload


@app.get("/search")
async def search(q: str = "", limit: int = Query(8, ge=1, le=20)):
    q = q.strip()[:100]
    if not q:
        return {"items": await queries.fetch_all(queries.SAMPLE, (limit,))}
    pattern = "%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    return {"items": await queries.fetch_all(queries.SEARCH, (pattern, pattern, limit))}


@app.get("/feed")
async def feed(
    kind: str = "all",
    hours: int = Query(24, ge=1, le=87600),
    limit: int = Query(40, ge=1, le=100),
):
    if kind not in ("all", "closed", "ghost", "repost"):
        raise HTTPException(400, "kind must be all, closed, ghost or repost")
    wide = kind != "all"
    items = []
    if kind in ("all", "closed"):
        cap = limit if wide else max(1, limit // 2)
        for r in await queries.fetch_all(queries.CLOSED_FEED, (hours, cap)):
            items.append(
                {
                    "kind": "closed",
                    "id": r["id"],
                    "title": r["title"],
                    "company": r["company"],
                    "url": r["url"],
                    "at": r["at"],
                    "score": None,
                    "reasons": [r["reason"]] if r["reason"] else [],
                }
            )
    for label in ("ghost", "repost"):
        if kind in ("all", label):
            cap = limit if wide else max(1, limit // 4)
            for r in await queries.fetch_all(queries.SCORED_FEED, (label, hours, cap)):
                items.append(
                    {
                        "kind": label,
                        "id": r["id"],
                        "title": r["title"],
                        "company": r["company"],
                        "url": r["url"],
                        "at": r["at"],
                        "score": round(r["score"], 2) if r["score"] is not None else None,
                        "reasons": r["reasons"] or [],
                    }
                )
    items.sort(key=lambda i: i["at"], reverse=True)
    return {"kind": kind, "hours": hours, "items": items[:limit]}


async def _dashboard():
    snap = await metrics.snapshot()
    rows = await queries.fetch_all(queries.LABELS)
    snap["labels"] = {r["label"]: r["n"] for r in rows}
    return snap


@app.get("/dashboard")
async def dashboard():
    return await cached("dashboard", 3, _dashboard)


async def _evaluation():
    rows = await queries.fetch_all(queries.EVAL)
    if not rows:
        return {"total": 0}
    return eval_metrics([(r["label"], r["predicted"]) for r in rows])


@app.get("/evaluation")
async def evaluation():
    return await cached("evaluation", 60, _evaluation)


async def event_stream(request, state, last_id, replay):
    if last_id is None:
        last_id = (await queries.fetch_one(queries.START, (state, replay)))["start"]
    yield "retry: 3000\n\n"
    idle = 0
    while True:
        if await request.is_disconnected():
            return
        rows = await queries.fetch_all(queries.NEW_EVENTS, (last_id, state))
        for r in rows:
            last_id = r["id"]
            yield f"id: {r['id']}\nevent: job.{state}\ndata: {json.dumps(r, default=str)}\n\n"
        if rows:
            idle = 0
        else:
            idle += 1
            if idle % 7 == 0:
                yield ": ping\n\n"
        await asyncio.sleep(2)


@app.get("/events")
async def events(request: Request, state: str = "closed", replay: int = Query(0, ge=0, le=50)):
    if state not in STATES:
        raise HTTPException(400, "state must be one of " + ", ".join(sorted(STATES)))
    header = request.headers.get("last-event-id", "")
    last_id = int(header) if header.isdigit() else None
    return StreamingResponse(
        event_stream(request, state, last_id, replay),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )