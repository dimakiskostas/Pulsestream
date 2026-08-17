import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Literal

import duckdb
from fastapi import FastAPI, Request

from pulsestream.config import settings
from pulsestream.models import RawEvent, TimeseriesPoint, TimeseriesResponse
from pulsestream.pipeline import run_consumer, run_producer
from pulsestream.sources.wikimedia import WikimediaSource
from pulsestream.storage.duck import init_db


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    con = duckdb.connect(settings.db_path)
    init_db(con)
    app.state.con = con

    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue(maxsize=settings.queue_maxsize)
    source: WikimediaSource = WikimediaSource()
    producer = asyncio.create_task(run_producer(source, queue))
    consumer = asyncio.create_task(run_consumer(queue, con))

    yield
    producer.cancel()
    queue.put_nowait(None)
    await queue.join()
    await asyncio.gather(producer, consumer, return_exceptions=True)
    con.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats/latest")
async def status_latest(request: Request) -> dict[str, object]:
    con: duckdb.DuckDBPyConnection = request.app.state.con
    row = con.execute(
        "SELECT COUNT(*), MAX(event_ts) FROM wiki_events WHERE event_ts > now() - INTERVAL 1 MINUTE"
    ).fetchone()
    if row is None:
        return {"events_last_min": 0, "latest_event_ts": None}

    count, latest = row
    return {"events_last_min": count, "latest_event_ts": latest}


@app.get("/timeseries")
async def per_bucket_counts(
    request: Request,
    bucket: Literal["10s", "1m", "5m", "1h"] = "10s",
    minutes: int = 10,
) -> TimeseriesResponse:
    bucket_intervals: dict[Literal["10s", "1m", "5m", "1h"], str] = {
        "10s": "10 seconds",
        "1m": "1 minute",
        "5m": "5 minutes",
        "1h": "1 hour",
    }
    interval = bucket_intervals[bucket]

    con: duckdb.DuckDBPyConnection = request.app.state.con
    rows = con.execute(
        f"SELECT time_bucket(INTERVAL '{interval}', event_ts) as bucket_ts, COUNT(*) as count "
        "FROM wiki_events "
        "WHERE event_ts > now() - (? * INTERVAL '{interval}') "
        "GROUP BY bucket_ts "
        "ORDER BY bucket_ts",
        [minutes],
    ).fetchall()
    points = [TimeseriesPoint(time=row[0], count=row[1]) for row in rows]
    return TimeseriesResponse(points=points)
