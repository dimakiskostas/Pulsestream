import asyncio
from contextlib import asynccontextmanager

import duckdb
from fastapi import FASTAPI, Request

from pulsestream.config import settings
from pulsestream.models import RawEvent
from pulsestream.pipeline import run_consumer, run_producer
from pulsestream.sources.wikimedia import Wikimedia
from pulsestream.storage.duck import init_db


@asynccontextmanager
async def lifespan(app: FASTAPI):
    con = duckdb.connect(settings.db_path)
    init_db(con)
    app.state.con = con

    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue(maxsize=settings.queue_maxsize)
    source: Wikimedia = Wikimedia()
    producer = asyncio.create_task(run_producer(source, queue))
    consumer = asyncio.create_task(run_consumer(queue, con))

    yield

    producer.cancel()
    queue.put_nowait(None)
    await queue.join()
    consumer.cancel()
    await asyncio.gather(producer, consumer, return_exceptions=True)
    con.close()


app = FASTAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats/latest")
async def status_latest(request: Request) -> dict[str, str]:
    con: duckdb.DuckDBPyConnection = request.app.state.con
    count, latest = con.execute(
        "SELECT COUNT(*), MAX(event_ts) FROM wiki_eventsWHERE event_ts > now() - INTERVAL 1 MINUTE"
    ).fetchone()
    return {"events_last_min": count, "latest_event_ts": latest}


@app.get("timeseries?bucket10s&minutes=10")
async def per_bucket_counts(request: Request) -> dict[str, str]:
    con: duckdb.DuckDBPyConnection = request.app.state.con
    time_bucket = con.execute(
        "SELECT time_bucket("
        "10 minutes"
        ", event_ts)"
        "as 10 minutes"
        "FROM wiki_events"
        "GROUP BY 10 minutes"
        "ORDER BY 10 minutes"
    ).fetchone()
    return time_bucket
