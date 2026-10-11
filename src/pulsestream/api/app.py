"""FastAPI application: the lifespan owns the pipeline and the DuckDB connection.

Tutorial Part 1.6. ``create_app`` is a factory so tests can inject settings and a
replay source; the module-level ``app`` is what uvicorn serves
(``pulsestream api`` -> ``uvicorn pulsestream.api.app:app``).
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import duckdb
from fastapi import FastAPI, Query, Request

from pulsestream import __version__
from pulsestream.api.schemas import (
    Bucket,
    HealthStatus,
    LakeStats,
    PipelineStatus,
    StatsLatest,
    TaskState,
    TimeseriesPoint,
    TimeseriesResponse,
)
from pulsestream.config import Settings
from pulsestream.config import settings as default_settings
from pulsestream.models import RawEvent
from pulsestream.pipeline import run_consumer, run_producer
from pulsestream.processing.processor import StreamProcessor
from pulsestream.sources.base import EventSource
from pulsestream.sources.fake import FakeSource, load_fixture
from pulsestream.sources.wikimedia import WikimediaSource
from pulsestream.storage.duck import connect, ensure_lake_view, lake_row_stats
from pulsestream.storage.parquet import ParquetRawStore

BUCKET_INTERVALS: dict[Bucket, str] = {
    "10s": "10 seconds",
    "1m": "1 minute",
    "5m": "5 minutes",
    "1h": "1 hour",
}


def build_source(cfg: Settings) -> EventSource:
    """The one place that knows which concrete source is wired in (tutorial Part 1.1)."""
    if cfg.source == "fake":
        return FakeSource(load_fixture(cfg.fixture_path), delay=cfg.fixture_delay)
    return WikimediaSource(cfg.stream_url, user_agent=cfg.user_agent, read_timeout=cfg.read_timeout)


def _task_state(task: asyncio.Task[None]) -> TaskState:
    if not task.done():
        return "running"
    if task.cancelled() or task.exception() is None:
        return "done"
    return "failed"


def create_app(cfg: Settings | None = None, source: EventSource | None = None) -> FastAPI:
    cfg = cfg if cfg is not None else default_settings

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        con = connect(cfg.db_path)
        queue: asyncio.Queue[RawEvent | None] = asyncio.Queue(maxsize=cfg.queue_maxsize)
        src = source if source is not None else build_source(cfg)
        raw_store = ParquetRawStore(cfg.lake_root) if cfg.lake_enabled else None
        processor = StreamProcessor(con, raw_store)
        producer = asyncio.create_task(run_producer(src, queue), name="producer")
        consumer = asyncio.create_task(
            run_consumer(queue, processor, cfg.batch_size, cfg.flush_interval), name="consumer"
        )
        app.state.con = con
        app.state.raw_store = raw_store
        app.state.processor = processor
        app.state.queue = queue
        app.state.producer = producer
        app.state.consumer = consumer
        try:
            yield  # ---- app runs ----
        finally:
            # Stop the producer first, then let the consumer flush what is buffered.
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            if not consumer.done():
                try:
                    await asyncio.wait_for(queue.put(None), timeout=1.0)
                    await asyncio.wait_for(consumer, timeout=cfg.flush_interval + 5.0)
                except TimeoutError:
                    consumer.cancel()
                    await asyncio.gather(consumer, return_exceptions=True)
            con.close()

    app = FastAPI(title="PulseStream", version=__version__, lifespan=lifespan)

    @app.get("/health", response_model=HealthStatus)
    async def health(request: Request) -> HealthStatus:
        state = request.app.state
        pipeline = PipelineStatus(
            producer=_task_state(state.producer),
            consumer=_task_state(state.consumer),
            queue_depth=state.queue.qsize(),
        )
        degraded = pipeline.consumer == "failed"
        return HealthStatus(status="degraded" if degraded else "ok", pipeline=pipeline)

    @app.get("/stats/latest", response_model=StatsLatest)
    async def stats_latest(request: Request) -> StatsLatest:
        con: duckdb.DuckDBPyConnection = request.app.state.con
        row = con.execute(
            "SELECT count(*), "
            "count(*) FILTER (WHERE event_ts > now() - INTERVAL 1 MINUTE), "
            "max(event_ts) FROM wiki_events"
        ).fetchone()
        total, last_min, latest = row if row is not None else (0, 0, None)
        lag = (datetime.now(UTC) - latest).total_seconds() if latest is not None else None
        return StatsLatest(
            events_total=total, events_last_min=last_min, latest_event_ts=latest, lag_seconds=lag
        )

    @app.get("/timeseries", response_model=TimeseriesResponse)
    async def timeseries(
        request: Request,
        bucket: Bucket = "10s",
        minutes: int = Query(default=10, ge=1, le=7 * 24 * 60),
    ) -> TimeseriesResponse:
        """Per-bucket event counts over the last ``minutes`` minutes (tutorial 1.6, your turn)."""
        interval = BUCKET_INTERVALS[bucket]  # allow-listed literal, never user text in SQL
        con: duckdb.DuckDBPyConnection = request.app.state.con
        rows = con.execute(
            f"SELECT time_bucket(INTERVAL '{interval}', event_ts) AS bucket_ts, count(*) "
            "FROM wiki_events "
            "WHERE event_ts > now() - (? * INTERVAL '1 minute') "
            "GROUP BY bucket_ts ORDER BY bucket_ts",
            [minutes],
        ).fetchall()
        points = [TimeseriesPoint(time=ts, count=n) for ts, n in rows]
        return TimeseriesResponse(bucket=bucket, minutes=minutes, points=points)

    @app.get("/lake", response_model=LakeStats)
    async def lake(request: Request) -> LakeStats:
        """The raw store: files on disk and rows as DuckDB counts them through the view."""
        store: ParquetRawStore | None = request.app.state.raw_store
        if store is None:
            return LakeStats(
                enabled=False, root=None, files=0, partitions=0, bytes=0, rows=0,
                first_received_at=None, last_received_at=None,
            )  # fmt: skip
        files = store.files()
        rows, first, last = 0, None, None
        if files:
            con: duckdb.DuckDBPyConnection = request.app.state.con
            ensure_lake_view(con, store.glob)
            rows, first, last = lake_row_stats(con, store.glob)
        return LakeStats(
            enabled=True,
            root=str(store.root),
            files=len(files),
            partitions=len(store.partitions()),
            bytes=sum(f.stat().st_size for f in files),
            rows=rows,
            first_received_at=first,
            last_received_at=last,
        )

    return app


app = create_app()
