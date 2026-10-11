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
from fastapi import FastAPI, HTTPException, Query, Request

from pulsestream import __version__
from pulsestream.api.schemas import (
    AggPoint,
    AggregatesResponse,
    Bucket,
    EntityCount,
    HealthStatus,
    JobsResponse,
    JobStatus,
    LakeStats,
    PipelineStatus,
    StatsLatest,
    TaskState,
    TimeseriesPoint,
    TimeseriesResponse,
    TopEntitiesResponse,
)
from pulsestream.config import Settings
from pulsestream.config import settings as default_settings
from pulsestream.models import RawEvent
from pulsestream.pipeline import run_consumer, run_producer
from pulsestream.processing.processor import StreamProcessor
from pulsestream.scheduler.jobs import JOB_NAMES, Maintenance
from pulsestream.scheduler.runner import build_scheduler
from pulsestream.sources.base import EventSource
from pulsestream.sources.fake import FakeSource, load_fixture
from pulsestream.sources.wikimedia import WikimediaSource
from pulsestream.storage.duck import aquery, connect, ensure_lake_view, lake_row_stats
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
        maintenance = Maintenance(
            con,
            raw_store,
            aggregate_window_minutes=cfg.aggregate_window_minutes,
            compaction_grace_seconds=cfg.compaction_grace_seconds,
            compaction_min_files=cfg.compaction_min_files,
        )
        scheduler = None
        if cfg.scheduler_enabled:
            scheduler = build_scheduler(
                maintenance,
                aggregate_interval_seconds=cfg.aggregate_interval_seconds,
                compaction_interval_seconds=cfg.compaction_interval_seconds,
            )
            scheduler.start()
        app.state.con = con
        app.state.raw_store = raw_store
        app.state.processor = processor
        app.state.maintenance = maintenance
        app.state.scheduler = scheduler
        app.state.queue = queue
        app.state.producer = producer
        app.state.consumer = consumer
        try:
            yield  # ---- app runs ----
        finally:
            # Stop the clock first, then the producer, then let the consumer flush.
            if scheduler is not None:
                scheduler.shutdown(wait=False)
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
        rows = await aquery(
            request.app.state.con,
            "SELECT count(*), "
            "count(*) FILTER (WHERE event_ts > now() - INTERVAL 1 MINUTE), "
            "max(event_ts) FROM wiki_events",
        )
        total, last_min, latest = rows[0] if rows else (0, 0, None)
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
        rows = await aquery(
            request.app.state.con,
            f"SELECT time_bucket(INTERVAL '{interval}', event_ts) AS bucket_ts, count(*) "
            "FROM wiki_events "
            "WHERE event_ts > now() - (? * INTERVAL '1 minute') "
            "GROUP BY bucket_ts ORDER BY bucket_ts",
            [minutes],
        )
        points = [TimeseriesPoint(time=ts, count=n) for ts, n in rows]
        return TimeseriesResponse(bucket=bucket, minutes=minutes, points=points)

    @app.get("/aggregates/minutely", response_model=AggregatesResponse)
    async def aggregates_minutely(
        request: Request,
        minutes: int = Query(default=60, ge=1, le=7 * 24 * 60),
        entity: str | None = Query(default=None, max_length=64),
    ) -> AggregatesResponse:
        """Per-minute buckets from agg_minutely (refreshed by the scheduler, ADR 0009).

        Without ``entity`` the buckets combine every entity ("*"); with it, one wiki.
        """
        if entity is None:
            sql = (
                "SELECT bucket_ts, '*', CAST(sum(count) AS INTEGER), "
                "CAST(sum(bot_count) AS INTEGER), sum(avg_value * count) / sum(count), "
                "CAST(max(max_value) AS INTEGER), CAST(sum(anomaly_count) AS INTEGER) "
                "FROM agg_minutely WHERE bucket_ts >= now() - (? * INTERVAL '1 minute') "
                "GROUP BY bucket_ts ORDER BY bucket_ts"
            )
            params: list[object] = [minutes]
        else:
            sql = (
                "SELECT bucket_ts, entity, count, bot_count, avg_value, max_value, anomaly_count "
                "FROM agg_minutely WHERE bucket_ts >= now() - (? * INTERVAL '1 minute') "
                "AND entity = ? ORDER BY bucket_ts"
            )
            params = [minutes, entity]
        rows = await aquery(request.app.state.con, sql, params)
        points = [
            AggPoint(
                bucket_ts=r[0],
                entity=r[1],
                count=r[2],
                bot_count=r[3],
                avg_value=r[4],
                max_value=r[5],
                anomaly_count=r[6],
            )
            for r in rows
        ]
        return AggregatesResponse(minutes=minutes, entity=entity, points=points)

    @app.get("/entities/top", response_model=TopEntitiesResponse)
    async def entities_top(
        request: Request,
        minutes: int = Query(default=60, ge=1, le=7 * 24 * 60),
        limit: int = Query(default=10, ge=1, le=100),
    ) -> TopEntitiesResponse:
        """The busiest entities over the window, from agg_minutely."""
        rows = await aquery(
            request.app.state.con,
            "SELECT entity, CAST(sum(count) AS INTEGER), CAST(sum(bot_count) AS INTEGER) "
            "FROM agg_minutely WHERE bucket_ts >= now() - (? * INTERVAL '1 minute') "
            "GROUP BY entity ORDER BY 2 DESC, entity LIMIT ?",
            [minutes, limit],
        )
        entities = [EntityCount(entity=r[0], count=r[1], bot_count=r[2]) for r in rows]
        return TopEntitiesResponse(minutes=minutes, entities=entities)

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

            def scan() -> tuple[int, datetime | None, datetime | None]:
                cur = con.cursor()
                try:
                    ensure_lake_view(cur, store.glob)
                    return lake_row_stats(cur, store.glob)
                finally:
                    cur.close()

            rows, first, last = await asyncio.to_thread(scan)
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

    def job_status(request: Request, name: str) -> JobStatus:
        run = request.app.state.maintenance.status[name]
        scheduler = request.app.state.scheduler
        job = scheduler.get_job(name) if scheduler is not None else None
        next_run = job.next_run_time if job is not None else None
        return JobStatus(
            name=run.name,
            runs=run.runs,
            failures=run.failures,
            last_started_at=run.last_started_at,
            last_duration_seconds=run.last_duration_seconds,
            last_result=run.last_result,
            last_error=run.last_error,
            next_run_at=next_run.astimezone(UTC) if next_run is not None else None,
        )

    @app.get("/jobs", response_model=JobsResponse)
    async def jobs(request: Request) -> JobsResponse:
        """Periodic maintenance jobs: schedule and the outcome of their last run."""
        return JobsResponse(
            scheduler_enabled=request.app.state.scheduler is not None,
            jobs=[job_status(request, name) for name in JOB_NAMES],
        )

    @app.post("/jobs/{name}/run", response_model=JobStatus)
    async def run_job(request: Request, name: str) -> JobStatus:
        """Run a maintenance job now (demos, tests, catching up after downtime)."""
        if name not in JOB_NAMES:
            raise HTTPException(status_code=404, detail=f"unknown job {name!r}")
        await asyncio.to_thread(request.app.state.maintenance.run, name)
        return job_status(request, name)

    return app


app = create_app()
