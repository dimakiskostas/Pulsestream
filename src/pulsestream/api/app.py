"""FastAPI application: the lifespan owns the pipeline and the DuckDB connection.

``create_app`` is a factory so tests can inject settings, a
replay source or a bus; the module-level ``app`` is what uvicorn serves
(``pulsestream api`` -> ``uvicorn pulsestream.api.app:app``).

With ``PULSE_BUS=redis`` the process reads the bus instead of the live source
(``BusSource``), acknowledges each micro-batch after it is written, and keeps
the bus gauges fresh; ingestion then runs in ``pulsestream ingest``. The rest of
the process, the DuckDB writer, the detectors, the scheduler and every endpoint,
is the same in both modes.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import duckdb
from fastapi import FastAPI, HTTPException, Query, Request, Response

from pulsestream import __version__
from pulsestream.api.schemas import (
    AggPoint,
    AggregatesResponse,
    AnomaliesResponse,
    AnomalyOut,
    AnomalySummary,
    Bucket,
    BusStatus,
    EntityAnomalies,
    EntityCount,
    FeaturePoint,
    FeaturesResponse,
    HealthStatus,
    JobsResponse,
    JobStatus,
    LakeStats,
    LivenessStatus,
    PipelineStatus,
    ReadinessStatus,
    StatsLatest,
    TaskState,
    TimeseriesPoint,
    TimeseriesResponse,
    TopEntitiesResponse,
)
from pulsestream.bus.base import EventBus
from pulsestream.bus.monitor import keep_stats_fresh
from pulsestream.common import metrics
from pulsestream.config import Settings
from pulsestream.config import settings as default_settings
from pulsestream.models import Metric, RawEvent, Severity
from pulsestream.pipeline import BatchHandler, run_consumer, run_producer
from pulsestream.processing.detection import DetectionStage
from pulsestream.processing.processor import StreamProcessor
from pulsestream.scheduler.jobs import JOB_NAMES, Maintenance
from pulsestream.scheduler.runner import build_scheduler
from pulsestream.sources.base import EventSource
from pulsestream.sources.bus import BusSource
from pulsestream.storage.duck import aquery, connect, ensure_lake_view, lake_row_stats
from pulsestream.storage.parquet import ParquetRawStore
from pulsestream.wiring import build_bus, build_source, consumer_name

BUCKET_INTERVALS: dict[Bucket, str] = {
    "10s": "10 seconds",
    "1m": "1 minute",
    "5m": "5 minutes",
    "1h": "1 hour",
}


def _task_state(task: asyncio.Task[None]) -> TaskState:
    if not task.done():
        return "running"
    if task.cancelled() or task.exception() is None:
        return "done"
    return "failed"


def create_app(
    cfg: Settings | None = None, source: EventSource | None = None, bus: EventBus | None = None
) -> FastAPI:
    cfg = cfg if cfg is not None else default_settings

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        con = connect(cfg.db_path)
        queue: asyncio.Queue[RawEvent | None] = asyncio.Queue(maxsize=cfg.queue_maxsize)
        the_bus: EventBus | None = None
        bus_source: BusSource | None = None
        if cfg.bus == "redis":
            the_bus = bus if bus is not None else build_bus(cfg)
            bus_source = BusSource(the_bus, cfg.bus_group, consumer_name(cfg))
        src: EventSource
        if source is not None:
            src = source
        elif bus_source is not None:
            src = bus_source
        else:
            src = build_source(cfg)
        # Acknowledge a batch only after its rows are written, and only when the events
        # came from the bus (an injected replay source has nothing to acknowledge).
        after_batch: BatchHandler | None = bus_source.ack if src is bus_source else None
        raw_store = ParquetRawStore(cfg.lake_root) if cfg.lake_enabled else None
        detection = DetectionStage.from_settings(cfg) if cfg.detection_enabled else None
        processor = StreamProcessor(con, raw_store, detection)
        producer = asyncio.create_task(run_producer(src, queue), name="producer")
        consumer = asyncio.create_task(
            run_consumer(
                queue, processor, cfg.batch_size, cfg.flush_interval, after_batch=after_batch
            ),
            name="consumer",
        )
        monitor = (
            asyncio.create_task(
                keep_stats_fresh(the_bus, cfg.bus_group, cfg.bus_stats_interval_seconds),
                name="bus-monitor",
            )
            if the_bus is not None
            else None
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
        app.state.started_at = datetime.now(UTC)
        app.state.source = src
        app.state.bus = the_bus
        app.state.bus_source = bus_source
        app.state.con = con
        app.state.raw_store = raw_store
        app.state.detection = detection
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
            if monitor is not None:
                monitor.cancel()
                await asyncio.gather(monitor, return_exceptions=True)
            if the_bus is not None and bus is None:  # we built it, we close it
                await the_bus.close()
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

    @app.get("/health/live", response_model=LivenessStatus)
    async def live() -> LivenessStatus:
        """Liveness: the process is up and serving. Nothing else is checked."""
        return LivenessStatus(status="alive")

    @app.get("/health/ready", response_model=ReadinessStatus)
    async def ready(request: Request, response: Response) -> ReadinessStatus:
        """Readiness: every stage can do useful work; 503 with the failing checks otherwise."""
        state = request.app.state
        now = datetime.now(UTC)
        processed_at = state.processor.last_processed_at
        since = processed_at if processed_at is not None else state.started_at
        silence = (now - since).total_seconds()
        producer = state.producer
        producer_failed = (
            producer.done() and not producer.cancelled() and producer.exception() is not None
        )
        try:
            await aquery(state.con, "SELECT 1")
            store_ok = True
        except Exception:
            store_ok = False
        checks = {
            "consumer": not state.consumer.done(),
            "producer": not producer_failed,
            "store": store_ok,
            "source": bool(getattr(state.source, "connected", True)),
            "fresh": silence <= cfg.ready_max_silence_seconds,
        }
        if state.bus is not None:
            checks["bus"] = await state.bus.ping()
        is_ready = all(checks.values())
        response.status_code = 200 if is_ready else 503
        return ReadinessStatus(
            ready=is_ready,
            checks=checks,
            last_processed_at=processed_at,
            silence_seconds=round(silence, 3),
        )

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        """Prometheus text exposition of every pulsestream_* series."""
        body, content_type = metrics.render()
        return Response(content=body, media_type=content_type)

    @app.get("/bus", response_model=BusStatus)
    async def bus_status(request: Request) -> BusStatus:
        """The stream and this process's consumer group; mode "none" without a bus."""
        state = request.app.state
        if state.bus is None:
            return BusStatus(mode="none")
        try:
            stats = await state.bus.stats(cfg.bus_group)
        except Exception as exc:
            detail = f"bus unreachable: {type(exc).__name__}: {exc}"
            raise HTTPException(status_code=503, detail=detail) from exc
        src: BusSource | None = state.bus_source
        return BusStatus(
            mode="redis",
            stream=cfg.bus_stream,
            group=cfg.bus_group,
            consumer=src.consumer if src is not None else None,
            length=stats.length,
            first_id=stats.first_id,
            last_id=stats.last_id,
            pending=stats.pending,
            lag=stats.lag,
            inflight=src.inflight if src is not None else 0,
            delivered_total=src.delivered_total if src is not None else 0,
            acked_total=src.acked_total if src is not None else 0,
        )

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
        """Per-bucket event counts over the last ``minutes`` minutes."""
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

    @app.get("/anomalies", response_model=AnomaliesResponse)
    async def anomalies(
        request: Request,
        minutes: int = Query(default=60, ge=1, le=7 * 24 * 60),
        entity: str | None = Query(default=None, max_length=64),
        metric: Metric | None = None,
        severity: Severity | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> AnomaliesResponse:
        """Findings whose window started within the last ``minutes``, newest first."""
        clauses = ["window_start >= now() - (? * INTERVAL '1 minute')"]
        params: list[object] = [minutes]
        for column, value in (("entity", entity), ("metric", metric), ("severity", severity)):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        rows = await aquery(
            request.app.state.con,
            "SELECT anomaly_id, detected_at, entity, metric, value, expected, score, method, "
            "severity, window_start, window_end, event_id, model_version, details "
            f"FROM anomalies WHERE {' AND '.join(clauses)} "
            "ORDER BY window_start DESC, entity LIMIT ?",
            [*params, limit],
        )
        return AnomaliesResponse(
            minutes=minutes,
            entity=entity,
            metric=metric,
            severity=severity,
            anomalies=[AnomalyOut.from_row(r) for r in rows],
        )

    @app.get("/anomalies/summary", response_model=AnomalySummary)
    async def anomalies_summary(
        request: Request, minutes: int = Query(default=60, ge=1, le=7 * 24 * 60)
    ) -> AnomalySummary:
        """Counts of findings in the window by severity and metric, and the entities with most."""
        con = request.app.state.con
        where = "FROM anomalies WHERE window_start >= now() - (? * INTERVAL '1 minute')"
        by_severity = await aquery(con, f"SELECT severity, count(*) {where} GROUP BY 1", [minutes])
        by_metric = await aquery(con, f"SELECT metric, count(*) {where} GROUP BY 1", [minutes])
        top = await aquery(
            con,
            f"SELECT entity, count(*) AS n {where} GROUP BY 1 ORDER BY n DESC, entity LIMIT 10",
            [minutes],
        )
        return AnomalySummary(
            minutes=minutes,
            total=sum(int(n) for _, n in by_severity),
            by_severity={s: int(n) for s, n in by_severity},
            by_metric={m: int(n) for m, n in by_metric},
            top_entities=[EntityAnomalies(entity=e, count=int(n)) for e, n in top],
        )

    @app.get("/features", response_model=FeaturesResponse)
    async def features(
        request: Request,
        entity: str = Query(max_length=64),
        minutes: int = Query(default=60, ge=1, le=7 * 24 * 60),
    ) -> FeaturesResponse:
        """One entity's closed windows with the control band the detector judged them against."""
        rows = await aquery(
            request.app.state.con,
            "SELECT window_start, window_end, count, rate_per_min, ewma, mean, std, zscore, "
            "is_anomaly FROM features_rolling "
            "WHERE entity = ? AND window_start >= now() - (? * INTERVAL '1 minute') "
            "ORDER BY window_start",
            [entity, minutes],
        )
        points = []
        for start, end, count, rate, ewma, mean, std, zscore, flagged in rows:
            upper = lower = None
            if ewma is not None and std is not None:
                spread = max(std, cfg.min_std)
                upper = ewma + cfg.band_k * spread
                lower = max(0.0, ewma - cfg.band_k * spread)
            points.append(
                FeaturePoint(
                    window_start=start,
                    window_end=end,
                    count=count,
                    rate_per_min=rate,
                    ewma=ewma,
                    mean=mean,
                    std=std,
                    zscore=zscore,
                    upper=upper,
                    lower=lower,
                    is_anomaly=flagged,
                )
            )
        return FeaturesResponse(entity=entity, minutes=minutes, k=cfg.band_k, points=points)

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
