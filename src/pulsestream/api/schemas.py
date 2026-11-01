"""Pydantic response models: the API contract that OpenAPI documents and tests assert."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel

from pulsestream.models import Method, Metric, Severity

TaskState = Literal["running", "done", "failed"]
Bucket = Literal["10s", "1m", "5m", "1h"]


class PipelineStatus(BaseModel):
    producer: TaskState
    consumer: TaskState
    queue_depth: int


class HealthStatus(BaseModel):
    status: Literal["ok", "degraded"]
    pipeline: PipelineStatus


class LivenessStatus(BaseModel):
    status: Literal["alive"]


class ReadinessStatus(BaseModel):
    """Can this process do useful work right now? Each check is one reason it might not."""

    ready: bool
    checks: dict[str, bool]  # consumer, producer, store, source, fresh
    last_processed_at: datetime | None
    silence_seconds: float  # seconds since the last processed batch (or since start)


class BusStatus(BaseModel):
    """The stream between ingestion and processing, as this process sees it."""

    mode: Literal["none", "redis"]
    stream: str | None = None
    group: str | None = None
    consumer: str | None = None
    length: int = 0
    first_id: str | None = None
    last_id: str | None = None
    pending: int = 0  # delivered to the group, not yet acknowledged (any consumer)
    lag: int | None = None  # entries the group has not been handed yet
    inflight: int = 0  # handed to this process's consumer loop, not yet acknowledged
    delivered_total: int = 0
    acked_total: int = 0


class IngestStats(BaseModel):
    """The ingest service: what it pushed into the bus, and what the stream holds."""

    published_total: int
    batches: int
    publish_failures: int
    last_published_at: datetime | None
    queue_depth: int
    stream_length: int
    stream_first_id: str | None
    stream_last_id: str | None


class StatsLatest(BaseModel):
    events_total: int
    events_last_min: int
    latest_event_ts: datetime | None
    lag_seconds: float | None  # now - latest_event_ts; None until the first event lands


class TimeseriesPoint(BaseModel):
    time: datetime
    count: int


class TimeseriesResponse(BaseModel):
    bucket: Bucket
    minutes: int
    points: list[TimeseriesPoint] = []


class LakeStats(BaseModel):
    """The Parquet raw store as seen from the filesystem and from DuckDB."""

    enabled: bool
    root: str | None
    files: int
    partitions: int
    bytes: int
    rows: int
    first_received_at: datetime | None
    last_received_at: datetime | None


class AggPoint(BaseModel):
    bucket_ts: datetime
    entity: str  # "*" when all entities are combined
    count: int
    bot_count: int
    avg_value: float | None  # mean byte_delta (count-weighted when combined)
    max_value: int | None
    anomaly_count: int


class AggregatesResponse(BaseModel):
    minutes: int
    entity: str | None
    points: list[AggPoint] = []


class EntityCount(BaseModel):
    entity: str
    count: int
    bot_count: int


class TopEntitiesResponse(BaseModel):
    minutes: int
    entities: list[EntityCount] = []


class AnomalyOut(BaseModel):
    anomaly_id: UUID
    detected_at: datetime
    entity: str
    metric: Metric
    value: float
    expected: float | None
    score: float
    method: Method
    severity: Severity
    window_start: datetime
    window_end: datetime
    event_id: str | None
    model_version: str | None
    details: str | None

    @classmethod
    def from_row(cls, row: tuple[Any, ...]) -> "AnomalyOut":
        """Build from a SELECT in the anomalies table's column order."""
        names = (
            "anomaly_id",
            "detected_at",
            "entity",
            "metric",
            "value",
            "expected",
            "score",
            "method",
            "severity",
            "window_start",
            "window_end",
            "event_id",
            "model_version",
            "details",
        )
        return cls(**dict(zip(names, row, strict=True)))


class AnomaliesResponse(BaseModel):
    minutes: int
    entity: str | None
    metric: Metric | None
    severity: Severity | None
    anomalies: list[AnomalyOut] = []


class EntityAnomalies(BaseModel):
    entity: str
    count: int


class AnomalySummary(BaseModel):
    minutes: int
    total: int
    by_severity: dict[str, int]
    by_metric: dict[str, int]
    top_entities: list[EntityAnomalies] = []


class FeaturePoint(BaseModel):
    window_start: datetime
    window_end: datetime
    count: int
    rate_per_min: float
    ewma: float | None  # baseline before the window; None while warming up
    mean: float | None
    std: float | None
    zscore: float | None
    upper: float | None  # ewma + k * max(std, min_std): the band's top
    lower: float | None  # the band's floor, never below zero
    is_anomaly: bool


class FeaturesResponse(BaseModel):
    entity: str
    minutes: int
    k: float
    points: list[FeaturePoint] = []


class JobStatus(BaseModel):
    name: str
    runs: int
    failures: int
    last_started_at: datetime | None
    last_duration_seconds: float | None
    last_result: str | None
    last_error: str | None
    next_run_at: datetime | None  # None when the scheduler is disabled


class JobsResponse(BaseModel):
    scheduler_enabled: bool
    jobs: list[JobStatus]
