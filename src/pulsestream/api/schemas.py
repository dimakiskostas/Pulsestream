"""Pydantic response models: the API contract that OpenAPI documents and tests assert."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

TaskState = Literal["running", "done", "failed"]
Bucket = Literal["10s", "1m", "5m", "1h"]


class PipelineStatus(BaseModel):
    producer: TaskState
    consumer: TaskState
    queue_depth: int


class HealthStatus(BaseModel):
    status: Literal["ok", "degraded"]
    pipeline: PipelineStatus


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
