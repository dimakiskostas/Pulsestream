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
