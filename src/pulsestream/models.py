import json
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid4, uuid5

from pydantic import BaseModel, Field

# Namespace for deterministic anomaly ids: the same finding on the same window or
# event always gets the same id, so a replay never duplicates an anomaly.
ANOMALY_NAMESPACE = UUID("6f1c2b4e-9d3a-4c5e-8f7a-2b1d0e9c8a77")

Metric = Literal["edit_rate", "byte_delta"]
Method = Literal["ewma_band", "mad_zscore"]
Severity = Literal["warn", "high", "critical"]


class RawEvent(BaseModel):
    """Source-agnostic envelope. Never changes between sources."""

    ingest_id: UUID = Field(default_factory=uuid4)
    source: str
    received_at: datetime
    schema_version: int
    raw_payload: str  # keep the original; parse later


class WikiEvent(BaseModel):
    """Validated, source-specific domain event (see docs/decisions/0001 for the filter)."""

    event_id: str
    entity: str
    event_ts: datetime
    byte_delta: int
    is_bot: bool
    received_at: datetime
    event_type: Literal["edit", "new"]
    title: str
    namespace: int

    @classmethod
    def from_raw(cls, raw: RawEvent) -> "WikiEvent | None":
        types = ["edit", "new"]
        try:
            data = json.loads(raw.raw_payload)
        except (TypeError, ValueError):
            return None

        if data.get("type") not in types:
            return None

        event_id = data.get("meta", {}).get("id")
        entity = data.get("wiki")
        timestamp_value = data.get("timestamp")

        if isinstance(timestamp_value, (int, float)):
            event_ts = datetime.fromtimestamp(timestamp_value, tz=UTC)
        else:
            return None

        length = data.get("length", {})
        byte_delta = length.get("new", 0) - length.get("old", 0)

        is_bot = data.get("bot")

        title = data.get("title")
        namespace = data.get("namespace")
        return cls(
            event_id=event_id,
            entity=str(entity),
            event_ts=event_ts,
            byte_delta=int(byte_delta),
            is_bot=bool(is_bot),
            received_at=raw.received_at,
            event_type=data.get("type"),
            title=title,
            namespace=namespace,
        )


class FeatureSnapshot(BaseModel):
    """Rolling features of one entity for one closed tumbling window."""

    entity: str
    window_start: datetime
    window_end: datetime
    count: int
    rate_per_min: float
    ewma: float | None  # baseline *before* this window; None while warming up
    mean: float | None
    std: float | None
    zscore: float | None
    is_anomaly: bool = False


class Anomaly(BaseModel):
    """One flagged observation. ``anomaly_id`` is deterministic, see ``anomaly_id()``."""

    anomaly_id: UUID
    detected_at: datetime
    entity: str
    metric: Metric
    value: float
    expected: float | None  # the baseline the value was compared against
    score: float  # signed z-like score; the sign is the direction
    method: Method
    severity: Severity
    window_start: datetime
    window_end: datetime
    event_id: str | None = None  # set for per-event (byte_delta) findings
    model_version: str | None = None  # None for the statistical detectors
    details: str | None = None


def anomaly_id(method: str, entity: str, metric: str, key: str) -> UUID:
    """Stable id for a finding: same method, entity, metric and key -> same UUID."""
    return uuid5(ANOMALY_NAMESPACE, f"{method}:{entity}:{metric}:{key}")


def severity_for(score: float) -> Severity:
    magnitude = abs(score)
    if magnitude >= 8.0:
        return "critical"
    if magnitude >= 5.0:
        return "high"
    return "warn"
