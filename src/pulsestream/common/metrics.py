"""Prometheus metrics for the pipeline, exposed at ``GET /metrics``.

Process-wide singletons, as prometheus_client expects: the pipeline increments
them wherever the work happens, and the API renders the registry on request.
Label combinations that matter are initialised at import so every series exists
(at zero) from the first scrape; a dashboard then shows "0 errors" rather than
"no data".
"""

from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

INGEST_EVENTS = Counter(
    "pulsestream_ingest_events_total",
    "Events by stage: raw as received, kept after validation",
    ["stage"],
)
ERRORS = Counter("pulsestream_errors_total", "Errors by stage", ["stage"])
PROCESSING_LAG = Gauge(
    "pulsestream_processing_lag_seconds",
    "Seconds between the newest processed event's own timestamp and the time it was processed",
)
QUEUE_DEPTH = Gauge("pulsestream_queue_depth", "Raw events waiting in the in-process queue")
PROCESSING_DURATION = Histogram(
    "pulsestream_processing_duration_seconds",
    "Wall time to process one micro-batch (lake, validation, DuckDB, detection)",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
ANOMALIES = Counter(
    "pulsestream_anomalies_detected_total",
    "Findings by metric and severity",
    ["metric", "severity"],
)
SOURCE_RECONNECTS = Counter(
    "pulsestream_source_reconnects_total", "Reconnects to the event source after a failure"
)
JOB_RUNS = Counter(
    "pulsestream_job_runs_total", "Maintenance job runs by outcome", ["job", "outcome"]
)
BUS_PUBLISHED = Counter(
    "pulsestream_bus_published_total", "Events appended to the bus by the ingest service"
)
BUS_REDELIVERIES = Counter(
    "pulsestream_bus_redeliveries_total",
    "Events delivered a second time: re-read after a restart or claimed from a dead consumer",
)
BUS_STREAM_LENGTH = Gauge("pulsestream_bus_stream_length", "Entries retained in the bus stream")
BUS_PENDING = Gauge(
    "pulsestream_bus_pending", "Events delivered to the processing group and not yet acknowledged"
)
BUS_LAG = Gauge("pulsestream_bus_lag", "Entries the processing group has not been handed yet")

for _stage in ("raw", "kept"):
    INGEST_EVENTS.labels(stage=_stage)
for _stage in ("source", "processing", "job", "bus"):
    ERRORS.labels(stage=_stage)
for _metric in ("edit_rate", "byte_delta"):
    for _severity in ("warn", "high", "critical"):
        ANOMALIES.labels(metric=_metric, severity=_severity)
for _job in ("refresh_aggregates", "compact_lake"):
    for _outcome in ("ok", "failed"):
        JOB_RUNS.labels(job=_job, outcome=_outcome)


def render() -> tuple[bytes, str]:
    """The registry in Prometheus text exposition format, with its content type."""
    return generate_latest(), CONTENT_TYPE_LATEST
