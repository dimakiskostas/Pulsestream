"""The per-batch processing step: persist raw, validate, persist typed, detect.

``StreamProcessor`` is the ``Processor`` the consumer loop drives. Keeping it
separate from the loop means the loop only knows about batching and the queue,
while everything that happens to a batch (the raw lake, validation, DuckDB, the
rolling features and the anomaly detectors) lives here and is testable with a
plain list of events.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import duckdb
import structlog

from pulsestream.common import metrics
from pulsestream.models import RawEvent, WikiEvent
from pulsestream.pipeline import BatchResult
from pulsestream.processing.detection import DetectionResult, DetectionStage
from pulsestream.storage.duck import write_anomalies, write_batch, write_features

log = structlog.get_logger(__name__)


class RawStore(Protocol):
    def write_batch(self, raws: list[RawEvent]) -> list[Path]: ...


class StreamProcessor:
    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        raw_store: RawStore | None = None,
        detection: DetectionStage | None = None,
    ) -> None:
        self._con = con
        self._raw_store = raw_store
        self._detection = detection
        self.raw_total = 0  # running totals since start (replay reports, readiness)
        self.kept_total = 0
        self.anomalies_total = 0
        self.last_processed_at: datetime | None = None  # wall clock of the last batch
        self.last_event_ts: datetime | None = None  # newest event_ts seen so far

    def process(self, raws: list[RawEvent]) -> BatchResult:
        first = str(raws[0].ingest_id) if raws else None
        with (
            structlog.contextvars.bound_contextvars(ingest_id=first, batch=len(raws)),
            metrics.PROCESSING_DURATION.time(),
        ):
            try:
                return self._process(raws)
            except Exception:
                metrics.ERRORS.labels(stage="processing").inc()
                log.exception("batch.failed", raw=len(raws))
                raise

    def _process(self, raws: list[RawEvent]) -> BatchResult:
        if self._raw_store is not None:
            # Raw first, unfiltered: the lake holds exactly what arrived, so a changed
            # filter is a replay from disk rather than a re-ingest.
            self._raw_store.write_batch(raws)
        events = [e for e in (WikiEvent.from_raw(r) for r in raws) if e is not None]
        write_batch(self._con, events)
        found = 0
        if self._detection is not None and events:
            found = len(self._persist(self._detection.ingest(events)).anomalies)
        self.raw_total += len(raws)
        self.kept_total += len(events)
        metrics.INGEST_EVENTS.labels(stage="raw").inc(len(raws))
        metrics.INGEST_EVENTS.labels(stage="kept").inc(len(events))
        now = datetime.now(UTC)
        self.last_processed_at = now
        newest = max((e.event_ts for e in events), default=None)
        if newest is not None and (self.last_event_ts is None or newest > self.last_event_ts):
            self.last_event_ts = newest
        if self.last_event_ts is not None:
            metrics.PROCESSING_LAG.set(max((now - self.last_event_ts).total_seconds(), 0.0))
        log.debug("batch.processed", raw=len(raws), kept=len(events), anomalies=found)
        return BatchResult(raw=len(raws), kept=len(events), anomalies=found)

    def finish(self) -> DetectionResult:
        """Close the open windows and persist them: for the end of a replay, not for live use."""
        if self._detection is None:
            return DetectionResult()
        return self._persist(self._detection.flush())

    def _persist(self, result: DetectionResult) -> DetectionResult:
        write_features(self._con, result.snapshots)
        write_anomalies(self._con, result.anomalies)
        self.anomalies_total += len(result.anomalies)
        for anomaly in result.anomalies:
            metrics.ANOMALIES.labels(metric=anomaly.metric, severity=anomaly.severity).inc()
            log.info(
                "anomaly.detected",
                entity=anomaly.entity,
                metric=anomaly.metric,
                severity=anomaly.severity,
                value=anomaly.value,
                score=round(anomaly.score, 2),
                window_start=anomaly.window_start.isoformat(),
            )
        return result
