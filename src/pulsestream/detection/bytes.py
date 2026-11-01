"""Byte-delta anomalies: an edit far larger than the entity's recent edits.

Per entity, the last ``window`` absolute byte deltas form the reference. A new
edit's |delta| is scored with the modified z-score (median and MAD), which a few
huge edits cannot drag around the way a mean and standard deviation can. Guards:
warm-up, no score when the MAD is zero (recent edits all the same size), and a
minimum size in bytes, because "far larger than this wiki's usual edit" is routine
on wikis whose usual edit is tiny. The defaults (50 000 bytes, modified z above 8)
were calibrated on a live run: with 1 000 bytes and 3.5 the detector raised 1 500
findings in 13 minutes, almost all ordinary file-page edits on Commons. Severity
follows the size: warn from the floor, high from 2.5x, critical from 5x.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta

from pulsestream.detection.features import median_abs_deviation, modified_zscore
from pulsestream.models import Anomaly, Method, Metric, Severity, WikiEvent, anomaly_id

METHOD: Method = "mad_zscore"
METRIC: Metric = "byte_delta"


def severity_for_size(magnitude: float, min_bytes: int) -> Severity:
    """Tiers by size relative to the floor: the floor is a warning, 2.5x high, 5x critical."""
    if magnitude >= 5 * min_bytes:
        return "critical"
    if magnitude >= 2.5 * min_bytes:
        return "high"
    return "warn"


class ByteDeltaDetector:
    def __init__(
        self,
        *,
        window: int = 200,
        warmup: int = 50,
        threshold: float = 8.0,
        min_bytes: int = 50_000,
    ) -> None:
        if window < 2 or warmup < 2 or warmup > window or threshold <= 0:
            raise ValueError("window >= warmup >= 2 and threshold > 0")
        self._window = window
        self._warmup = warmup
        self._threshold = threshold
        self._min_bytes = min_bytes
        self._history: dict[str, deque[float]] = {}

    def observe(self, event: WikiEvent, *, detected_at: datetime | None = None) -> Anomaly | None:
        """Score one edit against the entity's recent edit sizes, then add it to them."""
        history = self._history.get(event.entity)
        if history is None:
            history = self._history[event.entity] = deque(maxlen=self._window)
        magnitude = float(abs(event.byte_delta))
        anomaly: Anomaly | None = None
        if len(history) >= self._warmup and magnitude >= self._min_bytes:
            median, mad = median_abs_deviation(list(history))
            score = modified_zscore(magnitude, median, mad)
            if score is not None and score > self._threshold:
                signed = score if event.byte_delta >= 0 else -score
                minute = event.event_ts.replace(second=0, microsecond=0)
                anomaly = Anomaly(
                    anomaly_id=anomaly_id(METHOD, event.entity, METRIC, event.event_id),
                    detected_at=detected_at if detected_at is not None else event.received_at,
                    entity=event.entity,
                    metric=METRIC,
                    value=float(event.byte_delta),
                    expected=median,
                    score=signed,
                    method=METHOD,
                    severity=severity_for_size(magnitude, self._min_bytes),
                    window_start=minute,
                    window_end=minute + timedelta(minutes=1),
                    event_id=event.event_id,
                    details=(
                        f"|delta| {magnitude:g} bytes vs median {median:.0f}, MAD {mad:.0f} "
                        f"over the last {len(history)} edits ({event.title})"
                    ),
                )
        history.append(magnitude)
        return anomaly

    def entities(self) -> int:
        return len(self._history)
