"""The detection stage: validated events in, feature snapshots and findings out.

Each micro-batch is fed in event-time order. Every edit goes to the byte-delta
detector; every edit also advances the tumbling counter, and each window the
counter closes goes to the rate detector. Nothing here touches storage: the
processor persists what comes back, and a replay drives the same stage with
the same results. ``detected_at`` is left to the detectors (the window end for
rates, the edit's receipt for sizes), so findings are reproducible from the
data alone.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pulsestream.config import Settings
from pulsestream.detection.bytes import ByteDeltaDetector
from pulsestream.detection.rate import EwmaBandDetector
from pulsestream.models import Anomaly, FeatureSnapshot, WikiEvent
from pulsestream.processing.windows import TumblingCounter


@dataclass
class DetectionResult:
    snapshots: list[FeatureSnapshot] = field(default_factory=list)
    anomalies: list[Anomaly] = field(default_factory=list)


class DetectionStage:
    def __init__(
        self,
        counter: TumblingCounter,
        rate: EwmaBandDetector,
        sizes: ByteDeltaDetector | None = None,
    ) -> None:
        self._counter = counter
        self._rate = rate
        self._sizes = sizes
        self.windows_total = 0
        self.anomalies_total = 0

    @classmethod
    def from_settings(cls, cfg: Settings) -> DetectionStage:
        counter = TumblingCounter(
            window_seconds=cfg.window_seconds,
            lateness_seconds=cfg.lateness_seconds,
            max_gap_windows=cfg.max_gap_windows,
        )
        rate = EwmaBandDetector(
            alpha=cfg.ewma_alpha,
            k=cfg.band_k,
            warmup=cfg.warmup_windows,
            std_window=cfg.std_window,
            min_std=cfg.min_std,
            min_abs=cfg.min_abs_deviation,
            detect_drops=cfg.detect_drops,
        )
        sizes = (
            ByteDeltaDetector(
                window=cfg.byte_delta_window,
                warmup=cfg.byte_delta_warmup,
                threshold=cfg.byte_delta_threshold,
                min_bytes=cfg.byte_delta_min_bytes,
            )
            if cfg.byte_delta_enabled
            else None
        )
        return cls(counter, rate, sizes)

    @property
    def late_events(self) -> int:
        return self._counter.late_events

    def ingest(self, events: list[WikiEvent]) -> DetectionResult:
        """Feed one validated micro-batch; return what it closed and what it flagged."""
        result = DetectionResult()
        for event in sorted(events, key=lambda e: e.event_ts):
            if self._sizes is not None:
                finding = self._sizes.observe(event)
                if finding is not None:
                    result.anomalies.append(finding)
            for window in self._counter.observe(event.entity, event.event_ts):
                snapshot, finding = self._rate.observe(window)
                result.snapshots.append(snapshot)
                if finding is not None:
                    result.anomalies.append(finding)
        self._account(result)
        return result

    def flush(self) -> DetectionResult:
        """Close every open window (end of a replay). Not used live: open windows are partial."""
        result = DetectionResult()
        for window in self._counter.flush():
            snapshot, finding = self._rate.observe(window)
            result.snapshots.append(snapshot)
            if finding is not None:
                result.anomalies.append(finding)
        self._account(result)
        return result

    def _account(self, result: DetectionResult) -> None:
        self.windows_total += len(result.snapshots)
        self.anomalies_total += len(result.anomalies)
