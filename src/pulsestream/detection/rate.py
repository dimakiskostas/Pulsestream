"""Edit-rate anomalies: a window count outside EWMA ± k·std of the entity's own history.

Per entity the detector keeps an EWMA of the per-window count and a rolling sample
standard deviation of recent counts. For every closed window it compares the count
with the baseline *as it was before that window*, then updates the baseline. Four
guards keep it honest on this stream:

* warm-up: no verdict until ``warmup`` windows have been seen for the entity;
* a floor on the standard deviation and a minimum absolute deviation, so a wiki
  that averages one edit a minute cannot raise a "critical" on four edits;
* a winsorised update: a flagged count is clipped to the band before it enters the
  baseline, so one spike does not widen the band enough to hide the next one;
* a discontinuity (the counter forgot the entity and it reappeared) resets the
  entity's state, so stale statistics never judge fresh data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from pulsestream.detection.features import RollingStats, clip, ewma_update
from pulsestream.models import Anomaly, FeatureSnapshot, Method, Metric, anomaly_id, severity_for
from pulsestream.processing.windows import ClosedWindow

METHOD: Method = "ewma_band"
METRIC: Metric = "edit_rate"


@dataclass
class _State:
    stats: RollingStats
    ewma: float | None = None
    seen: int = 0
    last_start: datetime | None = None
    anomalies: int = field(default=0)


class EwmaBandDetector:
    def __init__(
        self,
        *,
        alpha: float = 0.3,
        k: float = 3.0,
        warmup: int = 10,
        std_window: int = 30,
        min_std: float = 1.0,
        min_abs: float = 5.0,
        detect_drops: bool = True,
        winsorize: bool = True,
    ) -> None:
        if warmup < 2 or k <= 0 or min_std <= 0 or min_abs < 0:
            raise ValueError("warmup >= 2, k > 0, min_std > 0, min_abs >= 0")
        self._alpha = alpha
        self._k = k
        self._warmup = warmup
        self._std_window = std_window
        self._min_std = min_std
        self._min_abs = min_abs
        self._detect_drops = detect_drops
        self._winsorize = winsorize
        self._states: dict[str, _State] = {}

    def _state_for(self, window: ClosedWindow) -> _State:
        width = window.window_end - window.window_start
        state = self._states.get(window.entity)
        if (
            state is None
            or state.last_start is not None
            and window.window_start != state.last_start + width
        ):
            state = _State(stats=RollingStats(window=self._std_window))
            self._states[window.entity] = state
        return state

    def observe(
        self, window: ClosedWindow, *, detected_at: datetime | None = None
    ) -> tuple[FeatureSnapshot, Anomaly | None]:
        """Score one closed window against the entity's baseline, then update the baseline."""
        state = self._state_for(window)
        width_seconds = (window.window_end - window.window_start).total_seconds()
        value = float(window.count)
        ewma, mean, std = state.ewma, state.stats.mean, state.stats.std

        zscore: float | None = None
        anomaly: Anomaly | None = None
        update = value
        if state.seen >= self._warmup and ewma is not None and std is not None:
            spread = max(std, self._min_std)
            zscore = (value - ewma) / spread
            upper, lower = ewma + self._k * spread, ewma - self._k * spread
            spike = value > upper and value - ewma >= self._min_abs
            drop = self._detect_drops and value < lower and ewma - value >= self._min_abs
            if spike or drop:
                anomaly = Anomaly(
                    anomaly_id=anomaly_id(
                        METHOD, window.entity, METRIC, window.window_start.isoformat()
                    ),
                    detected_at=detected_at if detected_at is not None else window.window_end,
                    entity=window.entity,
                    metric=METRIC,
                    value=value,
                    expected=ewma,
                    score=zscore,
                    method=METHOD,
                    severity=severity_for(zscore),
                    window_start=window.window_start,
                    window_end=window.window_end,
                    details=(
                        f"count {value:g} outside [{lower:.1f}, {upper:.1f}] "
                        f"(ewma {ewma:.1f}, std {spread:.1f}, k {self._k:g})"
                    ),
                )
                state.anomalies += 1
                if self._winsorize:
                    update = clip(value, lower, upper)  # keep the band honest

        state.ewma = ewma_update(state.ewma, update, self._alpha)
        state.stats.push(update)
        state.seen += 1
        state.last_start = window.window_start

        snapshot = FeatureSnapshot(
            entity=window.entity,
            window_start=window.window_start,
            window_end=window.window_end,
            count=window.count,
            rate_per_min=value * 60.0 / width_seconds,
            ewma=ewma,
            mean=mean,
            std=std,
            zscore=zscore,
            is_anomaly=anomaly is not None,
        )
        return snapshot, anomaly

    def entities(self) -> int:
        return len(self._states)
