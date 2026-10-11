"""Pure feature math: EWMA, rolling mean/std, MAD-based z-scores (HLD §7, §12).

Everything here is a small, deterministic function or a tiny state object with no
I/O, so the statistics the detectors rely on can be pinned by unit tests with known
numbers. Statistics first, models later (HLD decision D6).
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import dataclass, field

# 0.6745 is Phi^-1(0.75): it scales the MAD so that, for normal data, the modified
# z-score is comparable to an ordinary z-score (Iglewicz & Hoaglin, 1993).
MAD_SCALE = 0.6745


def ewma_update(previous: float | None, value: float, alpha: float) -> float:
    """Exponentially weighted moving average step; the first observation seeds it."""
    if not 0.0 < alpha <= 1.0:
        raise ValueError("alpha must be in (0, 1]")
    return value if previous is None else alpha * value + (1.0 - alpha) * previous


@dataclass
class RollingStats:
    """Mean and sample standard deviation over the last ``window`` values."""

    window: int
    _values: deque[float] = field(default_factory=deque, repr=False)

    def __post_init__(self) -> None:
        if self.window < 2:
            raise ValueError("window must be at least 2")
        self._values = deque(self._values, maxlen=self.window)

    def push(self, value: float) -> None:
        self._values.append(value)

    def __len__(self) -> int:
        return len(self._values)

    @property
    def full(self) -> bool:
        return len(self._values) == self.window

    @property
    def mean(self) -> float | None:
        return statistics.fmean(self._values) if self._values else None

    @property
    def std(self) -> float | None:
        """Sample standard deviation; ``None`` with fewer than two values."""
        if len(self._values) < 2:
            return None
        return statistics.stdev(self._values)

    def values(self) -> list[float]:
        return list(self._values)


def median_abs_deviation(values: list[float]) -> tuple[float, float]:
    """Return (median, MAD) of ``values``; MAD is the median of absolute deviations."""
    if not values:
        raise ValueError("need at least one value")
    med = statistics.median(values)
    mad = statistics.median(abs(v - med) for v in values)
    return med, mad


def modified_zscore(value: float, median: float, mad: float) -> float | None:
    """Iglewicz-Hoaglin modified z-score; ``None`` when the MAD is zero (no spread)."""
    if mad <= 0.0:
        return None
    return MAD_SCALE * (value - median) / mad


def zscore(value: float, mean: float, std: float) -> float | None:
    """Ordinary z-score; ``None`` when the standard deviation is zero."""
    if std <= 0.0 or math.isnan(std):
        return None
    return (value - mean) / std


def clip(value: float, lower: float, upper: float) -> float:
    """Winsorise: pull ``value`` inside [lower, upper]."""
    return max(lower, min(upper, value))
