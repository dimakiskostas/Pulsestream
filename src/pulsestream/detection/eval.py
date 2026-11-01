"""Evaluate the rate detector with synthetic anomalies, the only ground truth we have.

The stream carries no labels. The honest alternative is to inject known spikes
into a realistic baseline and measure how many the detector catches (recall) and
how often it fires elsewhere (precision). ``synthetic_series`` draws a Poisson
baseline with a slow diurnal swing, ``inject`` multiplies chosen windows, and
``evaluate`` runs a detector over the result and compares flagged windows with the
injected ones. ``pulsestream eval`` prints the numbers for a few configurations,
including the consecutive-spikes case that a non-winsorised baseline misses.
"""

from __future__ import annotations

import math
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial

from pulsestream.detection.rate import EwmaBandDetector
from pulsestream.processing.windows import ClosedWindow

START = datetime(2026, 1, 1, tzinfo=UTC)
MINUTES_PER_DAY = 1440


@dataclass(frozen=True)
class Evaluation:
    injected: tuple[int, ...]
    flagged: tuple[int, ...]

    @property
    def tp(self) -> int:
        return len(set(self.injected) & set(self.flagged))

    @property
    def fp(self) -> int:
        return len(set(self.flagged) - set(self.injected))

    @property
    def fn(self) -> int:
        return len(set(self.injected) - set(self.flagged))

    @property
    def precision(self) -> float:
        return self.tp / (self.tp + self.fp) if self.tp + self.fp else 0.0

    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if self.tp + self.fn else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if p + r else 0.0


def poisson(rng: random.Random, rate: float) -> int:
    """Knuth's method; fine for the rates this project sees (up to a few hundred)."""
    limit, k, p = math.exp(-rate), 0, 1.0
    while p > limit:
        k += 1
        p *= rng.random()
    return k - 1


def synthetic_series(
    n: int, *, baseline: float = 20.0, swing: float = 0.3, seed: int = 7
) -> list[int]:
    """``n`` per-minute counts: Poisson around a baseline with a daily sinusoidal swing."""
    rng = random.Random(seed)
    return [
        poisson(rng, baseline * (1.0 + swing * math.sin(2 * math.pi * i / MINUTES_PER_DAY)))
        for i in range(n)
    ]


def inject(series: list[int], positions: list[int], factor: float = 4.0) -> list[int]:
    """Multiply the counts at ``positions`` by ``factor``; the rest stays as it is."""
    out = list(series)
    for i in positions:
        out[i] = round(out[i] * factor)
    return out


def inject_level(series: list[int], positions: list[int], level: int) -> list[int]:
    """Set the counts at ``positions`` to one fixed ``level``: a run of equal spikes."""
    out = list(series)
    for i in positions:
        out[i] = level
    return out


def spike_positions(n: int, count: int, *, warmup: int = 10, seed: int = 11) -> list[int]:
    """``count`` positions after warm-up, at least 20 windows apart, chosen deterministically."""
    rng = random.Random(seed)
    candidates = list(range(warmup + 20, n - 1))
    chosen: list[int] = []
    while candidates and len(chosen) < count:
        pick = rng.choice(candidates)
        chosen.append(pick)
        candidates = [c for c in candidates if abs(c - pick) >= 20]
    return sorted(chosen)


def evaluate(
    series: list[int],
    injected: list[int],
    *,
    detector_factory: Callable[[], EwmaBandDetector] = EwmaBandDetector,
    width_seconds: int = 60,
    entity: str = "synthetic",
) -> Evaluation:
    """Run a fresh detector over the series; flag spikes (positive score) only."""
    detector = detector_factory()
    width = timedelta(seconds=width_seconds)
    flagged: list[int] = []
    for i, count in enumerate(series):
        window = ClosedWindow(entity, START + i * width, START + (i + 1) * width, count)
        _, anomaly = detector.observe(window)
        if anomaly is not None and anomaly.score > 0:
            flagged.append(i)
    return Evaluation(tuple(injected), tuple(flagged))


@dataclass(frozen=True)
class Scenario:
    name: str
    evaluation: Evaluation


def standard_scenarios(
    windows: int = 600, spikes: int = 12, factor: float = 4.0, baseline: float = 20.0
) -> list[Scenario]:
    """The table `pulsestream eval` prints: a few k values and the consecutive-spikes case."""
    base = synthetic_series(windows, baseline=baseline)
    positions = spike_positions(windows, spikes)
    series = inject(base, positions, factor)
    scenarios = [
        Scenario(
            f"{spikes} isolated spikes x{factor:g}, k={k:g}",
            evaluate(series, positions, detector_factory=partial(EwmaBandDetector, k=k)),
        )
        for k in (2.5, 3.0, 4.0)
    ]
    # Four equal spikes in a row, at 3.5x the baseline level: with a plain EWMA update
    # each spike lifts the baseline and widens the band, so the later ones go unseen.
    run = [positions[0] + i for i in range(4)]
    consecutive = inject_level(base, run, round(3.5 * baseline))
    scenarios.append(
        Scenario(
            "4 consecutive spikes x3.5, winsorised baseline",
            evaluate(consecutive, run, detector_factory=lambda: EwmaBandDetector(winsorize=True)),
        )
    )
    scenarios.append(
        Scenario(
            "4 consecutive spikes x3.5, plain baseline",
            evaluate(consecutive, run, detector_factory=lambda: EwmaBandDetector(winsorize=False)),
        )
    )
    return scenarios


def format_table(scenarios: list[Scenario]) -> str:
    header = (
        f"{'scenario':<44} {'precision':>9} {'recall':>7} {'f1':>6} {'tp':>4} {'fp':>4} {'fn':>4}"
    )
    lines = [header, "-" * len(header)]
    for s in scenarios:
        e = s.evaluation
        lines.append(
            f"{s.name:<44} {e.precision:>9.2f} {e.recall:>7.2f} {e.f1:>6.2f} "
            f"{e.tp:>4} {e.fp:>4} {e.fn:>4}"
        )
    return "\n".join(lines)
