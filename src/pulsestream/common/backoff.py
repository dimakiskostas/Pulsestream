"""Exponential backoff with jitter, shared by everything that reconnects."""

from __future__ import annotations

import random


def base_delay_for(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    return float(min(cap, base * (2**attempt)))


def delay_for(
    attempt: int, base: float = 1.0, cap: float = 30.0, rng: random.Random | None = None
) -> float:
    """Exponential backoff with jitter: uniform in [d/2, d), d = min(cap, base * 2**attempt)."""
    d = base_delay_for(attempt, base, cap)
    jitter = rng.uniform(0, d / 2) if rng is not None else random.uniform(0, d / 2)
    return d / 2 + jitter
