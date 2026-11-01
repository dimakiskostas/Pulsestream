"""Keep the bus gauges fresh for Prometheus between requests."""

from __future__ import annotations

import asyncio

import structlog

from pulsestream.bus.base import EventBus

log = structlog.get_logger(__name__)


async def keep_stats_fresh(bus: EventBus, group: str, interval: float) -> None:
    """Refresh stream length, pending and lag every ``interval`` seconds, forever."""
    while True:
        try:
            await bus.stats(group)
        except Exception as exc:  # a broker that is away shows up in the probes already
            log.debug("bus.stats_failed", reason=f"{type(exc).__name__}: {exc}")
        await asyncio.sleep(interval)
