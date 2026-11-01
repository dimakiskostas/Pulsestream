"""Micro-batches from the queue go to the bus, with retries that never drop.

The source → queue half is the Phase 1 producer, unchanged: a full queue blocks
the source, which is still the backpressure. ``Publisher`` is the handler the
shared batching loop drives on the ingest side. While the broker is away the
batch in hand is retried with backoff, the queue fills behind it and the source
blocks; when the broker is back everything flows again, in order.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import structlog

from pulsestream.bus.base import EventBus
from pulsestream.common import metrics
from pulsestream.common.backoff import delay_for
from pulsestream.models import RawEvent

log = structlog.get_logger(__name__)

SleepFn = Callable[[float], Awaitable[None]]


class Publisher:
    def __init__(
        self,
        bus: EventBus,
        *,
        max_attempts: int | None = None,
        sleep: SleepFn = asyncio.sleep,
    ) -> None:
        """``max_attempts=None`` retries forever (a service); a number gives up with the
        broker's error after that many consecutive failures (tests, one-off tools)."""
        self._bus = bus
        self._max_attempts = max_attempts
        self._sleep = sleep
        self.published_total = 0
        self.batches = 0
        self.failures = 0  # publish attempts that failed since start
        self.last_published_at: datetime | None = None

    async def publish(self, batch: list[RawEvent]) -> None:
        attempt = 0
        while True:
            try:
                ids = await self._bus.publish(batch)
            except Exception as exc:
                attempt += 1
                self.failures += 1
                metrics.ERRORS.labels(stage="bus").inc()
                if self._max_attempts is not None and attempt > self._max_attempts:
                    raise
                delay = delay_for(attempt - 1)
                log.warning(
                    "publish.retry",
                    attempt=attempt,
                    delay=round(delay, 2),
                    events=len(batch),
                    reason=f"{type(exc).__name__}: {exc}",
                )
                await self._sleep(delay)
            else:
                self.published_total += len(ids)
                self.batches += 1
                self.last_published_at = datetime.now(UTC)
                log.debug("batch.published", events=len(ids), first_id=ids[0] if ids else None)
                return
