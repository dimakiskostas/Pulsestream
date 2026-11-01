"""An ``EventSource`` over the bus: processing reads the stream, not the live source.

The pipeline keeps its shape when the broker arrives (the promise of ADR 0002):
the consumer still takes ``RawEvent`` objects from a queue. What changes is where
the producer gets them, and that every batch is acknowledged after it has been
written. ``BusSource`` remembers the broker id of each event it handed over
until ``ack`` is called with those events; an acknowledgement that fails is kept
and retried with the next one, because an unacknowledged entry costs a
redelivery while a dead consumer costs the pipeline (ADR 0017).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from uuid import UUID

import structlog

from pulsestream.bus.base import EventBus
from pulsestream.common import metrics
from pulsestream.models import RawEvent

log = structlog.get_logger(__name__)


class BusSource:
    def __init__(self, bus: EventBus, group: str, consumer: str) -> None:
        self._bus = bus
        self.group = group
        self.consumer = consumer
        self._inflight: dict[UUID, str] = {}  # handed over, not yet acknowledged
        self._unacked: list[str] = []  # acknowledgements that failed, retried next time
        self.delivered_total = 0
        self.acked_total = 0

    @property
    def connected(self) -> bool:
        return bool(getattr(self._bus, "connected", True))

    @property
    def inflight(self) -> int:
        return len(self._inflight) + len(self._unacked)

    async def stream(self) -> AsyncIterator[RawEvent]:
        async for delivery in self._bus.subscribe(self.group, self.consumer):
            self._inflight[delivery.event.ingest_id] = delivery.message_id
            self.delivered_total += 1
            yield delivery.event

    async def ack(self, events: Sequence[RawEvent]) -> int:
        """Acknowledge the events of a processed batch; returns how many ids were sent."""
        ids = self._unacked + [
            self._inflight.pop(e.ingest_id) for e in events if e.ingest_id in self._inflight
        ]
        self._unacked = []
        if not ids:
            return 0
        try:
            await self._bus.ack(self.group, ids)
        except Exception as exc:
            self._unacked = ids
            metrics.ERRORS.labels(stage="bus").inc()
            log.warning("bus.ack_failed", entries=len(ids), reason=f"{type(exc).__name__}: {exc}")
            return 0
        self.acked_total += len(ids)
        return len(ids)
