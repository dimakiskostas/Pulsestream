"""What ingestion and processing agree on when a broker sits between them.

``EventBus`` is the one shape both sides depend on: ingestion publishes ``RawEvent``
envelopes, processing subscribes as a consumer group and acknowledges each event
once the batch it belongs to has been written, and a replay reads a time range.
The wire format is the envelope's five fields as strings, so any broker that
carries string fields (Redis Streams here, a Kafka-compatible one later) moves
it unchanged.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from pulsestream.models import RawEvent

FIELDS = ("ingest_id", "source", "received_at", "schema_version", "raw_payload")


def encode(event: RawEvent) -> dict[str, str]:
    """The envelope as string fields; ``received_at`` travels as ISO-8601 with its offset."""
    return {
        "ingest_id": str(event.ingest_id),
        "source": event.source,
        "received_at": event.received_at.isoformat(),
        "schema_version": str(event.schema_version),
        "raw_payload": event.raw_payload,
    }


def decode(fields: Mapping[str, str]) -> RawEvent:
    """The inverse of ``encode``; a missing field is a ``KeyError`` (a foreign message)."""
    return RawEvent(
        ingest_id=UUID(fields["ingest_id"]),
        source=fields["source"],
        received_at=datetime.fromisoformat(fields["received_at"]),
        schema_version=int(fields["schema_version"]),
        raw_payload=fields["raw_payload"],
    )


@dataclass(frozen=True)
class Delivery:
    """One event as the bus handed it over; ``message_id`` is what gets acknowledged."""

    message_id: str
    event: RawEvent


@dataclass(frozen=True)
class BusStats:
    """What the stream and one consumer group look like right now."""

    length: int  # entries retained in the stream
    first_id: str | None  # oldest retained entry (its id carries the publish time)
    last_id: str | None
    pending: int  # delivered to the group and not yet acknowledged
    lag: int | None  # entries the group has not been handed yet; None if the broker can't say


class EventBus(Protocol):
    async def publish(self, events: Sequence[RawEvent]) -> list[str]:
        """Append events in order; returns their broker ids."""
        ...

    def subscribe(self, group: str, consumer: str) -> AsyncIterator[Delivery]:
        """Deliver events to this consumer of the group, oldest unacknowledged first, forever."""
        ...

    async def ack(self, group: str, message_ids: Sequence[str]) -> int:
        """Acknowledge delivered events; returns how many were still pending."""
        ...

    def replay(
        self, *, since: datetime | None = None, until: datetime | None = None
    ) -> AsyncIterator[Delivery]:
        """Read ``[since, until)`` by publish time, outside any group, without side effects."""
        ...

    async def stats(self, group: str) -> BusStats: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...
