"""Redis Streams implementation of the ``EventBus`` (ADR 0016).

One stream holds every envelope in publish order. Processing reads it through a
consumer group, so each entry is handed to one consumer and stays *pending* until
that consumer acknowledges it. An entry whose consumer died is re-read when a
consumer of the same name comes back, or claimed from the dead consumer once it
has been idle for ``claim_idle``; that is how at-least-once delivery survives a
crash (ADR 0017). Entry ids are millisecond timestamps assigned by the server, so
a time window is an ``XRANGE`` and retention is a ``MINID`` trim on every append.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any

import structlog
from redis import exceptions as redis_exceptions
from redis.asyncio import Redis

from pulsestream.bus.base import BusStats, Delivery, decode, encode
from pulsestream.common import metrics
from pulsestream.common.backoff import delay_for
from pulsestream.models import RawEvent

log = structlog.get_logger(__name__)

SleepFn = Callable[[float], Awaitable[None]]
MAX_SEQ = 18446744073709551615  # the largest sequence number an entry id can carry
TRANSIENT = (redis_exceptions.ConnectionError, redis_exceptions.TimeoutError)


def id_at(moment: datetime) -> str:
    """The smallest entry id a message published at ``moment`` can have."""
    return f"{int(moment.timestamp() * 1000)}-0"


def id_before(moment: datetime) -> str:
    """The largest entry id published strictly before ``moment``."""
    return f"{int(moment.timestamp() * 1000) - 1}-{MAX_SEQ}"


def next_id(message_id: str) -> str:
    ms, seq = message_id.split("-")
    return f"{ms}-{int(seq) + 1}"


class RedisStreamsBus:
    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        stream: str = "pulsestream:events",
        retention: timedelta = timedelta(hours=6),
        count: int = 100,
        block_ms: int = 1000,
        claim_idle: timedelta = timedelta(seconds=60),
        claim_every: timedelta = timedelta(seconds=5),
        exact_trim: bool = False,
        client: Redis | None = None,
        sleep: SleepFn = asyncio.sleep,
    ) -> None:
        """
        ``retention`` is how far back the stream keeps entries (trimmed on publish);
        ``count``/``block_ms`` shape each read; ``claim_idle`` is how long a delivered
        entry may sit unacknowledged with another consumer before this one takes it
        over, checked every ``claim_every``. ``client`` and ``sleep`` are injectable so
        tests run on fakeredis without waiting.
        """
        self._client = client if client is not None else Redis.from_url(url, decode_responses=True)
        self.stream = stream
        self._retention = retention
        self._count = count
        self._block_ms = block_ms
        self._claim_idle_ms = int(claim_idle.total_seconds() * 1000)
        self._claim_every = claim_every.total_seconds()
        self._approximate = not exact_trim
        self._sleep = sleep
        self.connected = False  # true after the last broker call succeeded

    # -- producing -------------------------------------------------------------

    async def publish(self, events: Sequence[RawEvent]) -> list[str]:
        if not events:
            return []
        minid = id_at(datetime.now(UTC) - self._retention)
        async with self._client.pipeline(transaction=False) as pipe:
            for event in events:
                fields: dict[Any, Any] = encode(event)
                pipe.xadd(self.stream, fields, minid=minid, approximate=self._approximate)
            ids = await pipe.execute()
        self.connected = True
        metrics.BUS_PUBLISHED.inc(len(ids))
        return [str(i) for i in ids]

    # -- consuming -------------------------------------------------------------

    async def ensure_group(self, group: str) -> None:
        """Create the consumer group at the start of the stream; a second call is a no-op."""
        try:
            await self._client.xgroup_create(self.stream, group, id="0", mkstream=True)
        except redis_exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def subscribe(self, group: str, consumer: str) -> AsyncIterator[Delivery]:
        """Pending entries from a previous life first, then stale entries of dead consumers
        every ``claim_every``, then new entries, forever; reconnects with backoff."""
        attempt = 0
        while True:
            try:
                await self.ensure_group(group)
                self.connected = True
                async for delivery in self._own_pending(group, consumer):
                    yield delivery
                last_claim = -1e9  # claim on the first pass
                while True:
                    if monotonic() - last_claim >= self._claim_every:
                        for delivery in await self._claim_stale(group, consumer):
                            yield delivery
                        last_claim = monotonic()
                    for delivery in await self._read(group, consumer, ">", block=True):
                        yield delivery
                    attempt = 0
            except TRANSIENT as exc:
                self.connected = False
                metrics.ERRORS.labels(stage="bus").inc()
                attempt += 1
                delay = delay_for(attempt - 1)
                log.warning(
                    "bus.reconnect",
                    attempt=attempt,
                    delay=round(delay, 2),
                    reason=f"{type(exc).__name__}: {exc}",
                )
                await self._sleep(delay)

    async def ack(self, group: str, message_ids: Sequence[str]) -> int:
        if not message_ids:
            return 0
        return int(await self._client.xack(self.stream, group, *message_ids))

    async def _own_pending(self, group: str, consumer: str) -> AsyncIterator[Delivery]:
        """Entries handed to this consumer name that were never acknowledged (a restart)."""
        cursor = "0"
        while True:
            deliveries = await self._read(group, consumer, cursor, block=False)
            if not deliveries:
                return
            metrics.BUS_REDELIVERIES.inc(len(deliveries))
            for delivery in deliveries:
                yield delivery
            # XREADGROUP with an explicit id returns pending entries *after* it (unlike
            # XRANGE), so the cursor is the last id itself, not the one past it.
            cursor = deliveries[-1].message_id

    async def _claim_stale(self, group: str, consumer: str) -> list[Delivery]:
        """Take over entries another consumer has left unacknowledged for ``claim_idle``."""
        stale = await self._client.xpending_range(
            self.stream, group, min="-", max="+", count=self._count, idle=self._claim_idle_ms
        )
        ids: list[Any] = [str(p["message_id"]) for p in stale if str(p["consumer"]) != consumer]
        if not ids:
            return []
        entries = await self._client.xclaim(
            self.stream, group, consumer, min_idle_time=self._claim_idle_ms, message_ids=ids
        )
        deliveries = await self._deliveries(group, entries)
        metrics.BUS_REDELIVERIES.inc(len(deliveries))
        log.warning("bus.claimed", entries=len(deliveries), from_consumers=len(stale))
        return deliveries

    async def _read(self, group: str, consumer: str, start: str, *, block: bool) -> list[Delivery]:
        # RESP2 shape: [[stream, [(id, fields), ...]]], or None when a blocking read times out.
        response: Any = await self._client.xreadgroup(
            group,
            consumer,
            {self.stream: start},
            count=self._count,
            block=self._block_ms if block else None,
        )
        entries = response[0][1] if response else []
        return await self._deliveries(group, entries)

    async def _deliveries(self, group: str | None, entries: Sequence[Any]) -> list[Delivery]:
        """Decode entries; one that is not an envelope is acknowledged and skipped, not retried."""
        deliveries = []
        for message_id, fields in entries:
            try:
                deliveries.append(Delivery(message_id=str(message_id), event=decode(fields)))
            except (KeyError, ValueError) as exc:
                metrics.ERRORS.labels(stage="bus").inc()
                log.error("bus.undecodable", message_id=str(message_id), reason=str(exc))
                if group is not None:
                    await self.ack(group, [str(message_id)])
        return deliveries

    # -- replay and introspection ---------------------------------------------

    async def replay(
        self, *, since: datetime | None = None, until: datetime | None = None
    ) -> AsyncIterator[Delivery]:
        start = id_at(since) if since is not None else "-"
        end = id_before(until) if until is not None else "+"
        while True:
            entries = await self._client.xrange(self.stream, min=start, max=end, count=self._count)
            if not entries:
                return
            for delivery in await self._deliveries(None, entries):
                yield delivery
            start = next_id(str(entries[-1][0]))

    async def stats(self, group: str) -> BusStats:
        length = int(await self._client.xlen(self.stream))
        first_id = last_id = None
        pending, lag = 0, None
        if length:
            info = await self._client.xinfo_stream(self.stream)
            first_id = str(info["first-entry"][0]) if info.get("first-entry") else None
            last_id = str(info["last-entry"][0]) if info.get("last-entry") else None
            for grp in await self._client.xinfo_groups(self.stream):
                if str(grp["name"]) == group:
                    pending = int(grp["pending"])
                    lag = int(grp["lag"]) if grp.get("lag") is not None else None
        metrics.BUS_STREAM_LENGTH.set(length)
        metrics.BUS_PENDING.set(pending)
        if lag is not None:
            metrics.BUS_LAG.set(lag)
        return BusStats(length=length, first_id=first_id, last_id=last_id, pending=pending, lag=lag)

    async def ping(self) -> bool:
        try:
            self.connected = bool(await self._client.ping())
        except TRANSIENT:
            self.connected = False
        return self.connected

    async def close(self) -> None:
        await self._client.aclose()
