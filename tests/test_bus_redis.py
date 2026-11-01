"""The Redis Streams bus on fakeredis: ordering, acks, crash recovery, replay, retention."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import fakeredis.aioredis
from redis import exceptions as redis_exceptions

from pulsestream.bus.base import Delivery
from pulsestream.bus.redis import RedisStreamsBus, id_at, id_before, next_id
from pulsestream.common import metrics
from pulsestream.models import RawEvent
from tests.factories import _raw, create_payload


def raws(n: int, start: int = 0) -> list[RawEvent]:
    return [_raw(create_payload(meta={"id": f"m-{start + i}"})) for i in range(n)]


async def take(it: AsyncIterator[Delivery], n: int) -> list[Delivery]:
    out: list[Delivery] = []
    async for item in it:
        out.append(item)
        if len(out) >= n:
            break
    return out


async def no_sleep(_: float) -> None:
    return None


def make_bus(**kw: Any) -> RedisStreamsBus:
    kw.setdefault("client", fakeredis.aioredis.FakeRedis(decode_responses=True))
    kw.setdefault("block_ms", 10)
    kw.setdefault("sleep", no_sleep)
    return RedisStreamsBus(**kw)


def test_entry_ids_follow_time() -> None:
    moment = datetime(2026, 11, 1, 13, 0, tzinfo=UTC)
    assert id_at(moment) == "1793538000000-0"
    assert id_before(moment) == "1793537999999-18446744073709551615"
    assert next_id("1793538000000-7") == "1793538000000-8"


async def test_publish_then_replay_keeps_order() -> None:
    bus = make_bus()
    events = raws(5)
    ids = await bus.publish(events)
    assert len(ids) == 5 and ids == sorted(ids, key=lambda i: tuple(map(int, i.split("-"))))
    replayed = [d async for d in bus.replay()]
    assert [d.event for d in replayed] == events
    assert [d.message_id for d in replayed] == ids
    assert await bus.publish([]) == []


async def test_subscribe_delivers_each_event_once_and_ack_clears_pending() -> None:
    bus = make_bus()
    events = raws(4)
    await bus.publish(events)
    got = await take(bus.subscribe("processors", "c1"), 4)
    assert [d.event for d in got] == events
    before = await bus.stats("processors")
    assert before.length == 4 and before.pending == 4 and before.lag == 0
    assert await bus.ack("processors", [d.message_id for d in got]) == 4
    after = await bus.stats("processors")
    assert after.pending == 0 and after.first_id == got[0].message_id
    assert await bus.ack("processors", []) == 0


async def test_restart_with_the_same_consumer_name_redelivers_what_was_not_acked() -> None:
    bus = make_bus(count=1)  # one entry per read, so exactly three are delivered before the crash
    events = raws(5)
    await bus.publish(events)
    first_life = bus.subscribe("processors", "api-1")
    got = await take(first_life, 3)
    await first_life.aclose()  # the process dies with two entries pending
    await bus.ack("processors", [got[0].message_id])

    redelivered = metrics.BUS_REDELIVERIES._value.get()
    second_life = await take(bus.subscribe("processors", "api-1"), 4)
    assert [d.event for d in second_life] == events[1:], "pending first, then the new entries"
    assert metrics.BUS_REDELIVERIES._value.get() - redelivered == 2


async def test_a_new_consumer_claims_entries_a_dead_one_left_idle() -> None:
    bus = make_bus(claim_idle=timedelta(0), claim_every=timedelta(0))
    events = raws(3)
    await bus.publish(events)
    dead = bus.subscribe("processors", "old-host")
    await take(dead, 3)
    await dead.aclose()
    await asyncio.sleep(0.01)

    got = await take(bus.subscribe("processors", "new-host"), 3)
    assert [d.event for d in got] == events
    assert (await bus.stats("processors")).pending == 3, "claimed, still unacknowledged"


async def test_a_live_consumer_does_not_claim_its_own_entries() -> None:
    bus = make_bus(claim_idle=timedelta(0), claim_every=timedelta(0))
    await bus.publish(raws(2))
    stream = bus.subscribe("processors", "only")
    first = await take(stream, 2)
    # Nothing was acknowledged; the next read must not hand the same two back as "stale".
    more = raws(1, start=10)
    await bus.publish(more)
    nxt = await take(stream, 1)
    assert nxt[0].event == more[0] and nxt[0].message_id not in {d.message_id for d in first}


async def test_replay_window_is_inclusive_since_exclusive_until() -> None:
    bus = make_bus()
    early = raws(2)
    await bus.publish(early)
    await asyncio.sleep(0.002)
    cut = datetime.now(UTC)
    await asyncio.sleep(0.002)
    late = raws(3, start=5)
    await bus.publish(late)
    assert [d.event for d in [x async for x in bus.replay(since=cut)]] == late
    assert [d.event for d in [x async for x in bus.replay(until=cut)]] == early
    assert [x async for x in bus.replay(since=cut, until=cut)] == []


async def test_retention_trims_entries_older_than_the_window() -> None:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bus = make_bus(client=client, retention=timedelta(hours=1), exact_trim=True)
    stale_ms = int((time.time() - 7200) * 1000)
    await client.xadd(bus.stream, {"ingest_id": "stale"}, id=f"{stale_ms}-0")
    await bus.publish(raws(2))
    stats = await bus.stats("processors")
    assert stats.length == 2 and stats.first_id is not None
    assert int(stats.first_id.split("-")[0]) > stale_ms


async def test_undecodable_entries_are_acknowledged_and_skipped() -> None:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bus = make_bus(client=client)
    await client.xadd(bus.stream, {"garbage": "yes"})
    events = raws(1)
    await bus.publish(events)
    got = await take(bus.subscribe("processors", "c1"), 1)
    assert got[0].event == events[0]
    assert (await bus.stats("processors")).pending == 1, "the garbage entry is not pending"


class Flaky:
    """A client whose first ``n`` group reads fail with a connection error."""

    def __init__(self, inner: Any, failures: int) -> None:
        self._inner = inner
        self.failures = failures

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
        if self.failures:
            self.failures -= 1
            raise redis_exceptions.ConnectionError("broker away")
        return await self._inner.xreadgroup(*args, **kwargs)

    async def ping(self) -> bool:
        raise redis_exceptions.ConnectionError("broker away")


async def test_subscribe_reconnects_with_backoff_and_reports_connected() -> None:
    client = Flaky(fakeredis.aioredis.FakeRedis(decode_responses=True), failures=2)
    waits: list[float] = []

    async def record(seconds: float) -> None:
        waits.append(seconds)

    bus = make_bus(client=client, sleep=record)  # type: ignore[arg-type]
    errors = metrics.ERRORS.labels(stage="bus")._value.get()
    events = raws(1)
    await bus.publish(events)
    got = await take(bus.subscribe("processors", "c1"), 1)
    assert got[0].event == events[0] and bus.connected
    assert len(waits) == 2 and 0.5 <= waits[0] < 1.0 <= waits[1] < 2.0
    assert metrics.ERRORS.labels(stage="bus")._value.get() - errors == 2
    assert await bus.ping() is False and bus.connected is False


async def test_stats_on_an_empty_stream() -> None:
    bus = make_bus()
    stats = await bus.stats("processors")
    assert stats.length == 0 and stats.first_id is None and stats.pending == 0 and stats.lag is None
    assert await bus.ping() is True
    await bus.close()
