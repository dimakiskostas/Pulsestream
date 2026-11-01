"""Processing reads the bus: the two services meet in the stream, acks follow the write,
a crash before the ack is a redelivery and never a duplicate row."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import fakeredis.aioredis
import httpx
from redis import exceptions as redis_exceptions

from pulsestream.api.app import create_app
from pulsestream.bus.redis import RedisStreamsBus
from pulsestream.config import Settings
from pulsestream.ingest.app import create_ingest_app
from pulsestream.models import RawEvent, WikiEvent
from pulsestream.pipeline import BatchResult, run_consumer, run_producer
from pulsestream.processing.processor import StreamProcessor
from pulsestream.sources.bus import BusSource
from pulsestream.sources.fake import FakeSource, load_fixture
from pulsestream.storage.duck import connect
from tests.factories import _raw, create_payload


def raws(n: int) -> list[RawEvent]:
    return [_raw(create_payload(meta={"id": f"bp-{k}"})) for k in range(n)]


async def no_sleep(_: float) -> None:
    return None


def make_bus(client: Any = None, **kw: Any) -> RedisStreamsBus:
    client = client if client is not None else fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisStreamsBus(client=client, block_ms=10, sleep=no_sleep, **kw)


def rows(con: Any) -> int:
    return int(con.execute("SELECT count(*) FROM wiki_events").fetchone()[0])


@asynccontextmanager
async def serving(app: Any) -> AsyncIterator[httpx.AsyncClient]:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            client.app_ref = app  # type: ignore[attr-defined]
            yield client


async def test_bus_source_hands_events_over_and_acks_what_was_processed() -> None:
    bus = make_bus()
    events = raws(4)
    await bus.publish(events)
    src = BusSource(bus, "processors", "api-1")
    got: list[RawEvent] = []
    async for raw in src.stream():
        got.append(raw)
        if len(got) == 4:
            break
    assert got == events and src.inflight == 4 and src.connected
    assert await src.ack(events[:2]) == 2
    assert src.inflight == 2 and (await bus.stats("processors")).pending == 2
    assert await src.ack(raws(1)) == 0, "events it never handed over are ignored"
    assert await src.ack(events[2:]) == 2 and (await bus.stats("processors")).pending == 0


async def test_a_failed_ack_is_retried_with_the_next_one() -> None:
    class FlakyAck:
        def __init__(self, inner: RedisStreamsBus) -> None:
            self._inner = inner
            self.fail_next = True

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

        async def ack(self, group: str, ids: list[str]) -> int:
            if self.fail_next:
                self.fail_next = False
                raise redis_exceptions.ConnectionError("broker away")
            return await self._inner.ack(group, ids)

    inner = make_bus()
    bus = FlakyAck(inner)
    events = raws(4)
    await inner.publish(events)
    src = BusSource(bus, "processors", "api-1")  # type: ignore[arg-type]
    got = []
    async for raw in src.stream():
        got.append(raw)
        if len(got) == 4:
            break
    assert await src.ack(events[:2]) == 0 and src.inflight == 4, "kept for the next attempt"
    assert await src.ack(events[2:]) == 4 and src.inflight == 0
    assert (await inner.stats("processors")).pending == 0


async def test_ingest_and_api_meet_in_the_bus(tmp_path: Path) -> None:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    events = load_fixture("tests/fixtures/sample_events.jsonl")
    kept = sum(1 for e in events if WikiEvent.from_raw(e) is not None)
    ingest_cfg = Settings(_env_file=None, bus="redis", batch_size=50, flush_interval=0.05)
    api_cfg = Settings(
        _env_file=None,
        bus="redis",
        bus_consumer="api-under-test",
        db_path=str(tmp_path / "bus.duckdb"),
        lake_root=str(tmp_path / "raw"),
        scheduler_enabled=False,
        batch_size=50,
        flush_interval=0.05,
    )
    ingest = create_ingest_app(ingest_cfg, source=FakeSource(events), bus=make_bus(client))
    api = create_app(api_cfg, bus=make_bus(client))
    async with serving(api) as api_client, serving(ingest) as ingest_client:
        for _ in range(600):
            total = (await api_client.get("/stats/latest")).json()["events_total"]
            if total >= kept:
                break
            await asyncio.sleep(0.02)
        assert total == kept
        for _ in range(100):  # the last batch's ack follows its write
            bus_status = (await api_client.get("/bus")).json()
            if bus_status["pending"] == 0:
                break
            await asyncio.sleep(0.02)
        assert bus_status["mode"] == "redis" and bus_status["consumer"] == "api-under-test"
        assert bus_status["length"] == len(events) and bus_status["pending"] == 0
        ready = await api_client.get("/health/ready")
        assert ready.status_code == 200 and ready.json()["checks"]["bus"] is True
        assert (await ingest_client.get("/stats")).json()["published_total"] == len(events)
    assert bus_status["lag"] == 0


async def test_a_crash_before_the_ack_redelivers_without_duplicate_rows(tmp_path: Path) -> None:
    bus = make_bus()
    events = raws(10)
    await bus.publish(events)
    con = connect(tmp_path / "crash.duckdb")

    class CrashesOnSecondBatch:
        def __init__(self) -> None:
            self.inner = StreamProcessor(con)
            self.calls = 0

        def process(self, batch: list[RawEvent]) -> BatchResult:
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("power cut")
            return self.inner.process(batch)

    async def run(processor: Any, consumer_name: str) -> tuple[BusSource, BaseException | None]:
        src = BusSource(bus, "processors", consumer_name)
        queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
        producer = asyncio.create_task(run_producer(src, queue))
        consumer = asyncio.create_task(
            run_consumer(queue, processor, batch_size=4, flush_interval=0.05, after_batch=src.ack)
        )
        for _ in range(200):
            if consumer.done() or (src.acked_total >= 10 and queue.empty()):
                break
            await asyncio.sleep(0.02)
        producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)
        if not consumer.done():
            await queue.put(None)
            await asyncio.wait_for(consumer, 5)
        return src, consumer.exception() if not consumer.cancelled() else None

    first, error = await run(CrashesOnSecondBatch(), "api-1")
    assert isinstance(error, RuntimeError) and rows(con) == 4
    assert first.acked_total == 4 and (await bus.stats("processors")).pending == 6

    second, error = await run(StreamProcessor(con), "api-1")
    assert error is None and rows(con) == 10, "redelivered, written once (PRIMARY KEY)"
    assert second.delivered_total == 6 and (await bus.stats("processors")).pending == 0
    con.close()
