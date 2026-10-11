import asyncio
from collections.abc import Iterator

import duckdb
import pytest

import pulsestream.pipeline as pipeline
from pulsestream.models import RawEvent, WikiEvent
from pulsestream.pipeline import run_consumer, should_flush
from tests.factories import _raw, create_payload


def test_should_flush():
    assert should_flush(5, 2.0, 1.0)
    assert not should_flush(0, 2.0, 1.0)
    assert should_flush(5, 1.0, 1.0)
    assert not should_flush(5, 1.0, 2.0)
    assert not should_flush(0, 2.0, 2.0)
    assert not should_flush(0, 1.0, 2.0)


def make_raws(n: int) -> list[RawEvent]:
    """``n`` valid edits with distinct event ids."""
    return [_raw(create_payload(meta={"id": f"evt-{i:04d}"})) for i in range(n)]


@pytest.fixture
def written(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[list[str]]]:
    """Record every batch handed to ``write_batch`` (as lists of event ids)."""
    calls: list[list[str]] = []

    def fake_write_batch(con: duckdb.DuckDBPyConnection, batch: list[WikiEvent]) -> None:
        calls.append([e.event_id for e in batch])

    monkeypatch.setattr(pipeline, "write_batch", fake_write_batch)
    yield calls


async def test_consumer_flushes_full_batches_exactly_once(
    con: duckdb.DuckDBPyConnection, written: list[list[str]]
) -> None:
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    for raw in make_raws(12):
        await queue.put(raw)
    await queue.put(None)

    await run_consumer(queue, con, batch_size=5, flush_interval=60.0)

    assert [len(b) for b in written] == [5, 5, 2]
    flat = [eid for b in written for eid in b]
    assert len(flat) == len(set(flat)) == 12, "a flushed batch must never be re-written"


async def test_consumer_flushes_on_time_when_batch_is_not_full(
    con: duckdb.DuckDBPyConnection, written: list[list[str]]
) -> None:
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    for raw in make_raws(3):
        await queue.put(raw)
    task = asyncio.create_task(run_consumer(queue, con, batch_size=100, flush_interval=0.05))

    await asyncio.sleep(0.3)
    assert written == [[f"evt-{i:04d}" for i in range(3)]]

    await queue.put(None)
    await asyncio.wait_for(task, timeout=2)
    assert written == [[f"evt-{i:04d}" for i in range(3)]], "no empty flush on shutdown"


async def test_consumer_never_writes_empty_batches(
    con: duckdb.DuckDBPyConnection, written: list[list[str]]
) -> None:
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    task = asyncio.create_task(run_consumer(queue, con, batch_size=10, flush_interval=0.02))
    await asyncio.sleep(0.15)  # several flush intervals pass with nothing to write
    await queue.put(None)
    await asyncio.wait_for(task, timeout=2)
    assert written == []


async def test_consumer_drops_unparseable_and_filtered_events(
    con: duckdb.DuckDBPyConnection, written: list[list[str]]
) -> None:
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    await queue.put(_raw(create_payload(meta={"id": "keep-1"})))
    await queue.put(_raw(create_payload(meta={"id": "drop-log"}, type="log")))
    await queue.put(
        RawEvent(
            source="test",
            received_at=make_raws(1)[0].received_at,
            schema_version=1,
            raw_payload="not json",
        )
    )
    await queue.put(_raw(create_payload(meta={"id": "keep-2"})))
    await queue.put(None)

    await run_consumer(queue, con, batch_size=50, flush_interval=60.0)

    assert written == [["keep-1", "keep-2"]]
