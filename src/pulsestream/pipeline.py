"""Producer/consumer pipeline over an in-process ``asyncio.Queue``.

The producer pulls ``RawEvent`` objects from an ``EventSource`` and puts them on a
bounded queue. A blocking ``put`` on a full queue is the backpressure mechanism: a
slow consumer slows the producer down instead of growing memory without limit.

The consumer validates raw events into ``WikiEvent`` records and writes them to
DuckDB in micro-batches. A batch is flushed when it reaches
``batch_size`` *or* when ``flush_interval`` seconds have passed since the last
flush, whichever comes first, so a quiet stream still lands within seconds.
``None`` on the queue is a sentinel: flush whatever is buffered and stop.
"""

import asyncio
from time import monotonic

import duckdb

from pulsestream.models import RawEvent, WikiEvent
from pulsestream.sources.base import EventSource
from pulsestream.storage.duck import write_batch


async def run_producer(source: EventSource, queue: asyncio.Queue[RawEvent | None]) -> None:
    async for raw in source.stream():
        await queue.put(raw)  # blocks when the queue is full == backpressure


def should_flush(
    batch_len: int, sec_since_latest_flush: float, flush_interval: float = 2.0
) -> bool:
    """Time-based flush rule: a non-empty batch that has waited at least ``flush_interval``."""
    return batch_len > 0 and sec_since_latest_flush >= flush_interval


async def run_consumer(
    queue: asyncio.Queue[RawEvent | None],
    con: duckdb.DuckDBPyConnection,
    batch_size: int = 50,
    flush_interval: float = 2.0,
) -> None:
    batch: list[WikiEvent] = []
    last_flush = monotonic()

    def flush() -> None:
        nonlocal last_flush
        if batch:  # never issue an empty write
            write_batch(con, batch)
            batch.clear()  # a flushed batch must not be written again
        last_flush = monotonic()

    while True:
        remaining = flush_interval - (monotonic() - last_flush)
        try:
            raw = await asyncio.wait_for(queue.get(), timeout=max(0.0, remaining))
        except TimeoutError:
            flush()
            continue

        try:
            if raw is None:  # sentinel -> flush & stop (used by tests and shutdown)
                flush()
                return
            event = WikiEvent.from_raw(raw)
            if event is not None:
                batch.append(event)
            if len(batch) >= batch_size or should_flush(
                len(batch), monotonic() - last_flush, flush_interval
            ):
                flush()
        finally:
            queue.task_done()
