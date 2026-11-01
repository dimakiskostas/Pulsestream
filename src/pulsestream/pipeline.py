"""Producer/consumer pipeline over an in-process ``asyncio.Queue``.

The producer pulls ``RawEvent`` objects from an ``EventSource`` into a bounded queue;
a full queue blocks ``put``, which is the backpressure mechanism.

``run_batches`` owns *when*: it collects raw events into micro-batches and awaits a
handler for each. Flush rules (ADR 0004): the batch reached ``batch_size``, or
``flush_interval`` seconds passed since the last flush and the batch is non-empty.
``None`` on the queue is the sentinel: flush whatever is buffered and stop. Two
handlers use it: the consumer below, whose ``Processor`` owns *what* happens to a
batch (lake, validation, DuckDB, detection), and the ingest service's publisher,
which hands batches to the bus.

Processing is synchronous work (DuckDB, Parquet) and runs in a worker thread via
``asyncio.to_thread``, so a slow batch never stalls the event loop that serves the
API and reads the stream (tutorial Part 1.5 gotcha: don't block the loop).
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import monotonic
from typing import Protocol

from pulsestream.common import metrics
from pulsestream.models import RawEvent
from pulsestream.sources.base import EventSource

BatchHandler = Callable[[list[RawEvent]], Awaitable[object]]  # the result is ignored


@dataclass(frozen=True)
class BatchResult:
    """What a processor did with one micro-batch."""

    raw: int  # raw events received
    kept: int  # events that passed validation (not necessarily new rows: writes are idempotent)
    anomalies: int = 0  # findings raised by the detection stage for this batch


class Processor(Protocol):
    def process(self, raws: list[RawEvent]) -> BatchResult:
        """Handle one micro-batch. Synchronous; the consumer runs it on a worker thread."""
        ...


async def run_producer(source: EventSource, queue: asyncio.Queue[RawEvent | None]) -> None:
    async for raw in source.stream():
        await queue.put(raw)  # blocks when the queue is full == backpressure


def should_flush(
    batch_len: int, sec_since_latest_flush: float, flush_interval: float = 2.0
) -> bool:
    """Time-based flush rule: a non-empty batch that has waited at least ``flush_interval``."""
    return batch_len > 0 and sec_since_latest_flush >= flush_interval


async def run_batches(
    queue: asyncio.Queue[RawEvent | None],
    handle: BatchHandler,
    batch_size: int = 50,
    flush_interval: float = 2.0,
) -> None:
    """Collect raw events into micro-batches and await ``handle`` for each one."""
    batch: list[RawEvent] = []
    last_flush = monotonic()

    async def flush() -> None:
        nonlocal last_flush
        if batch:  # never hand over an empty batch
            pending = list(batch)
            batch.clear()  # a flushed batch must not be processed again
            await handle(pending)
        last_flush = monotonic()

    while True:
        remaining = flush_interval - (monotonic() - last_flush)
        try:
            raw = await asyncio.wait_for(queue.get(), timeout=max(0.0, remaining))
        except TimeoutError:
            await flush()
            continue
        metrics.QUEUE_DEPTH.set(queue.qsize())

        try:
            if raw is None:  # sentinel -> flush & stop (used by tests and shutdown)
                await flush()
                return
            batch.append(raw)
            if len(batch) >= batch_size or should_flush(
                len(batch), monotonic() - last_flush, flush_interval
            ):
                await flush()
        finally:
            queue.task_done()


async def run_consumer(
    queue: asyncio.Queue[RawEvent | None],
    processor: Processor,
    batch_size: int = 50,
    flush_interval: float = 2.0,
    *,
    after_batch: BatchHandler | None = None,
) -> None:
    """Drive a synchronous ``Processor`` on a worker thread, one micro-batch at a time.

    ``after_batch`` runs on the event loop once a batch has been processed; with a
    bus behind the queue that is where the batch's entries are acknowledged, so an
    entry is never acknowledged before its rows are written (ADR 0017).
    """

    async def handle(pending: list[RawEvent]) -> None:
        await asyncio.to_thread(processor.process, pending)
        if after_batch is not None:
            await after_batch(pending)

    await run_batches(queue, handle, batch_size, flush_interval)
