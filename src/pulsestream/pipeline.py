"""Producer/consumer pipeline over an in-process ``asyncio.Queue``.

The producer pulls ``RawEvent`` objects from an ``EventSource`` into a bounded queue;
a full queue blocks ``put``, which is the backpressure mechanism.

The consumer owns *when*: it collects raw events into micro-batches and hands each
batch to a ``Processor``, which owns *what* happens to a batch (validation and
storage now; the raw lake, rolling features and anomaly detection in later phases).
Flush rules (ADR 0004): the batch reached ``batch_size``, or ``flush_interval``
seconds passed since the last flush and the batch is non-empty. ``None`` on the
queue is the sentinel: flush whatever is buffered and stop.
"""

import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Protocol

from pulsestream.models import RawEvent
from pulsestream.sources.base import EventSource


@dataclass(frozen=True)
class BatchResult:
    """What a processor did with one micro-batch."""

    raw: int  # raw events received
    kept: int  # events that passed validation (not necessarily new rows: writes are idempotent)


class Processor(Protocol):
    def process(self, raws: list[RawEvent]) -> BatchResult:
        """Handle one micro-batch. Synchronous; the consumer decides where it runs."""
        ...


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
    processor: Processor,
    batch_size: int = 50,
    flush_interval: float = 2.0,
) -> None:
    batch: list[RawEvent] = []
    last_flush = monotonic()

    def flush() -> None:
        nonlocal last_flush
        if batch:  # never hand over an empty batch
            processor.process(batch)
            batch.clear()  # a flushed batch must not be processed again
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
            batch.append(raw)
            if len(batch) >= batch_size or should_flush(
                len(batch), monotonic() - last_flush, flush_interval
            ):
                flush()
        finally:
            queue.task_done()
