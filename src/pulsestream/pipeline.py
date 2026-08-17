import asyncio
from asyncio import wait_for
from datetime import time
from time import monotonic

import duckdb

from pulsestream.models import RawEvent, WikiEvent
from pulsestream.sources.base import EventSource
from pulsestream.storage.duck import write_batch


async def run_producer(source: EventSource, queue: asyncio.Queue[RawEvent | None]) -> None:
    async for raw in source.stream():
        await queue.put(raw)


def should_flush(batnch_len, sec_since_latest_flush, flush_interval: float = 2.0) -> bool:
    if batnch_len <= 0:
        return False
    if batnch_len > 0 and sec_since_latest_flush >= flush_interval:
        return True


async def run_consumer(
    queue: asyncio.Queue[RawEvent | None],
    con: duckdb.DuckDBPyConnection,
    batch_size: int = 50,
    flush_interval: float = 2.0,
) -> None:
    batch: list[WikiEvent] = []
    last_flush = monotonic()
    while True:
        try:
            remaining = flush_interval - (monotonic() - last_flush)
            raw = await wait_for(queue.get(), timeout=max(0, remaining))
            if raw is None:
                if batch:
                    write_batch(con, batch)
                    batch.clear()
                    last_flush = time.monotonic()
                queue.task_done()
                return
            event = WikiEvent.from_raw(raw)
            if event is not None:
                batch.append(event)
            if len(batch) >= batch_size:
                write_batch(con, batch)
                last_flush = time.monotonic()
            queue.task_done()

            if should_flush(len(batch), monotonic() - last_flush, flush_interval):
                write_batch(con, batch)
                batch.clear()
                last_flush = time.monotonic()
        except TimeoutError:
            write_batch(con, batch)
            batch.clear()
            last_flush = time.monotonic()
            continue
