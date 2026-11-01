"""Rebuild a database from the lake: ``pulsestream replay``.

Why this exists: a changed filter, a new derived table, a corrupted DuckDB file,
or simply "what did the stream look like between 10:00 and 10:30?" are all a
replay, never a re-ingest (ADR 0008, 0009). The replay uses the live pipeline's
consumer and processor; only the source differs, and the raw store is switched
off so the lake is not written a second time.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pulsestream.config import Settings
from pulsestream.config import settings as default_settings
from pulsestream.models import RawEvent
from pulsestream.pipeline import run_consumer, run_producer
from pulsestream.processing.detection import DetectionStage
from pulsestream.processing.processor import StreamProcessor
from pulsestream.sources.lake import ParquetSource
from pulsestream.storage.aggregates import refresh_agg_minutely
from pulsestream.storage.duck import connect


@dataclass(frozen=True)
class ReplayReport:
    raw: int  # raw events read from the lake
    kept: int  # events that passed validation
    rows: int  # rows in wiki_events afterwards (idempotent: replaying twice gives the same)
    buckets: int  # agg_minutely buckets refreshed
    anomalies: int = 0  # findings raised while replaying


async def replay(
    lake_root: str | Path,
    db_path: str | Path,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    batch_size: int = 500,
    cfg: Settings | None = None,
) -> ReplayReport:
    cfg = cfg if cfg is not None else default_settings
    con = connect(db_path)
    try:
        detection = DetectionStage.from_settings(cfg) if cfg.detection_enabled else None
        processor = StreamProcessor(con, None, detection)  # no raw store: a replay reads the lake
        queue: asyncio.Queue[RawEvent | None] = asyncio.Queue(maxsize=batch_size * 4)
        source = ParquetSource(lake_root, since=since, until=until)
        producer = asyncio.create_task(run_producer(source, queue))
        consumer = asyncio.create_task(run_consumer(queue, processor, batch_size, 60.0))
        await producer
        await queue.put(None)
        await consumer
        processor.finish()  # the data is complete: close the last windows too

        # Aggregate everything that was replayed, however old it is.
        row = con.execute("SELECT min(event_ts), count(*) FROM wiki_events").fetchone()
        earliest, rows = (row[0], int(row[1])) if row is not None else (None, 0)
        buckets = 0
        if earliest is not None:
            minutes = int((datetime.now(UTC) - earliest).total_seconds() // 60) + 2
            buckets = refresh_agg_minutely(con, window_minutes=minutes)
        return ReplayReport(
            raw=processor.raw_total,
            kept=processor.kept_total,
            rows=rows,
            buckets=buckets,
            anomalies=processor.anomalies_total,
        )
    finally:
        con.close()
