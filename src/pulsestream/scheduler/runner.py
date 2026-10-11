"""APScheduler wiring: interval jobs on the asyncio loop, bodies in worker threads.

``AsyncIOScheduler`` runs plain functions through the loop's default executor, so
the job bodies (DuckDB and Parquet work) never block the event loop that serves
the API and reads the stream. ``max_instances=1`` with ``coalesce=True`` means a
slow compaction is never run twice at once and missed ticks collapse into one.
"""

from __future__ import annotations

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from pulsestream.scheduler.jobs import Maintenance


def build_scheduler(
    maintenance: Maintenance,
    *,
    aggregate_interval_seconds: int = 60,
    compaction_interval_seconds: int = 600,
) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")
    intervals = {
        "refresh_aggregates": aggregate_interval_seconds,
        "compact_lake": compaction_interval_seconds,
    }
    for name, seconds in intervals.items():
        scheduler.add_job(
            maintenance.run,
            IntervalTrigger(seconds=seconds),
            args=[name],
            id=name,
            name=name,
            max_instances=1,
            coalesce=True,
        )
    return scheduler
