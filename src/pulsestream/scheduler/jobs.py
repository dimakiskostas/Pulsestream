"""The job bodies and their run status.

Jobs execute in APScheduler's worker threads while the consumer writes from
another thread, and DuckDB wants one cursor per thread, so every job opens its
own cursor on the shared connection and closes it when done. ``Maintenance.run``
records timing and errors and never raises: a failing job is a fact to surface
on ``/jobs``, not a reason to stop the scheduler.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb
import structlog

from pulsestream.storage.aggregates import refresh_agg_minutely
from pulsestream.storage.parquet import ParquetRawStore

log = structlog.get_logger(__name__)

JOB_NAMES = ("refresh_aggregates", "compact_lake")


@dataclass
class JobRun:
    name: str
    runs: int = 0
    failures: int = 0
    last_started_at: datetime | None = None
    last_duration_seconds: float | None = None
    last_result: str | None = None
    last_error: str | None = None


class Maintenance:
    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        raw_store: ParquetRawStore | None,
        *,
        aggregate_window_minutes: int = 180,
        compaction_grace_seconds: float = 120.0,
        compaction_min_files: int = 2,
    ) -> None:
        self._con = con
        self._raw_store = raw_store
        self._window = aggregate_window_minutes
        self._grace = compaction_grace_seconds
        self._min_files = compaction_min_files
        self._jobs: dict[str, Callable[[], str]] = {
            "refresh_aggregates": self.refresh_aggregates,
            "compact_lake": self.compact_lake,
        }
        self.status: dict[str, JobRun] = {name: JobRun(name) for name in JOB_NAMES}

    def refresh_aggregates(self) -> str:
        cur = self._con.cursor()
        try:
            buckets = refresh_agg_minutely(cur, window_minutes=self._window)
        finally:
            cur.close()
        return f"{buckets} buckets"

    def compact_lake(self) -> str:
        if self._raw_store is None:
            return "lake disabled"
        results = self._raw_store.compact(min_files=self._min_files, grace_seconds=self._grace)
        merged = sum(r.merged for r in results)
        return f"{len(results)} partitions, {merged} files merged"

    def run(self, name: str) -> JobRun:
        """Run one job by name; record timing, result and error; never raise."""
        job = self.status[name]
        job.runs += 1
        job.last_started_at = datetime.now(UTC)
        started = time.monotonic()
        try:
            job.last_result = self._jobs[name]()
            job.last_error = None
            log.info("job.ok", job=name, result=job.last_result)
        except Exception as exc:
            job.failures += 1
            job.last_error = f"{type(exc).__name__}: {exc}"
            log.warning("job.failed", job=name, error=job.last_error)
        finally:
            job.last_duration_seconds = round(time.monotonic() - started, 3)
        return job
