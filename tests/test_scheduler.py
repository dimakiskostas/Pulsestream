import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from pulsestream.models import RawEvent
from pulsestream.scheduler.jobs import JOB_NAMES, Maintenance
from pulsestream.scheduler.runner import build_scheduler
from pulsestream.storage.duck import write_batch
from pulsestream.storage.parquet import ParquetRawStore
from tests.factories import create_WikiEvent


def maintenance(
    con: duckdb.DuckDBPyConnection, store: ParquetRawStore | None = None
) -> Maintenance:
    return Maintenance(
        con,
        store,
        aggregate_window_minutes=180,
        compaction_grace_seconds=120,
        compaction_min_files=2,
    )


def test_refresh_job_writes_aggregates_through_its_own_cursor(
    con: duckdb.DuckDBPyConnection,
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    write_batch(
        con,
        [create_WikiEvent(event_id=f"e{i}", event_ts=now - timedelta(seconds=i)) for i in range(3)],
    )

    run = maintenance(con).run("refresh_aggregates")

    assert (run.runs, run.failures, run.last_error) == (1, 0, None)
    assert run.last_result is not None and run.last_result.endswith("buckets")
    assert run.last_duration_seconds is not None and run.last_started_at is not None
    assert con.execute("SELECT sum(count) FROM agg_minutely").fetchone() == (3,)


def test_compaction_job_reports_merges_and_tolerates_a_disabled_lake(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    assert maintenance(con).run("compact_lake").last_result == "lake disabled"

    store = ParquetRawStore(tmp_path / "raw")
    ts = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)
    for i in range(3):
        files = store.write_batch(
            [
                RawEvent(
                    source="t",
                    received_at=ts + timedelta(seconds=i),
                    schema_version=1,
                    raw_payload="{}",
                )
            ]
        )
        old = time.time() - 3600
        os.utime(files[0], (old, old))

    run = maintenance(con, store).run("compact_lake")

    assert run.last_result == "1 partitions, 3 files merged"
    assert len(store.files()) == 1


def test_a_failing_job_is_recorded_not_raised(con: duckdb.DuckDBPyConnection) -> None:
    class BrokenConnection:
        def cursor(self) -> None:
            raise RuntimeError("database on fire")

    m = Maintenance(BrokenConnection(), None)  # type: ignore[arg-type]

    run = m.run("refresh_aggregates")

    assert (run.runs, run.failures) == (1, 1)
    assert run.last_error == "RuntimeError: database on fire"
    assert m.run("refresh_aggregates").failures == 2, "the scheduler can keep calling it"


def test_build_scheduler_registers_both_jobs_with_their_intervals(
    con: duckdb.DuckDBPyConnection,
) -> None:
    scheduler = build_scheduler(
        maintenance(con), aggregate_interval_seconds=60, compaction_interval_seconds=600
    )
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert set(jobs) == set(JOB_NAMES)
    assert jobs["refresh_aggregates"].trigger.interval == timedelta(seconds=60)
    assert jobs["compact_lake"].trigger.interval == timedelta(seconds=600)
    assert all(job.max_instances == 1 and job.coalesce for job in jobs.values())
