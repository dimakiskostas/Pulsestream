"""The pandera gates pass on real data and fail loudly on the mistakes they exist to catch."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pandera.polars as pa
import polars as pl
import pytest

from pulsestream.cli import main
from pulsestream.models import RawEvent
from pulsestream.pipeline import run_consumer, run_producer
from pulsestream.processing.processor import StreamProcessor
from pulsestream.quality import AGG_MINUTELY, RAW_LAKE, WIKI_EVENTS, check, validate_lake
from pulsestream.sources.fake import FakeSource, load_fixture
from pulsestream.storage.aggregates import refresh_agg_minutely
from pulsestream.storage.duck import connect
from pulsestream.storage.parquet import ParquetRawStore
from tests.test_integration_pipeline import EXPECTED_KEPT, FIXTURE


async def populate(con: duckdb.DuckDBPyConnection, lake: Path) -> ParquetRawStore:
    """Fixture -> lake + wiki_events + agg_minutely, exactly as the live pipeline would."""
    store = ParquetRawStore(lake)
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    await run_producer(FakeSource(load_fixture(FIXTURE)), queue)
    await queue.put(None)
    await run_consumer(queue, StreamProcessor(con, store), batch_size=50, flush_interval=60.0)
    refresh_agg_minutely(con, window_minutes=10**6)
    return store


async def test_fixture_data_passes_every_gate(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    store = await populate(con, tmp_path / "raw")

    report = check(con, store)

    assert report.rows["wiki_events"] == EXPECTED_KEPT and report.rows["raw_lake"] == 500
    assert report.rows["agg_minutely"] >= 1  # one row per (minute, wiki): 45 wikis in the sample


def test_failure_cases_name_the_offending_columns(con: duckdb.DuckDBPyConnection) -> None:
    now = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)
    bad = pl.DataFrame(
        {
            "event_id": ["not-a-uuid"],
            "entity": ["enwiki"],
            "event_ts": [now],
            "byte_delta": [1],
            "is_bot": [False],
            "received_at": [now - timedelta(hours=1)],  # received before it happened
            "namespace": [0],
            "title": ["T"],
            "event_type": ["log"],  # filtered type leaked through
        },
        schema={
            "event_id": pl.Utf8,
            "entity": pl.Utf8,
            "event_ts": pl.Datetime("us", "UTC"),
            "byte_delta": pl.Int32,
            "is_bot": pl.Boolean,
            "received_at": pl.Datetime("us", "UTC"),
            "namespace": pl.Int32,
            "title": pl.Utf8,
            "event_type": pl.Utf8,
        },
    )

    with pytest.raises(pa.errors.SchemaErrors) as exc:
        WIKI_EVENTS.validate(bad, lazy=True)

    checks = set(exc.value.failure_cases.select("column", "check").unique().iter_rows())
    assert ("event_id", "str_length(36, 36)") in checks
    assert ("event_type", "isin(['edit', 'new'])") in checks
    assert (
        "wiki_events",
        "received_after_event",
    ) in checks  # frame-level checks carry the schema name


def test_aggregate_gate_catches_misaligned_or_inconsistent_buckets() -> None:
    frame = pl.DataFrame(
        {
            "bucket_ts": [datetime(2026, 10, 11, 10, 0, 30, tzinfo=UTC)],  # not minute-aligned
            "entity": ["enwiki"],
            "count": [2],
            "bot_count": [3],  # more bots than events
            "avg_value": [None],
            "max_value": [None],
            "anomaly_count": [0],
        },
        schema={
            "bucket_ts": pl.Datetime("us", "UTC"),
            "entity": pl.Utf8,
            "count": pl.Int32,
            "bot_count": pl.Int32,
            "avg_value": pl.Float64,
            "max_value": pl.Int32,
            "anomaly_count": pl.Int32,
        },
    )
    with pytest.raises(pa.errors.SchemaErrors) as exc:
        AGG_MINUTELY.validate(frame, lazy=True)
    names = set(exc.value.failure_cases["check"].to_list())
    assert {"bots_within_count", "minute_aligned"} <= names


def test_lake_gate_rejects_a_foreign_schema_version(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    store.write_batch(
        [RawEvent(source="t", received_at=datetime.now(UTC), schema_version=2, raw_payload="{}")]
    )
    with pytest.raises(pa.errors.SchemaErrors):
        validate_lake(store)
    assert validate_lake(ParquetRawStore(tmp_path / "empty")) == 0
    assert RAW_LAKE.name == "raw_lake"


async def test_check_cli_reports_and_exits_zero(tmp_path: Path, capsys) -> None:
    con = connect(tmp_path / "q.duckdb")
    await populate(con, tmp_path / "raw")
    con.close()

    code = main(["check", "--db", str(tmp_path / "q.duckdb"), "--lake", str(tmp_path / "raw")])

    out = capsys.readouterr().out
    assert code == 0
    assert "wiki_events: 306 rows ok" in out and "raw_lake: 500 rows ok" in out


def test_check_cli_exits_one_on_violations(tmp_path: Path, capsys) -> None:
    con = connect(tmp_path / "bad.duckdb")
    con.execute(
        "INSERT INTO wiki_events VALUES ('x', 'enwiki', now(), 1, false, now(), 0, 'T', 'log')"
    )
    con.close()

    code = main(["check", "--db", str(tmp_path / "bad.duckdb"), "--no-lake"])

    assert code == 1
    assert "FAILED" in capsys.readouterr().out
