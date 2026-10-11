from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from pulsestream.models import RawEvent
from pulsestream.storage.duck import (
    connect,
    ensure_lake_view,
    init_db,
    lake_row_stats,
    write_batch,
)
from pulsestream.storage.parquet import ParquetRawStore
from tests.factories import create_WikiEvent


def test_write_batch(con: duckdb.DuckDBPyConnection):
    write_batch(con, [create_WikiEvent()])
    rows = con.execute("SELECT * FROM wiki_events").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "e49b3bf0-04d9-4104-877f-64cbc2e5e9d9"
    assert rows[0][1] == "enwiki"
    assert rows[0][2] == datetime.fromtimestamp(1786622924, tz=UTC)


def test_dupe_write(con: duckdb.DuckDBPyConnection):
    write_batch(con, [create_WikiEvent()])
    write_batch(con, [create_WikiEvent()])
    assert con.execute("SELECT COUNT(*) FROM wiki_events").fetchall()[0][0] == 1


def test_empty_batch_is_a_no_op(con: duckdb.DuckDBPyConnection):
    write_batch(con, [])  # DuckDB would raise on an empty executemany
    assert con.execute("SELECT COUNT(*) FROM wiki_events").fetchall()[0][0] == 0


def test_init_db(con: duckdb.DuckDBPyConnection):
    """Protects from schema drift"""
    rows = con.execute("DESCRIBE wiki_events").fetchall()
    assert len(rows) == 9
    assert rows[0][0] == "event_id"
    assert rows[1][0] == "entity"
    assert rows[2][0] == "event_ts"
    assert rows[3][0] == "byte_delta"
    assert rows[4][0] == "is_bot"
    assert rows[5][0] == "received_at"
    assert rows[6][0] == "namespace"
    assert rows[7][0] == "title"
    assert rows[8][0] == "event_type"


def test_init_db_is_idempotent(con: duckdb.DuckDBPyConnection):
    write_batch(con, [create_WikiEvent()])
    init_db(con)  # CREATE TABLE IF NOT EXISTS: re-running must keep the data
    assert con.execute("SELECT COUNT(*) FROM wiki_events").fetchall()[0][0] == 1


def test_column_ordering(con: duckdb.DuckDBPyConnection):
    event = create_WikiEvent(title="Value 1", entity="Value 2")
    write_batch(con, [event])
    rows = con.execute("SELECT wiki_events.title, wiki_events.entity FROM wiki_events").fetchall()
    assert rows[0] == ("Value 1", "Value 2")


def test_timestamps_come_back_in_utc(con: duckdb.DuckDBPyConnection):
    """The session is pinned to UTC, so TIMESTAMPTZ values round-trip as UTC datetimes."""
    ts = datetime(2026, 10, 11, 9, 18, tzinfo=UTC)
    write_batch(con, [create_WikiEvent(event_ts=ts)])
    (got,) = con.execute("SELECT event_ts FROM wiki_events").fetchone() or (None,)
    assert got == ts
    assert got.utcoffset() == timedelta(0)
    assert con.execute("SELECT current_setting('TimeZone')").fetchone() == ("UTC",)


def test_lake_view_exposes_parquet_rows_and_partition_columns(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    store = ParquetRawStore(tmp_path / "raw")
    ts = datetime(2026, 10, 11, 10, 30, tzinfo=UTC)
    store.write_batch([RawEvent(source="t", received_at=ts, schema_version=1, raw_payload="{}")])

    ensure_lake_view(con, store.glob)

    assert con.execute("SELECT count(*), min(dt), min(hour) FROM raw_events").fetchone() == (
        1,
        ts.date(),
        10,
    )
    assert lake_row_stats(con, store.glob) == (1, ts, ts)
    ensure_lake_view(con, store.glob)  # CREATE OR REPLACE: calling it again is harmless


def test_connect_creates_a_file_database_and_reopens_read_only(tmp_path: Path):
    path = tmp_path / "events.duckdb"
    con = connect(path)
    write_batch(con, [create_WikiEvent()])
    con.close()

    ro = connect(path, read_only=True)
    assert ro.execute("SELECT COUNT(*) FROM wiki_events").fetchone() == (1,)
    assert ro.execute("SELECT current_setting('TimeZone')").fetchone() == ("UTC",)
    ro.close()
