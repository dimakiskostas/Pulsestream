import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import polars as pl

from pulsestream.models import RawEvent
from pulsestream.storage.parquet import SCHEMA, ParquetRawStore


def raw_at(ts: datetime, n: int) -> RawEvent:
    return RawEvent(
        source="test", received_at=ts, schema_version=1, raw_payload=json.dumps({"n": n})
    )


def utc_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def test_write_batch_lands_one_file_in_the_hour_partition(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    base = datetime(2026, 10, 11, 10, 59, 0, tzinfo=UTC)
    raws = [raw_at(base + timedelta(seconds=s), s) for s in range(3)]

    files = store.write_batch(raws)

    assert len(files) == 1
    assert files[0].parent == tmp_path / "raw" / "dt=2026-10-11" / "hour=10"
    assert files[0].name.startswith("part-") and files[0].suffix == ".parquet"
    assert store.files() == files
    assert store.partitions() == [files[0].parent]
    assert not list((tmp_path / "raw").rglob("*.tmp")), "temp files are renamed away"


def test_duckdb_reads_the_lake_with_hive_partitions_in_utc(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    base = datetime(2026, 10, 11, 10, 15, 0, tzinfo=UTC)
    raws = [raw_at(base + timedelta(seconds=s), s) for s in range(3)]
    store.write_batch(raws)

    rows = (
        utc_con()
        .execute(
            "SELECT ingest_id, received_at, schema_version, raw_payload, dt, hour "
            f"FROM read_parquet('{store.glob}', hive_partitioning=true) ORDER BY received_at"
        )
        .fetchall()
    )

    assert [r[3] for r in rows] == [raw.raw_payload for raw in raws]
    assert [r[0] for r in rows] == [str(raw.ingest_id) for raw in raws]
    assert [r[1] for r in rows] == [raw.received_at for raw in raws]
    assert all(r[1].utcoffset() == timedelta(0) for r in rows)
    assert {(r[4].isoformat(), r[5]) for r in rows} == {("2026-10-11", 10)}


def test_a_batch_straddling_an_hour_boundary_splits_into_two_files(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    raws = [
        raw_at(datetime(2026, 10, 11, 10, 59, 59, tzinfo=UTC), 1),
        raw_at(datetime(2026, 10, 11, 11, 0, 0, tzinfo=UTC), 2),
    ]

    files = store.write_batch(raws)

    assert [f.parent.name for f in files] == ["hour=10", "hour=11"]
    assert len(store.partitions()) == 2


def test_empty_batch_writes_nothing(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    assert store.write_batch([]) == []
    assert store.files() == [] and store.partitions() == []


def test_non_utc_input_is_partitioned_by_its_utc_instant(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    athens = timezone(timedelta(hours=3))
    ts = datetime(2026, 10, 11, 13, 30, tzinfo=athens)  # 10:30 UTC

    (path,) = store.write_batch([raw_at(ts, 1)])

    assert path.parent.name == "hour=10"
    (got,) = utc_con().execute(
        f"SELECT received_at FROM read_parquet('{store.glob}')"
    ).fetchone() or (None,)
    assert got == ts and got.utcoffset() == timedelta(0)


def test_schema_is_pinned(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    (path,) = store.write_batch([raw_at(datetime(2026, 10, 11, 10, 0, tzinfo=UTC), 1)])
    assert pl.read_parquet(path).schema == SCHEMA
