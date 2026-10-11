"""Data-quality gates (HLD §10): pandera schemas for the lake and the DuckDB tables.

The schemas pin types, nullability, ranges and a few cross-column facts for the
three datasets the pipeline produces. They run in the test suite against the
replay fixture and on demand with ``pulsestream check`` against a live database
and lake, so a regression in the filter, the writer or the aggregates fails
loudly with a list of offending columns instead of drifting silently.

DuckDB INTEGER columns arrive in Polars as Int32 (via ``.pl()``), hence the dtypes.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pandera.polars as pa
import polars as pl

from pulsestream.storage.parquet import ParquetRawStore

UUID_LEN = 36

RAW_LAKE = pa.DataFrameSchema(
    {
        "ingest_id": pa.Column(pl.Utf8, pa.Check.str_length(UUID_LEN, UUID_LEN)),
        "source": pa.Column(pl.Utf8, pa.Check.str_length(min_value=1)),
        "received_at": pa.Column(pl.Datetime("us", "UTC")),
        "schema_version": pa.Column(pl.Int32, pa.Check.eq(1)),
        "raw_payload": pa.Column(pl.Utf8, pa.Check.str_startswith("{")),
    },
    strict=True,
    name="raw_lake",
)

WIKI_EVENTS = pa.DataFrameSchema(
    {
        "event_id": pa.Column(pl.Utf8, pa.Check.str_length(UUID_LEN, UUID_LEN)),
        "entity": pa.Column(pl.Utf8, pa.Check.str_length(min_value=1)),
        "event_ts": pa.Column(pl.Datetime("us", "UTC")),
        "byte_delta": pa.Column(pl.Int32),
        "is_bot": pa.Column(pl.Boolean),
        "received_at": pa.Column(pl.Datetime("us", "UTC")),
        "namespace": pa.Column(pl.Int32, pa.Check.ge(0)),
        "title": pa.Column(pl.Utf8, pa.Check.str_length(min_value=1)),
        "event_type": pa.Column(pl.Utf8, pa.Check.isin(["edit", "new"])),
    },
    checks=[
        # An event is received after it happened (allow a minute of clock skew).
        pa.Check(
            lambda data: data.lazyframe.select(
                pl.col("received_at") >= pl.col("event_ts") - pl.duration(minutes=1)
            ),
            name="received_after_event",
        ),
    ],
    strict=True,
    unique=["event_id"],
    name="wiki_events",
)

AGG_MINUTELY = pa.DataFrameSchema(
    {
        "bucket_ts": pa.Column(pl.Datetime("us", "UTC")),
        "entity": pa.Column(pl.Utf8, pa.Check.str_length(min_value=1)),
        "count": pa.Column(pl.Int32, pa.Check.ge(1)),
        "bot_count": pa.Column(pl.Int32, pa.Check.ge(0)),
        "avg_value": pa.Column(pl.Float64, nullable=True),
        "max_value": pa.Column(pl.Int32, nullable=True),
        "anomaly_count": pa.Column(pl.Int32, pa.Check.ge(0)),
    },
    checks=[
        pa.Check(
            lambda data: data.lazyframe.select(pl.col("bot_count") <= pl.col("count")),
            name="bots_within_count",
        ),
        pa.Check(
            lambda data: data.lazyframe.select(
                (pl.col("bucket_ts").dt.second() == 0) & (pl.col("bucket_ts").dt.microsecond() == 0)
            ),
            name="minute_aligned",
        ),
    ],
    strict=True,
    unique=["bucket_ts", "entity"],
    name="agg_minutely",
)

TABLES = {"wiki_events": WIKI_EVENTS, "agg_minutely": AGG_MINUTELY}


@dataclass(frozen=True)
class QualityReport:
    rows: dict[str, int]  # dataset name -> rows validated


def validate_tables(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Validate every DuckDB table; raises pandera SchemaErrors listing all failures."""
    counts: dict[str, int] = {}
    for table, schema in TABLES.items():
        frame = con.execute(f"SELECT * FROM {table}").pl()
        schema.validate(frame, lazy=True)
        counts[table] = frame.height
    return counts


def validate_lake(store: ParquetRawStore) -> int:
    """Validate the whole lake as one frame; returns the number of rows checked."""
    if not store.files():
        return 0
    frame = pl.read_parquet(store.glob, hive_partitioning=False)
    RAW_LAKE.validate(frame, lazy=True)
    return frame.height


def check(con: duckdb.DuckDBPyConnection, store: ParquetRawStore | None) -> QualityReport:
    rows = validate_tables(con)
    if store is not None:
        rows["raw_lake"] = validate_lake(store)
    return QualityReport(rows=rows)
