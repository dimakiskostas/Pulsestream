"""DuckDB access: schema, connections and micro-batch writes (HLD decisions D3, D4).

DuckDB is the embedded analytical store: zero ops, excellent OLAP performance, one
writer process. The ``wiki_events`` table keeps a PRIMARY KEY on ``event_id`` so
writes are idempotent (``ON CONFLICT DO NOTHING``): replaying a window or
re-delivering a batch after a crash never produces duplicate rows (D2).
"""

from datetime import datetime
from pathlib import Path

import duckdb

from pulsestream.models import WikiEvent
from pulsestream.storage.aggregates import init_aggregates


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wiki_events (
            event_id VARCHAR PRIMARY KEY,
            entity VARCHAR,
            event_ts TIMESTAMPTZ,
            byte_delta INTEGER,
            is_bot BOOLEAN,
            received_at TIMESTAMPTZ,
            namespace INTEGER,
            title VARCHAR,
            event_type VARCHAR
        )
        """
    )
    init_aggregates(conn)


def connect(path: str | Path = ":memory:", *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open DuckDB with a UTC session and (for writers) the schema in place.

    DuckDB renders TIMESTAMPTZ values in the connection's ``TimeZone`` setting, which
    defaults to the machine's local zone (Europe/Athens on the development box). Pinning
    the session to UTC means every datetime that leaves the database is UTC-aware,
    matching the project convention that all timestamps are UTC (HLD §8).
    """
    con = duckdb.connect(str(path), read_only=read_only)
    con.execute("SET TimeZone = 'UTC'")
    if not read_only:
        init_db(con)
    return con


def _sql_literal(text: str) -> str:
    return text.replace("'", "''")


def ensure_lake_view(con: duckdb.DuckDBPyConnection, glob: str) -> None:
    """Expose the Parquet lake as the ``raw_events`` view.

    DuckDB evaluates the glob at query time, so files added or compacted later are
    picked up automatically. ``dt`` and ``hour`` are derived from ``received_at``
    rather than read as hive partition columns: the directories are themselves
    derived from ``received_at`` (UTC), so the values are identical, and DuckDB
    1.5.5 raises an internal error when aggregating hive columns over a glob that
    matches exactly one file. Filters on ``received_at`` are still pushed down to
    Parquet row-group statistics. The view can only be created once at least one
    file exists (DuckDB binds the query when the view is defined and errors on an
    empty glob).
    """
    con.execute(
        "CREATE OR REPLACE VIEW raw_events AS "
        "SELECT *, CAST(received_at AS DATE) AS dt, hour(received_at) AS hour "
        f"FROM read_parquet('{_sql_literal(glob)}', hive_partitioning=false)"
    )


def lake_row_stats(
    con: duckdb.DuckDBPyConnection, glob: str
) -> tuple[int, datetime | None, datetime | None]:
    """Row count and received_at span of the lake, straight from the Parquet files."""
    row = con.execute(
        "SELECT count(*), min(received_at), max(received_at) "
        f"FROM read_parquet('{_sql_literal(glob)}', hive_partitioning=false)"
    ).fetchone()
    if row is None:
        return 0, None, None
    count, first, last = row
    return int(count), first, last


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[WikiEvent]) -> None:
    """Insert a micro-batch; rows whose event_id already exists are skipped."""
    if not batch:  # DuckDB rejects an empty executemany
        return
    con.executemany(
        "INSERT INTO wiki_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (event_id) DO NOTHING",
        [
            (
                e.event_id,
                e.entity,
                e.event_ts,
                e.byte_delta,
                e.is_bot,
                e.received_at,
                e.namespace,
                e.title,
                e.event_type,
            )
            for e in batch
        ],
    )
