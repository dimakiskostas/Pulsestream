"""DuckDB access: schema, connections and micro-batch writes (HLD decisions D3, D4).

DuckDB is the embedded analytical store: zero ops, excellent OLAP performance, one
writer process. The ``wiki_events`` table keeps a PRIMARY KEY on ``event_id`` so
writes are idempotent (``ON CONFLICT DO NOTHING``): replaying a window or
re-delivering a batch after a crash never produces duplicate rows (D2).
"""

import asyncio
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from pulsestream.models import Anomaly, FeatureSnapshot, WikiEvent
from pulsestream.storage.aggregates import init_aggregates

FEATURES_DDL = """
CREATE TABLE IF NOT EXISTS features_rolling (
    entity       VARCHAR     NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    count        INTEGER     NOT NULL,
    rate_per_min DOUBLE      NOT NULL,
    ewma         DOUBLE,
    mean         DOUBLE,
    std          DOUBLE,
    zscore       DOUBLE,
    is_anomaly   BOOLEAN     NOT NULL DEFAULT false,
    PRIMARY KEY (entity, window_start)
)
"""

ANOMALIES_DDL = """
CREATE TABLE IF NOT EXISTS anomalies (
    anomaly_id    UUID        PRIMARY KEY,
    detected_at   TIMESTAMPTZ NOT NULL,
    entity        VARCHAR     NOT NULL,
    metric        VARCHAR     NOT NULL,
    value         DOUBLE      NOT NULL,
    expected      DOUBLE,
    score         DOUBLE      NOT NULL,
    method        VARCHAR     NOT NULL,
    severity      VARCHAR     NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    event_id      VARCHAR,
    model_version VARCHAR,
    details       VARCHAR
)
"""


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
    conn.execute(FEATURES_DDL)
    conn.execute(ANOMALIES_DDL)
    init_aggregates(conn)


def connect(path: str | Path = ":memory:", *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open DuckDB with a UTC time zone and (for writers) the schema in place.

    DuckDB renders TIMESTAMPTZ values in the ``TimeZone`` setting, which defaults to
    the machine's local zone (Europe/Athens on the development box). Pinning it to
    UTC means every datetime that leaves the database is UTC-aware, matching the
    project convention that all timestamps are UTC (HLD §8). It is set GLOBAL: a
    plain ``SET`` is per connection and ``con.cursor()`` (used by every worker
    thread) would start from the local zone again.
    """
    con = duckdb.connect(str(path), read_only=read_only)
    con.execute("SET GLOBAL TimeZone = 'UTC'")
    if not read_only:
        init_db(con)
    return con


async def aquery(
    con: duckdb.DuckDBPyConnection, sql: str, params: Sequence[object] = ()
) -> list[tuple[Any, ...]]:
    """Run a read query on a worker thread with its own cursor.

    DuckDB calls are synchronous; on the event loop a slow query would stall ingestion
    and every other request. ``con.cursor()`` gives the worker thread a connection of
    its own to the same database (DuckDB wants one cursor per thread), and it sees
    everything the consumer has committed.
    """

    def run() -> list[tuple[Any, ...]]:
        cur = con.cursor()
        try:
            return cur.execute(sql, list(params)).fetchall()
        finally:
            cur.close()

    return await asyncio.to_thread(run)


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


def write_features(con: duckdb.DuckDBPyConnection, snapshots: list[FeatureSnapshot]) -> None:
    """Upsert closed-window features; a replay recomputes the same windows and replaces them."""
    if not snapshots:
        return
    con.executemany(
        "INSERT OR REPLACE INTO features_rolling VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                s.entity,
                s.window_start,
                s.window_end,
                s.count,
                s.rate_per_min,
                s.ewma,
                s.mean,
                s.std,
                s.zscore,
                s.is_anomaly,
            )
            for s in snapshots
        ],
    )


def write_anomalies(con: duckdb.DuckDBPyConnection, anomalies: list[Anomaly]) -> None:
    """Insert findings; ids are deterministic, so a replayed finding is skipped, not doubled."""
    if not anomalies:
        return
    con.executemany(
        "INSERT INTO anomalies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (anomaly_id) DO NOTHING",
        [
            (
                a.anomaly_id,
                a.detected_at,
                a.entity,
                a.metric,
                a.value,
                a.expected,
                a.score,
                a.method,
                a.severity,
                a.window_start,
                a.window_end,
                a.event_id,
                a.model_version,
                a.details,
            )
            for a in anomalies
        ],
    )


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
