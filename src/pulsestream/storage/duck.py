"""DuckDB access: schema, connections and micro-batch writes (HLD decisions D3, D4).

DuckDB is the embedded analytical store: zero ops, excellent OLAP performance, one
writer process. The ``wiki_events`` table keeps a PRIMARY KEY on ``event_id`` so
writes are idempotent (``ON CONFLICT DO NOTHING``): replaying a window or
re-delivering a batch after a crash never produces duplicate rows (D2).
"""

from pathlib import Path

import duckdb

from pulsestream.models import WikiEvent


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
