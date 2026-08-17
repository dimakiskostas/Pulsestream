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


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[WikiEvent]) -> None:
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
