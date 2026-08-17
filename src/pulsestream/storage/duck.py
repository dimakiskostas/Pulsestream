import duckdb

from .models import WikiEvent


def init_db(db_path: str) -> None:
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wiki_events (
            event_id VARCHAR PRIMARY KEY,
            entity VARCHAR,
            event_ts TIMESTAMP,
            byte_delta INTEGER,
            is_bot BOOLEAN,
            received_at TIMESTAMP,
            namespace INTEGER,
            title VARCHAR,
            event_type VARCHAR
        )
        """
    )


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[WikiEvent]) -> None:
    con.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
