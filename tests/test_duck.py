from datetime import UTC, datetime

import duckdb

from pulsestream.storage.duck import write_batch
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


def test_column_ordering(con: duckdb.DuckDBPyConnection):
    event = create_WikiEvent(title="Value 1", entity="Value 2")
    write_batch(con, [event])
    rows = con.execute("SELECT wiki_events.title, wiki_events.entity FROM wiki_events").fetchall()
    assert rows[0] == ("Value 1", "Value 2")
