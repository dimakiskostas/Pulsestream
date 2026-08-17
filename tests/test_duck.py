from collections.abc import Iterator

import duckdb

from pulsestream.storage.duck import write_batch
from tests.factories import create_WikiEvent


def test_write_batch(con: Iterator[duckdb.DuckDBPyConnection]):
    write_batch(con, [create_WikiEvent()])
    con.execute("SELECT * FROM wiki_events").fetchall()


def test_init_db(con: Iterator[duckdb.DuckDBPyConnection]):
    con.execute("SELECT * FROM wiki_events").fetchall()


def test_column_ordering(con: Iterator[duckdb.DuckDBPyConnection]):
    event = create_WikiEvent(title="Value 1", entity="Value 2")
    write_batch(con, [event])
    rows = con.execute("SELECT wiki_events.title, wiki_events.entity FROM wiki_events").fetchall()
    assert rows[0] == ("Value 1", "Value 2")
