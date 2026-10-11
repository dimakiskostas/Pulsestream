from datetime import UTC, datetime, timedelta

import duckdb

from pulsestream.models import WikiEvent
from pulsestream.storage.aggregates import refresh_agg_minutely
from pulsestream.storage.duck import write_batch
from tests.factories import create_WikiEvent

T0 = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)
NOW = T0 + timedelta(minutes=5)
COLUMNS = "bucket_ts, entity, count, bot_count, avg_value, max_value, anomaly_count"


def events() -> list[WikiEvent]:
    """enwiki: three events in the 10:00 bucket (one bot) and one at 10:01; dewiki: one bot edit."""
    return [
        create_WikiEvent(
            event_id="a1", entity="enwiki", event_ts=T0 + timedelta(seconds=5), byte_delta=10
        ),
        create_WikiEvent(
            event_id="a2",
            entity="enwiki",
            event_ts=T0 + timedelta(seconds=20),
            byte_delta=-20,
            is_bot=True,
        ),
        create_WikiEvent(
            event_id="a3", entity="enwiki", event_ts=T0 + timedelta(seconds=59), byte_delta=30
        ),
        create_WikiEvent(
            event_id="a4",
            entity="enwiki",
            event_ts=T0 + timedelta(minutes=1, seconds=1),
            byte_delta=5,
        ),
        create_WikiEvent(
            event_id="b1",
            entity="dewiki",
            event_ts=T0 + timedelta(seconds=30),
            byte_delta=100,
            is_bot=True,
        ),
    ]


def rows(con: duckdb.DuckDBPyConnection) -> list[tuple[object, ...]]:
    return con.execute(f"SELECT {COLUMNS} FROM agg_minutely ORDER BY bucket_ts, entity").fetchall()


def test_refresh_builds_minute_buckets_per_entity(con: duckdb.DuckDBPyConnection) -> None:
    write_batch(con, events())

    written = refresh_agg_minutely(con, window_minutes=60, now=NOW)

    assert written == 3
    assert rows(con) == [
        (T0, "dewiki", 1, 1, 100.0, 100, 0),
        (T0, "enwiki", 3, 1, (10 - 20 + 30) / 3, 30, 0),
        (T0 + timedelta(minutes=1), "enwiki", 1, 0, 5.0, 5, 0),
    ]


def test_refresh_is_idempotent_and_absorbs_late_events(con: duckdb.DuckDBPyConnection) -> None:
    write_batch(con, events())
    refresh_agg_minutely(con, window_minutes=60, now=NOW)
    first = rows(con)

    assert refresh_agg_minutely(con, window_minutes=60, now=NOW) == 3
    assert rows(con) == first, "recomputing is a no-op without new data"

    late = create_WikiEvent(
        event_id="late", entity="enwiki", event_ts=T0 + timedelta(seconds=40), byte_delta=0
    )
    write_batch(con, [late])
    refresh_agg_minutely(con, window_minutes=60, now=NOW)
    (enwiki_1000,) = [r for r in rows(con) if r[0] == T0 and r[1] == "enwiki"]
    assert enwiki_1000[2] == 4, "the late event updated its bucket instead of adding a row"
    assert len(rows(con)) == 3


def test_refresh_only_rewrites_buckets_inside_the_window(con: duckdb.DuckDBPyConnection) -> None:
    old = create_WikiEvent(
        event_id="old", entity="enwiki", event_ts=T0 - timedelta(hours=3), byte_delta=1
    )
    write_batch(con, [old, *events()])

    assert refresh_agg_minutely(con, window_minutes=60, now=NOW) == 3
    assert all(r[0] >= T0 for r in rows(con)), "the three-hour-old event is outside the window"

    # A bucket that was written while inside the window survives once the window moves on.
    assert refresh_agg_minutely(con, window_minutes=60, now=NOW + timedelta(hours=2)) == 0
    assert len(rows(con)) == 3
