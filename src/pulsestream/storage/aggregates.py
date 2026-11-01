"""Derived tables, recomputed rather than incremented (ADR 0009).

``agg_minutely`` is the chart table: one row per (minute bucket, entity) with the
event count, the bot count, the mean and max byte delta and, from Phase 3, the
number of anomalies raised in that bucket. ``refresh_agg_minutely`` rewrites the
last ``window_minutes`` of buckets from ``wiki_events`` with ``INSERT OR REPLACE``
on the primary key, so running it twice, or after a replay, never double-counts,
and a late event simply updates its bucket on the next refresh.
"""

from datetime import UTC, datetime, timedelta

import duckdb

AGG_MINUTELY_DDL = """
CREATE TABLE IF NOT EXISTS agg_minutely (
    bucket_ts     TIMESTAMPTZ NOT NULL,
    entity        VARCHAR     NOT NULL,
    count         INTEGER     NOT NULL,
    bot_count     INTEGER     NOT NULL,
    avg_value     DOUBLE,
    max_value     INTEGER,
    anomaly_count INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_ts, entity)
)
"""

REFRESH_SQL = """
INSERT OR REPLACE INTO agg_minutely
SELECT time_bucket(INTERVAL 1 MINUTE, event_ts)          AS bucket_ts,
       entity,
       CAST(count(*) AS INTEGER)                          AS count,
       CAST(count(*) FILTER (WHERE is_bot) AS INTEGER)    AS bot_count,
       avg(byte_delta)                                    AS avg_value,
       CAST(max(byte_delta) AS INTEGER)                   AS max_value,
       0                                                  AS anomaly_count
FROM wiki_events
WHERE event_ts >= ?
GROUP BY 1, 2
"""


def init_aggregates(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(AGG_MINUTELY_DDL)


def refresh_agg_minutely(
    con: duckdb.DuckDBPyConnection, *, window_minutes: int = 180, now: datetime | None = None
) -> int:
    """Recompute every (minute, entity) bucket that starts within the last ``window_minutes``.

    Returns the number of buckets written. Buckets older than the window are left as
    they are; they were final when they left it.
    """
    since = (now if now is not None else datetime.now(UTC)) - timedelta(minutes=window_minutes)
    row = con.execute(REFRESH_SQL, [since]).fetchone()
    return int(row[0]) if row is not None else 0
