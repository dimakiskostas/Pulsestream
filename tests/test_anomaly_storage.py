from datetime import UTC, datetime, timedelta

import duckdb

from pulsestream.models import Anomaly, FeatureSnapshot, anomaly_id, severity_for
from pulsestream.storage.aggregates import refresh_agg_minutely
from pulsestream.storage.duck import write_anomalies, write_batch, write_features
from tests.factories import create_WikiEvent

T0 = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)
T1 = T0 + timedelta(minutes=1)


def snapshot(count: int, zscore: float | None = None, is_anomaly: bool = False) -> FeatureSnapshot:
    return FeatureSnapshot(
        entity="enwiki",
        window_start=T0,
        window_end=T1,
        count=count,
        rate_per_min=float(count),
        ewma=10.0,
        mean=10.5,
        std=2.0,
        zscore=zscore,
        is_anomaly=is_anomaly,
    )


def anomaly(key: str, entity: str = "enwiki", metric: str = "edit_rate") -> Anomaly:
    return Anomaly(
        anomaly_id=anomaly_id("ewma_band", entity, metric, key),
        detected_at=T1,
        entity=entity,
        metric=metric,  # type: ignore[arg-type]
        value=42.0,
        expected=10.0,
        score=16.0,
        method="ewma_band",
        severity=severity_for(16.0),
        window_start=T0,
        window_end=T1,
    )


def test_anomaly_ids_are_deterministic_and_distinct_per_key() -> None:
    assert anomaly_id("ewma_band", "enwiki", "edit_rate", "k") == anomaly_id(
        "ewma_band", "enwiki", "edit_rate", "k"
    )
    assert anomaly_id("ewma_band", "enwiki", "edit_rate", "k") != anomaly_id(
        "ewma_band", "dewiki", "edit_rate", "k"
    )
    assert anomaly_id("mad_zscore", "enwiki", "byte_delta", "k") != anomaly_id(
        "ewma_band", "enwiki", "edit_rate", "k"
    )


def test_severity_thresholds() -> None:
    assert severity_for(3.2) == "warn"
    assert severity_for(-5.0) == "high"
    assert severity_for(8.0) == "critical"
    assert severity_for(-12.5) == "critical"


def test_features_are_upserted_by_entity_and_window(con: duckdb.DuckDBPyConnection) -> None:
    write_features(con, [snapshot(count=12)])
    write_features(con, [snapshot(count=13, zscore=1.5, is_anomaly=True)])  # replayed, revised

    rows = con.execute(
        "SELECT entity, window_start, count, zscore, is_anomaly FROM features_rolling"
    ).fetchall()
    assert rows == [("enwiki", T0, 13, 1.5, True)]
    write_features(con, [])  # no-op


def test_anomalies_are_written_once_per_id(con: duckdb.DuckDBPyConnection) -> None:
    write_anomalies(con, [anomaly("w1"), anomaly("w1"), anomaly("w2")])
    write_anomalies(con, [anomaly("w1")])
    write_anomalies(con, [])

    rows = con.execute(
        "SELECT entity, metric, severity, score, window_start, event_id FROM anomalies "
        "ORDER BY anomaly_id"
    ).fetchall()
    assert len(rows) == 2
    assert all(r[:4] == ("enwiki", "edit_rate", "critical", 16.0) for r in rows)
    assert all(r[4] == T0 and r[5] is None for r in rows)
    (uuid_type,) = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'anomalies' AND column_name = 'anomaly_id'"
    ).fetchone() or (None,)
    assert uuid_type == "UUID"


def test_aggregates_count_anomalies_per_minute_bucket(con: duckdb.DuckDBPyConnection) -> None:
    write_batch(
        con,
        [
            create_WikiEvent(event_id="e1", entity="enwiki", event_ts=T0 + timedelta(seconds=5)),
            create_WikiEvent(event_id="e2", entity="enwiki", event_ts=T0 + timedelta(seconds=50)),
            create_WikiEvent(event_id="d1", entity="dewiki", event_ts=T0 + timedelta(seconds=7)),
        ],
    )
    write_anomalies(
        con,
        [
            anomaly("rate"),  # edit_rate finding for the 10:00 enwiki window
            anomaly("e2", metric="byte_delta"),  # per-event finding in the same minute
            anomaly("other", entity="frwiki"),  # no events for frwiki: not a bucket
        ],
    )

    written = refresh_agg_minutely(con, window_minutes=60, now=T0 + timedelta(minutes=5))

    assert written == 2
    rows = con.execute(
        "SELECT entity, count, anomaly_count FROM agg_minutely ORDER BY entity"
    ).fetchall()
    assert rows == [("dewiki", 1, 0), ("enwiki", 2, 2)]
