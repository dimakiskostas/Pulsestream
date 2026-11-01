"""ApiClient against a scripted API: frames come out typed, errors come out as httpx errors."""

import json
from datetime import UTC, datetime

import httpx
import polars as pl
import pytest

from pulsestream.dashboard.data import (
    ANOMALIES_SCHEMA,
    FEATURES_SCHEMA,
    MINUTELY_SCHEMA,
    TOP_SCHEMA,
    ApiClient,
)

CANNED = {
    "/health": {
        "status": "ok",
        "pipeline": {"producer": "running", "consumer": "running", "queue_depth": 0},
    },
    "/stats/latest": {
        "events_total": 42,
        "events_last_min": 7,
        "latest_event_ts": "2026-10-11T10:54:26Z",
        "lag_seconds": 2.8,
    },
    "/lake": {
        "enabled": True,
        "root": "data/raw",
        "files": 3,
        "partitions": 1,
        "bytes": 1000,
        "rows": 42,
        "first_received_at": None,
        "last_received_at": None,
    },
    "/jobs": {"scheduler_enabled": True, "jobs": [{"name": "refresh_aggregates", "runs": 1}]},
    "/aggregates/minutely": {
        "minutes": 60,
        "entity": None,
        "points": [
            {
                "bucket_ts": "2026-10-11T10:53:00Z",
                "entity": "*",
                "count": 5,
                "bot_count": 2,
                "avg_value": 12.5,
                "max_value": 40,
                "anomaly_count": 0,
            },
            {
                "bucket_ts": "2026-10-11T10:54:00Z",
                "entity": "*",
                "count": 9,
                "bot_count": 1,
                "avg_value": None,
                "max_value": None,
                "anomaly_count": 1,
            },
        ],
    },
    "/entities/top": {
        "minutes": 60,
        "entities": [{"entity": "enwiki", "count": 9, "bot_count": 1}],
    },
    "/anomalies": {
        "minutes": 60,
        "entity": None,
        "metric": None,
        "severity": None,
        "anomalies": [
            {
                "anomaly_id": "4b2c6a4e-0f45-5d0b-9b7d-5f9a1c2d3e4f",
                "detected_at": "2026-10-11T10:53:00Z",
                "entity": "enwiki",
                "metric": "edit_rate",
                "value": 40.0,
                "expected": 10.0,
                "score": 20.0,
                "method": "ewma_band",
                "severity": "critical",
                "window_start": "2026-10-11T10:52:00Z",
                "window_end": "2026-10-11T10:53:00Z",
                "event_id": None,
                "model_version": None,
                "details": "count 40 outside [5.5, 14.5]",
            }
        ],
    },
    "/anomalies/summary": {
        "minutes": 60,
        "total": 1,
        "by_severity": {"critical": 1},
        "by_metric": {"edit_rate": 1},
        "top_entities": [{"entity": "enwiki", "count": 1}],
    },
    "/features": {
        "entity": "enwiki",
        "minutes": 60,
        "k": 3.0,
        "points": [
            {
                "window_start": "2026-10-11T10:52:00Z",
                "window_end": "2026-10-11T10:53:00Z",
                "count": 40,
                "rate_per_min": 40.0,
                "ewma": 10.0,
                "mean": 10.0,
                "std": 1.5,
                "zscore": 20.0,
                "upper": 14.5,
                "lower": 5.5,
                "is_anomaly": True,
            }
        ],
    },
}


def scripted() -> tuple[ApiClient, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = CANNED.get(request.url.path)
        if body is None:
            return httpx.Response(404, json={"detail": "nope"})
        return httpx.Response(200, content=json.dumps(body))

    return ApiClient("http://api.test", transport=httpx.MockTransport(handler)), seen


def test_simple_endpoints_return_json() -> None:
    client, _ = scripted()
    assert client.health()["status"] == "ok"
    assert client.stats()["events_total"] == 42
    assert client.lake()["files"] == 3
    assert client.jobs() == [{"name": "refresh_aggregates", "runs": 1}]


def test_minutely_becomes_a_typed_frame_and_passes_query_params() -> None:
    client, seen = scripted()

    frame = client.minutely(minutes=30, entity="enwiki")

    assert frame.schema == MINUTELY_SCHEMA
    assert frame["bucket_ts"].to_list() == [
        datetime(2026, 10, 11, 10, 53, tzinfo=UTC),
        datetime(2026, 10, 11, 10, 54, tzinfo=UTC),
    ]
    assert frame["count"].to_list() == [5, 9]
    assert frame["avg_value"].to_list() == [12.5, None]
    assert seen[-1].url.params["minutes"] == "30" and seen[-1].url.params["entity"] == "enwiki"


def test_entity_none_is_not_sent_and_empty_results_keep_the_schema() -> None:
    client, seen = scripted()
    CANNED["/aggregates/minutely"]["points"] = []  # type: ignore[index]
    try:
        frame = client.minutely()
    finally:
        CANNED["/aggregates/minutely"]["points"] = [  # type: ignore[index]
            {
                "bucket_ts": "2026-10-11T10:53:00Z",
                "entity": "*",
                "count": 5,
                "bot_count": 2,
                "avg_value": 12.5,
                "max_value": 40,
                "anomaly_count": 0,
            },
        ]
    assert frame.is_empty() and frame.schema == MINUTELY_SCHEMA
    assert "entity" not in seen[-1].url.params


def test_top_entities_frame() -> None:
    client, _ = scripted()
    frame = client.top_entities(limit=5)
    assert frame.schema == TOP_SCHEMA
    assert frame.equals(
        pl.DataFrame([{"entity": "enwiki", "count": 9, "bot_count": 1}], schema=TOP_SCHEMA)
    )


def test_anomalies_summary_and_features_frames() -> None:
    client, seen = scripted()

    found = client.anomalies(limit=5)
    assert found.schema == ANOMALIES_SCHEMA and found.height == 1
    assert found["entity"][0] == "enwiki" and found["severity"][0] == "critical"
    assert found["window_start"][0] == datetime(2026, 10, 11, 10, 52, tzinfo=UTC)
    assert seen[-1].url.params["limit"] == "5"

    summary = client.anomaly_summary(minutes=30)
    assert summary["total"] == 1 and seen[-1].url.params["minutes"] == "30"

    band = client.features("enwiki")
    assert band.schema == FEATURES_SCHEMA
    assert band["upper"][0] == 14.5 and band["lower"][0] == 5.5 and band["is_anomaly"][0] is True
    assert seen[-1].url.params["entity"] == "enwiki"


def test_http_errors_propagate() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="down")

    client = ApiClient("http://api.test", transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        client.stats()
