"""ApiClient against a scripted API: frames come out typed, errors come out as httpx errors."""

import json
from datetime import UTC, datetime

import httpx
import polars as pl
import pytest

from pulsestream.dashboard.data import MINUTELY_SCHEMA, TOP_SCHEMA, ApiClient

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


def test_http_errors_propagate() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="down")

    client = ApiClient("http://api.test", transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        client.stats()
