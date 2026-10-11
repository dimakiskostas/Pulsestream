"""API contract tests: drive the ASGI app in-process with httpx, no server, no network."""

import asyncio
import time
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from pulsestream.api.app import create_app
from pulsestream.config import Settings
from pulsestream.models import RawEvent
from pulsestream.sources.fake import FakeSource
from tests.factories import _raw, create_payload

N_EVENTS = 10


def recent_raws() -> list[RawEvent]:
    """10 edits on two wikis time-stamped 'now', plus one categorize event that is dropped."""
    now = int(time.time())
    raws = [
        _raw(
            create_payload(
                meta={"id": f"api-{i}"},
                wiki="enwiki" if i % 2 else "dewiki",
                timestamp=now - i,
                bot=(i % 3 == 0),
            )
        )
        for i in range(N_EVENTS)
    ]
    raws.append(_raw(create_payload(meta={"id": "api-drop"}, type="categorize")))
    return raws


@pytest.fixture
async def client(tmp_path: Path) -> AsyncIterator[httpx.AsyncClient]:
    cfg = Settings(
        _env_file=None, db_path=str(tmp_path / "api.duckdb"), batch_size=4, flush_interval=0.05
    )
    app = create_app(cfg, source=FakeSource(recent_raws()))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            # The replay is finite; wait (up to ~10 s) until the consumer has flushed all of it.
            for _ in range(500):
                if (await c.get("/stats/latest")).json()["events_total"] == N_EVENTS:
                    break
                await asyncio.sleep(0.02)
            yield c


async def test_health_reports_pipeline_task_states(client: httpx.AsyncClient) -> None:
    body = (await client.get("/health")).json()
    assert body["status"] == "ok"
    assert body["pipeline"]["producer"] == "done"  # finite replay source has finished
    assert body["pipeline"]["consumer"] == "running"
    assert body["pipeline"]["queue_depth"] == 0


async def test_stats_latest_counts_and_lag(client: httpx.AsyncClient) -> None:
    body = (await client.get("/stats/latest")).json()
    assert body["events_total"] == N_EVENTS
    assert body["events_last_min"] == N_EVENTS
    assert body["latest_event_ts"].endswith("Z") or "+00:00" in body["latest_event_ts"]
    assert 0 <= body["lag_seconds"] < 60


async def test_timeseries_buckets_sum_to_the_total(client: httpx.AsyncClient) -> None:
    body = (await client.get("/timeseries", params={"bucket": "10s", "minutes": 5})).json()
    assert body["bucket"] == "10s" and body["minutes"] == 5
    assert sum(p["count"] for p in body["points"]) == N_EVENTS
    times = [p["time"] for p in body["points"]]
    assert times == sorted(times)


async def test_timeseries_rejects_unknown_bucket_and_bad_minutes(client: httpx.AsyncClient) -> None:
    assert (await client.get("/timeseries", params={"bucket": "7s"})).status_code == 422
    assert (await client.get("/timeseries", params={"minutes": 0})).status_code == 422


async def test_openapi_documents_the_response_models(client: httpx.AsyncClient) -> None:
    schema = (await client.get("/openapi.json")).json()
    assert schema["info"]["title"] == "PulseStream"
    assert {"HealthStatus", "StatsLatest", "TimeseriesResponse"} <= set(
        schema["components"]["schemas"]
    )


async def test_lifespan_shutdown_flushes_and_closes_cleanly(tmp_path: Path) -> None:
    cfg = Settings(
        _env_file=None, db_path=str(tmp_path / "x.duckdb"), batch_size=1000, flush_interval=60.0
    )
    app = create_app(cfg, source=FakeSource(recent_raws()))
    async with app.router.lifespan_context(app):
        await asyncio.wait_for(app.state.producer, timeout=5)  # everything is queued/buffered
        await asyncio.sleep(0.05)
    # The sentinel made the consumer flush the partial batch before the connection closed.
    assert app.state.consumer.done() and app.state.consumer.exception() is None
    from pulsestream.storage.duck import connect

    con = connect(tmp_path / "x.duckdb", read_only=True)
    assert con.execute("SELECT count(*) FROM wiki_events").fetchone() == (N_EVENTS,)
    con.close()
