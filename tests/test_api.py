"""API contract tests: drive the ASGI app in-process with httpx, no server, no network."""

import asyncio
import time
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import polars as pl
import pytest

from pulsestream.api.app import create_app
from pulsestream.config import Settings
from pulsestream.models import RawEvent
from pulsestream.pipeline import BatchResult
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
        _env_file=None,
        db_path=str(tmp_path / "api.duckdb"),
        lake_root=str(tmp_path / "raw"),
        batch_size=4,
        flush_interval=0.05,
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
    assert {
        "HealthStatus",
        "StatsLatest",
        "TimeseriesResponse",
        "LakeStats",
        "AggregatesResponse",
        "TopEntitiesResponse",
        "JobsResponse",
    } <= set(schema["components"]["schemas"])


async def test_aggregates_and_top_entities_follow_the_refresh_job(
    client: httpx.AsyncClient,
) -> None:
    # Charts read agg_minutely, which the scheduler refreshes; before a refresh it is empty.
    assert (await client.get("/aggregates/minutely")).json()["points"] == []

    await client.post("/jobs/refresh_aggregates/run")

    combined = (await client.get("/aggregates/minutely", params={"minutes": 5})).json()
    assert combined["entity"] is None and combined["minutes"] == 5
    assert all(p["entity"] == "*" for p in combined["points"])
    assert sum(p["count"] for p in combined["points"]) == N_EVENTS
    assert sum(p["bot_count"] for p in combined["points"]) == 4  # i % 3 == 0 -> 0, 3, 6, 9
    assert all(p["anomaly_count"] == 0 for p in combined["points"])

    enwiki = (await client.get("/aggregates/minutely", params={"entity": "enwiki"})).json()
    assert enwiki["entity"] == "enwiki"
    assert sum(p["count"] for p in enwiki["points"]) == 5  # odd i
    assert all(p["entity"] == "enwiki" for p in enwiki["points"])

    top = (await client.get("/entities/top", params={"minutes": 5, "limit": 1})).json()
    assert top["entities"] == [
        {"entity": "dewiki", "count": 5, "bot_count": 2}
    ]  # tie -> name order
    both = (await client.get("/entities/top", params={"minutes": 5})).json()["entities"]
    assert [e["entity"] for e in both] == ["dewiki", "enwiki"]

    assert (await client.get("/aggregates/minutely", params={"minutes": 0})).status_code == 422
    assert (await client.get("/entities/top", params={"limit": 0})).status_code == 422


async def test_jobs_are_scheduled_and_can_be_run_on_demand(client: httpx.AsyncClient) -> None:
    body = (await client.get("/jobs")).json()
    assert body["scheduler_enabled"] is True
    assert [j["name"] for j in body["jobs"]] == ["refresh_aggregates", "compact_lake"]
    assert all(j["runs"] == 0 and j["next_run_at"] is not None for j in body["jobs"])
    assert all(j["next_run_at"].endswith("Z") for j in body["jobs"]), "UTC, like every timestamp"

    run = (await client.post("/jobs/refresh_aggregates/run")).json()
    assert (run["runs"], run["failures"], run["last_error"]) == (1, 0, None)
    assert run["last_result"].endswith("buckets")

    assert (await client.post("/jobs/nope/run")).status_code == 404


async def test_lake_reports_files_rows_and_span(client: httpx.AsyncClient) -> None:
    body = (await client.get("/lake")).json()
    assert body["enabled"] is True and body["root"].endswith("raw")
    assert body["files"] >= 1 and body["partitions"] >= 1 and body["bytes"] > 0
    assert body["rows"] == N_EVENTS + 1, "the dropped categorize event is in the lake"
    assert body["first_received_at"] <= body["last_received_at"]


async def test_lifespan_shutdown_flushes_and_closes_cleanly(tmp_path: Path) -> None:
    cfg = Settings(
        _env_file=None,
        db_path=str(tmp_path / "x.duckdb"),
        lake_root=str(tmp_path / "raw"),
        batch_size=1000,
        flush_interval=60.0,
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
    # The raw lake received the whole batch, including the dropped categorize event.
    lake_rows = sum(pl.read_parquet(f).height for f in app.state.raw_store.files())
    assert lake_rows == N_EVENTS + 1


async def test_api_stays_responsive_while_a_batch_is_being_processed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DuckDB work runs on a worker thread, so /health answers mid-batch."""
    import pulsestream.api.app as app_module
    from pulsestream.processing.processor import StreamProcessor

    class SlowProcessor(StreamProcessor):
        in_process = False

        def process(self, raws: list[RawEvent]) -> BatchResult:
            SlowProcessor.in_process = True
            try:
                time.sleep(1.0)  # blocking, like a heavy query or a big Parquet write
                return super().process(raws)
            finally:
                SlowProcessor.in_process = False

    monkeypatch.setattr(app_module, "StreamProcessor", SlowProcessor)
    cfg = Settings(
        _env_file=None,
        db_path=str(tmp_path / "slow.duckdb"),
        lake_root=str(tmp_path / "raw"),
        batch_size=1,
        flush_interval=0.05,
    )
    app = create_app(cfg, source=FakeSource(recent_raws()))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            for _ in range(500):  # wait until a batch is inside the slow processor
                if SlowProcessor.in_process:
                    break
                await asyncio.sleep(0.01)
            assert SlowProcessor.in_process
            response = await c.get("/health")
            assert response.status_code == 200
            assert SlowProcessor.in_process, "the request completed while processing was busy"


async def test_lake_can_be_disabled(tmp_path: Path) -> None:
    cfg = Settings(
        _env_file=None,
        db_path=str(tmp_path / "y.duckdb"),
        lake_root=str(tmp_path / "raw"),
        lake_enabled=False,
        flush_interval=0.05,
    )
    app = create_app(cfg, source=FakeSource(recent_raws()))
    async with app.router.lifespan_context(app):
        await asyncio.wait_for(app.state.producer, timeout=5)
        await asyncio.sleep(0.2)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            body = (await c.get("/lake")).json()
    assert body["enabled"] is False and body["files"] == 0 and body["rows"] == 0
    assert app.state.raw_store is None
    assert not (tmp_path / "raw").exists()
