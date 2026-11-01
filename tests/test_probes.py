"""Liveness says the process serves; readiness says the pipeline can do useful work."""

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from pulsestream.api.app import create_app
from pulsestream.config import Settings
from pulsestream.models import RawEvent
from pulsestream.sources.fake import FakeSource
from tests.factories import _raw, create_payload


def raws(n: int = 5) -> list[RawEvent]:
    now = int(time.time())
    return [_raw(create_payload(meta={"id": f"p-{i}"}, timestamp=now - i)) for i in range(n)]


@asynccontextmanager
async def running(
    tmp_path: Path, source: object, **overrides: object
) -> AsyncIterator[httpx.AsyncClient]:
    cfg = Settings(
        _env_file=None,
        db_path=str(tmp_path / "probe.duckdb"),
        lake_root=str(tmp_path / "raw"),
        scheduler_enabled=False,
        batch_size=2,
        flush_interval=0.05,
        **overrides,  # type: ignore[arg-type]
    )
    app = create_app(cfg, source=source)  # type: ignore[arg-type]
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            for _ in range(300):  # wait for the replay to be processed
                if (await client.get("/stats/latest")).json()["events_total"] >= 1:
                    break
                await asyncio.sleep(0.02)
            client.app_ref = app  # type: ignore[attr-defined]
            yield client


async def test_liveness_is_cheap_and_always_200(tmp_path: Path) -> None:
    async with running(tmp_path, FakeSource(raws())) as client:
        response = await client.get("/health/live")
    assert response.status_code == 200 and response.json() == {"status": "alive"}


async def test_ready_after_a_replay_has_been_processed(tmp_path: Path) -> None:
    async with running(tmp_path, FakeSource(raws())) as client:
        response = await client.get("/health/ready")
    body = response.json()
    assert response.status_code == 200 and body["ready"] is True
    assert body["checks"] == {
        "consumer": True,
        "producer": True,
        "store": True,
        "source": True,
        "fresh": True,
    }
    assert body["last_processed_at"] is not None and body["silence_seconds"] < 60


async def test_not_ready_when_the_pipeline_has_gone_quiet(tmp_path: Path) -> None:
    async with running(tmp_path, FakeSource(raws()), ready_max_silence_seconds=0) as client:
        await asyncio.sleep(0.05)
        response = await client.get("/health/ready")
    body = response.json()
    assert response.status_code == 503 and body["ready"] is False
    assert body["checks"]["fresh"] is False and body["checks"]["consumer"] is True


async def test_not_ready_when_the_live_source_is_disconnected(tmp_path: Path) -> None:
    class DisconnectedSource(FakeSource):
        connected = False  # what WikimediaSource reports between a drop and a resume

    async with running(tmp_path, DisconnectedSource(raws())) as client:
        response = await client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["checks"]["source"] is False


async def test_not_ready_when_the_store_is_unreachable(tmp_path: Path) -> None:
    class Broken:
        def cursor(self) -> None:
            raise RuntimeError("database gone")

    async with running(tmp_path, FakeSource(raws())) as client:
        real = client.app_ref.state.con  # type: ignore[attr-defined]
        client.app_ref.state.con = Broken()  # type: ignore[attr-defined]
        try:
            response = await client.get("/health/ready")
        finally:
            client.app_ref.state.con = real  # type: ignore[attr-defined]
    assert response.status_code == 503 and response.json()["checks"]["store"] is False


async def test_not_ready_when_the_producer_failed(tmp_path: Path) -> None:
    class Exploding:
        async def stream(self) -> AsyncIterator[RawEvent]:
            raise RuntimeError("source died")
            yield  # pragma: no cover

    async with running(tmp_path, Exploding()) as client:
        await asyncio.sleep(0.05)
        response = await client.get("/health/ready")
    body = response.json()
    assert response.status_code == 503 and body["checks"]["producer"] is False
