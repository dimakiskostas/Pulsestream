"""The ingest service end to end on fakeredis: source → queue → bus, probes, retries."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import fakeredis.aioredis
import httpx
import pytest
from redis import exceptions as redis_exceptions

from pulsestream.bus.redis import RedisStreamsBus
from pulsestream.common import metrics
from pulsestream.config import Settings
from pulsestream.ingest.app import create_ingest_app
from pulsestream.models import RawEvent
from pulsestream.sources.fake import FakeSource
from tests.factories import _raw, create_payload


def raws(n: int) -> list[RawEvent]:
    return [_raw(create_payload(meta={"id": f"i-{k}"})) for k in range(n)]


async def no_sleep(_: float) -> None:
    return None


def make_bus(**kw: Any) -> RedisStreamsBus:
    kw.setdefault("client", fakeredis.aioredis.FakeRedis(decode_responses=True))
    return RedisStreamsBus(block_ms=10, sleep=no_sleep, **kw)


@asynccontextmanager
async def running(
    source: object, bus: object, **overrides: object
) -> AsyncIterator[httpx.AsyncClient]:
    params: dict[str, object] = {"bus": "redis", "batch_size": 3, "flush_interval": 0.05}
    params.update(overrides)
    cfg = Settings(_env_file=None, **params)  # type: ignore[arg-type]
    app = create_ingest_app(cfg, source=source, bus=bus)  # type: ignore[arg-type]
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            client.app_ref = app  # type: ignore[attr-defined]
            yield client


async def published(client: httpx.AsyncClient, at_least: int) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for _ in range(400):
        stats = (await client.get("/stats")).json()
        if stats["published_total"] >= at_least:
            break
        await asyncio.sleep(0.01)
    return stats


async def test_everything_the_source_yields_lands_on_the_bus_in_order() -> None:
    bus = make_bus()
    events = raws(10)
    async with running(FakeSource(events), bus) as client:
        stats = await published(client, 10)
    assert stats["published_total"] == 10 and stats["stream_length"] == 10
    assert stats["batches"] == 4 and stats["publish_failures"] == 0  # 3 + 3 + 3 + 1 by time
    assert stats["stream_first_id"] is not None and stats["queue_depth"] == 0
    assert [d.event for d in [x async for x in bus.replay()]] == events


async def test_ready_while_publishing_and_not_ready_without_the_bus() -> None:
    bus = make_bus()
    async with running(FakeSource(raws(5)), bus) as client:
        await published(client, 5)
        response = await client.get("/health/ready")
        assert response.status_code == 200
        body = response.json()
        assert body["checks"] == {
            "producer": True,
            "publisher": True,
            "source": True,
            "bus": True,
            "fresh": True,
        }
        assert body["last_processed_at"] is not None

        class Gone:
            async def ping(self) -> bool:
                return False

            async def stats(self, group: str) -> None:
                raise redis_exceptions.ConnectionError("broker away")

        state = client.app_ref.state  # type: ignore[attr-defined]
        real, state.bus = state.bus, Gone()
        try:
            response = await client.get("/health/ready")
            stats_response = await client.get("/stats")
        finally:
            state.bus = real
        assert response.status_code == 503 and response.json()["checks"]["bus"] is False
        assert stats_response.status_code == 503
        assert stats_response.json()["detail"].startswith("bus unreachable: ConnectionError")
        assert (await client.get("/health/live")).json() == {"status": "alive"}


class FlakyBus:
    """A bus whose first ``failures`` publishes fail as a broker outage would."""

    def __init__(self, inner: RedisStreamsBus, failures: int) -> None:
        self._inner = inner
        self.failures = failures

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def publish(self, events: list[RawEvent]) -> list[str]:
        if self.failures:
            self.failures -= 1
            raise redis_exceptions.ConnectionError("broker away")
        return await self._inner.publish(events)


async def test_publish_retries_until_the_broker_is_back_and_nothing_is_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pulsestream.ingest.publisher.delay_for", lambda *a, **k: 0.001)
    inner = make_bus()
    bus = FlakyBus(inner, failures=2)
    errors = metrics.ERRORS.labels(stage="bus")._value.get()
    events = raws(7)
    async with running(FakeSource(events), bus) as client:
        stats = await published(client, 7)
    assert stats["published_total"] == 7 and stats["publish_failures"] == 2
    assert metrics.ERRORS.labels(stage="bus")._value.get() - errors == 2
    assert [d.event for d in [x async for x in inner.replay()]] == events, "order kept, no loss"


async def test_not_ready_when_the_source_is_disconnected_or_quiet() -> None:
    class Disconnected(FakeSource):
        connected = False

    async with running(Disconnected(raws(2)), make_bus()) as client:
        await published(client, 2)
        body = (await client.get("/health/ready")).json()
        assert body["ready"] is False and body["checks"]["source"] is False

    async with running(FakeSource(raws(2)), make_bus(), ready_max_silence_seconds=0) as client:
        await published(client, 2)
        await asyncio.sleep(0.02)
        response = await client.get("/health/ready")
        assert response.status_code == 503 and response.json()["checks"]["fresh"] is False


async def test_shutdown_flushes_the_partial_batch() -> None:
    bus = make_bus()
    events = raws(5)
    async with running(FakeSource(events), bus, batch_size=100, flush_interval=30.0) as client:
        await asyncio.sleep(0.05)
        assert (await client.get("/stats")).json()["published_total"] == 0, "still buffered"
    assert [d.event for d in [x async for x in bus.replay()]] == events, "flushed on shutdown"
