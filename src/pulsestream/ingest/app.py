"""The ingest service: source → bounded queue → bus, with probes and metrics.

It has the same shape as the API process (a FastAPI lifespan owns the tasks), so
the container, the probes and ``/metrics`` work the same way, but it serves no
data: ``pulsestream ingest`` is a producer and nothing else. Killing it stops
new events from arriving; processing keeps running on what is in the bus, and
when it comes back the stream resumes.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI, Request, Response

from pulsestream import __version__
from pulsestream.api.schemas import IngestStats, LivenessStatus, ReadinessStatus
from pulsestream.bus.base import EventBus
from pulsestream.common import metrics
from pulsestream.config import Settings
from pulsestream.config import settings as default_settings
from pulsestream.ingest.publisher import Publisher
from pulsestream.models import RawEvent
from pulsestream.pipeline import run_batches, run_producer
from pulsestream.sources.base import EventSource
from pulsestream.wiring import build_bus, build_source


def create_ingest_app(
    cfg: Settings | None = None, source: EventSource | None = None, bus: EventBus | None = None
) -> FastAPI:
    cfg = cfg if cfg is not None else default_settings

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        src = source if source is not None else build_source(cfg)
        the_bus: EventBus = bus if bus is not None else build_bus(cfg)
        queue: asyncio.Queue[RawEvent | None] = asyncio.Queue(maxsize=cfg.queue_maxsize)
        publisher = Publisher(the_bus)
        producer = asyncio.create_task(run_producer(src, queue), name="producer")
        publishing = asyncio.create_task(
            run_batches(queue, publisher.publish, cfg.batch_size, cfg.flush_interval),
            name="publisher",
        )
        app.state.started_at = datetime.now(UTC)
        app.state.source = src
        app.state.bus = the_bus
        app.state.queue = queue
        app.state.publisher = publisher
        app.state.producer = producer
        app.state.publishing = publishing
        try:
            yield
        finally:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            if not publishing.done():
                try:
                    await asyncio.wait_for(queue.put(None), timeout=1.0)
                    await asyncio.wait_for(publishing, timeout=cfg.flush_interval + 5.0)
                except TimeoutError:
                    publishing.cancel()
                    await asyncio.gather(publishing, return_exceptions=True)
            if bus is None:  # we built it, we close it
                await the_bus.close()

    app = FastAPI(title="PulseStream ingest", version=__version__, lifespan=lifespan)

    @app.get("/health/live", response_model=LivenessStatus)
    async def live() -> LivenessStatus:
        return LivenessStatus(status="alive")

    @app.get("/health/ready", response_model=ReadinessStatus)
    async def ready(request: Request, response: Response) -> ReadinessStatus:
        """Source connected, bus answering, both tasks alive, something published lately."""
        state = request.app.state
        now = datetime.now(UTC)
        published_at = state.publisher.last_published_at
        since = published_at if published_at is not None else state.started_at
        silence = (now - since).total_seconds()
        producer = state.producer
        producer_failed = (
            producer.done() and not producer.cancelled() and producer.exception() is not None
        )
        checks = {
            "producer": not producer_failed,
            "publisher": not state.publishing.done(),
            "source": bool(getattr(state.source, "connected", True)),
            "bus": await state.bus.ping(),
            "fresh": silence <= cfg.ready_max_silence_seconds,
        }
        is_ready = all(checks.values())
        response.status_code = 200 if is_ready else 503
        return ReadinessStatus(
            ready=is_ready,
            checks=checks,
            last_processed_at=published_at,
            silence_seconds=round(silence, 3),
        )

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        body, content_type = metrics.render()
        return Response(content=body, media_type=content_type)

    @app.get("/stats", response_model=IngestStats)
    async def stats(request: Request) -> IngestStats:
        """What this process has pushed into the bus, and what the bus holds."""
        state = request.app.state
        bus_stats = await state.bus.stats(cfg.bus_group)
        return IngestStats(
            published_total=state.publisher.published_total,
            batches=state.publisher.batches,
            publish_failures=state.publisher.failures,
            last_published_at=state.publisher.last_published_at,
            queue_depth=state.queue.qsize(),
            stream_length=bus_stats.length,
            stream_first_id=bus_stats.first_id,
            stream_last_id=bus_stats.last_id,
        )

    return app


app = create_ingest_app()
