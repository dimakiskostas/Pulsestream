"""The one place that knows which concrete source and bus are wired in.

Everything else depends on the ``EventSource`` and ``EventBus`` shapes; the
settings decide here which implementation stands behind them.
"""

from __future__ import annotations

import socket
from datetime import timedelta

from pulsestream.bus.redis import RedisStreamsBus
from pulsestream.config import Settings
from pulsestream.sources.base import EventSource
from pulsestream.sources.fake import FakeSource, load_fixture
from pulsestream.sources.wikimedia import WikimediaSource


def build_source(cfg: Settings) -> EventSource:
    if cfg.source == "fake":
        return FakeSource(load_fixture(cfg.fixture_path), delay=cfg.fixture_delay)
    return WikimediaSource(cfg.stream_url, user_agent=cfg.user_agent, read_timeout=cfg.read_timeout)


def build_bus(cfg: Settings) -> RedisStreamsBus:
    return RedisStreamsBus(
        cfg.redis_url,
        stream=cfg.bus_stream,
        retention=timedelta(minutes=cfg.bus_retention_minutes),
        count=cfg.bus_read_count,
        block_ms=cfg.bus_block_ms,
        claim_idle=timedelta(seconds=cfg.bus_claim_idle_seconds),
    )


def consumer_name(cfg: Settings) -> str:
    """The name this process acknowledges under: stable across restarts of the same host."""
    return cfg.bus_consumer or socket.gethostname()
