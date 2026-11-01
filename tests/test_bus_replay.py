"""A past window of the stream can be replayed into a fresh database, from the bus."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import fakeredis.aioredis

from pulsestream.bus.redis import RedisStreamsBus
from pulsestream.cli import build_parser
from pulsestream.config import Settings
from pulsestream.models import WikiEvent
from pulsestream.replay import replay
from pulsestream.sources.bus import BusReplaySource
from pulsestream.sources.fake import load_fixture
from pulsestream.storage.duck import connect


async def no_sleep(_: float) -> None:
    return None


def make_bus() -> RedisStreamsBus:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisStreamsBus(client=client, block_ms=10, sleep=no_sleep)


async def test_replay_from_the_bus_rebuilds_the_rows_of_a_window(tmp_path: Path) -> None:
    bus = make_bus()
    events = load_fixture("tests/fixtures/sample_events.jsonl")
    early, late = events[:200], events[200:]
    await bus.publish(early)
    await asyncio.sleep(0.005)
    cut = datetime.now(UTC)
    await asyncio.sleep(0.005)
    await bus.publish(late)
    cfg = Settings(_env_file=None, detection_enabled=False)

    window = await replay(
        tmp_path / "unused-lake",
        tmp_path / "window.duckdb",
        source=BusReplaySource(bus, since=cut),
        cfg=cfg,
    )
    kept_late = sum(1 for e in late if WikiEvent.from_raw(e) is not None)
    assert window.raw == len(late) and window.kept == kept_late and window.rows == kept_late

    everything = await replay(
        tmp_path / "unused-lake",
        tmp_path / "all.duckdb",
        source=BusReplaySource(bus),
        cfg=cfg,
    )
    kept_all = sum(1 for e in events if WikiEvent.from_raw(e) is not None)
    assert everything.raw == len(events) and everything.rows == kept_all
    con = connect(tmp_path / "all.duckdb", read_only=True)
    try:
        assert (
            con.execute("SELECT count(DISTINCT event_id) FROM wiki_events").fetchone()[0]
            == kept_all
        )
    finally:
        con.close()
    assert (await bus.stats("processors")).pending == 0, "a replay never joins the group"


def test_the_replay_command_can_read_the_bus() -> None:
    args = build_parser().parse_args(
        [
            "replay",
            "--from-bus",
            "--redis-url",
            "redis://broker:6379/1",
            "--since",
            "2026-11-01T12:00:00",
        ]
    )
    assert args.from_bus is True and args.redis_url == "redis://broker:6379/1"
    assert args.since == datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
    assert build_parser().parse_args(["replay"]).from_bus is False
