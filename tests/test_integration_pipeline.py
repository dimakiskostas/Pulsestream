"""FakeSource -> queue -> consumer -> DuckDB against the committed replay fixture.

Tutorial Part 2.3. The fixture is ~20 s of the live recentchange stream captured on
2026-10-11 with `pulsestream capture -n 500`. Its composition is pinned below so a
change in the filter (ADR 0001) or a silently truncated fixture fails loudly.
"""

import asyncio
import json
from collections import Counter
from datetime import timedelta
from pathlib import Path

import duckdb

from pulsestream.models import RawEvent, WikiEvent
from pulsestream.pipeline import run_consumer, run_producer
from pulsestream.processing.processor import StreamProcessor
from pulsestream.sources.fake import FakeSource, load_fixture

FIXTURE = Path(__file__).parent / "fixtures" / "sample_events.jsonl"

# 500 raw events = 295 edit + 11 new (kept) + 164 categorize + 30 log (dropped)
EXPECTED_TYPES = {"edit": 295, "new": 11, "categorize": 164, "log": 30}
EXPECTED_KEPT = 306
EXPECTED_BOTS = 128
EXPECTED_TOP_WIKI = ("wikidatawiki", 112)


def fixture() -> list[RawEvent]:
    return load_fixture(FIXTURE)


def test_fixture_composition_matches_the_filter_contract() -> None:
    raws = fixture()
    assert len(raws) == 500
    assert Counter(json.loads(r.raw_payload)["type"] for r in raws) == EXPECTED_TYPES
    kept = [e for e in (WikiEvent.from_raw(r) for r in raws) if e is not None]
    assert len(kept) == EXPECTED_KEPT
    assert len({e.event_id for e in kept}) == EXPECTED_KEPT, "meta.id is globally unique"
    assert all(e.event_ts.tzinfo is not None and e.received_at.tzinfo is not None for e in kept)


async def test_pipeline_persists_exactly_the_valid_events(con: duckdb.DuckDBPyConnection) -> None:
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    await run_producer(FakeSource(fixture()), queue)  # finite source -> completes
    await queue.put(None)  # sentinel stops the consumer

    await run_consumer(queue, StreamProcessor(con), batch_size=50, flush_interval=60.0)

    total, distinct = con.execute(
        "SELECT count(*), count(DISTINCT event_id) FROM wiki_events"
    ).fetchone() or (0, 0)
    assert (total, distinct) == (EXPECTED_KEPT, EXPECTED_KEPT)
    assert con.execute("SELECT count(*) FROM wiki_events WHERE is_bot").fetchone() == (
        EXPECTED_BOTS,
    )
    assert con.execute("SELECT count(*) FROM wiki_events WHERE event_type = 'new'").fetchone() == (
        11,
    )
    top = con.execute(
        "SELECT entity, count(*) AS n FROM wiki_events "
        "GROUP BY entity ORDER BY n DESC, entity LIMIT 1"
    ).fetchone()
    assert top == EXPECTED_TOP_WIKI
    # Ingest lag is non-negative and small: the sample was captured live.
    min_lag, max_lag = con.execute(
        "SELECT min(received_at - event_ts), max(received_at - event_ts) FROM wiki_events"
    ).fetchone() or (None, None)
    assert min_lag >= timedelta(0)
    assert max_lag < timedelta(minutes=5)


async def test_replaying_the_fixture_twice_is_idempotent(con: duckdb.DuckDBPyConnection) -> None:
    """At-least-once delivery + dedupe on event_id (HLD decision D2)."""
    for _ in range(2):
        queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
        await run_producer(FakeSource(fixture()), queue)
        await queue.put(None)
        await run_consumer(queue, StreamProcessor(con), batch_size=50, flush_interval=60.0)
    assert con.execute("SELECT count(*) FROM wiki_events").fetchone() == (EXPECTED_KEPT,)


async def test_bounded_queue_applies_backpressure_without_losing_events(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A tiny queue forces the producer to block on put; everything still arrives."""
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue(maxsize=8)
    producer = asyncio.create_task(run_producer(FakeSource(fixture()), queue))
    consumer = asyncio.create_task(
        run_consumer(queue, StreamProcessor(con), batch_size=7, flush_interval=0.05)
    )

    await asyncio.wait_for(producer, timeout=10)
    await queue.put(None)
    await asyncio.wait_for(consumer, timeout=10)

    assert con.execute("SELECT count(*) FROM wiki_events").fetchone() == (EXPECTED_KEPT,)
