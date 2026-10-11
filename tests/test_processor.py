from datetime import UTC, datetime
from pathlib import Path

import duckdb

from pulsestream.models import RawEvent
from pulsestream.pipeline import BatchResult
from pulsestream.processing.processor import StreamProcessor
from tests.factories import _raw, create_payload


def mixed_batch() -> list[RawEvent]:
    """Two kept events, one dropped type, one malformed payload."""
    return [
        _raw(create_payload(meta={"id": "keep-1"})),
        _raw(create_payload(meta={"id": "drop-log"}, type="log")),
        RawEvent(source="test", received_at=datetime.now(UTC), schema_version=1, raw_payload="x"),
        _raw(create_payload(meta={"id": "keep-2"}, type="new")),
    ]


def test_processor_validates_persists_and_counts(con: duckdb.DuckDBPyConnection) -> None:
    result = StreamProcessor(con).process(mixed_batch())

    assert result == BatchResult(raw=4, kept=2)
    ids = [r[0] for r in con.execute("SELECT event_id FROM wiki_events ORDER BY 1").fetchall()]
    assert ids == ["keep-1", "keep-2"]


def test_processor_is_idempotent_across_batches(con: duckdb.DuckDBPyConnection) -> None:
    processor = StreamProcessor(con)
    batch = [_raw(create_payload(meta={"id": "same"}))]
    assert processor.process(batch) == BatchResult(raw=1, kept=1)
    assert processor.process(batch) == BatchResult(raw=1, kept=1), "kept counts validated events"
    assert con.execute("SELECT count(*) FROM wiki_events").fetchone() == (1,)


def test_processor_handles_an_empty_batch(con: duckdb.DuckDBPyConnection) -> None:
    assert StreamProcessor(con).process([]) == BatchResult(raw=0, kept=0)


class RecordingStore:
    """A RawStore that remembers every batch it was handed."""

    def __init__(self) -> None:
        self.batches: list[list[RawEvent]] = []

    def write_batch(self, raws: list[RawEvent]) -> list[Path]:
        self.batches.append(list(raws))
        return []


def test_processor_hands_every_raw_event_to_the_raw_store(
    con: duckdb.DuckDBPyConnection,
) -> None:
    store = RecordingStore()
    raws = mixed_batch()

    result = StreamProcessor(con, store).process(raws)

    assert result == BatchResult(raw=4, kept=2)
    assert store.batches == [raws], "dropped and malformed events reach the raw store too"
    assert con.execute("SELECT count(*) FROM wiki_events").fetchone() == (2,)
