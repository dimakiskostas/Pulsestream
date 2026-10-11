"""The per-batch processing step: persist raw, validate, persist typed.

``StreamProcessor`` is the ``Processor`` the consumer loop drives. Keeping it
separate from the loop means the loop only knows about batching and the queue,
while everything that happens to a batch (the raw lake, validation, DuckDB and,
in later phases, rolling features and the anomaly detector) lives here and is
testable with a plain list of events.
"""

from pathlib import Path
from typing import Protocol

import duckdb
import structlog

from pulsestream.models import RawEvent, WikiEvent
from pulsestream.pipeline import BatchResult
from pulsestream.storage.duck import write_batch

log = structlog.get_logger(__name__)


class RawStore(Protocol):
    def write_batch(self, raws: list[RawEvent]) -> list[Path]: ...


class StreamProcessor:
    def __init__(self, con: duckdb.DuckDBPyConnection, raw_store: RawStore | None = None) -> None:
        self._con = con
        self._raw_store = raw_store

    def process(self, raws: list[RawEvent]) -> BatchResult:
        if self._raw_store is not None:
            # Raw first, unfiltered: the lake holds exactly what arrived, so a changed
            # filter is a replay from disk rather than a re-ingest.
            self._raw_store.write_batch(raws)
        events = [e for e in (WikiEvent.from_raw(r) for r in raws) if e is not None]
        write_batch(self._con, events)
        log.debug("batch.processed", raw=len(raws), kept=len(events))
        return BatchResult(raw=len(raws), kept=len(events))
