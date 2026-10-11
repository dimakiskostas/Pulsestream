"""Partitioned Parquet raw store: the immutable, replayable source of truth (HLD §7).

Layout, hive-style so DuckDB reads it with ``hive_partitioning=true``::

    <root>/dt=YYYY-MM-DD/hour=HH/part-<received_ms>-<uid>.parquet    one file per micro-batch
    <root>/dt=YYYY-MM-DD/hour=HH/merged-<received_ms>-<uid>.parquet  produced by compaction

Partition keys come from ``received_at`` in UTC, i.e. when *we* got the event: it is
the write-time clock, monotone and under our control. ``event_ts`` (when it
happened) can be late and belongs to the data, not to the layout.

Every file is written to a ``.tmp`` sibling and renamed into place, so a reader that
globs ``*.parquet`` never sees a half-written file.
"""

from __future__ import annotations

import os
import uuid
from collections import defaultdict
from datetime import UTC
from pathlib import Path

import polars as pl

from pulsestream.models import RawEvent

SCHEMA = pl.Schema(
    {
        "ingest_id": pl.Utf8(),
        "source": pl.Utf8(),
        "received_at": pl.Datetime("us", "UTC"),
        "schema_version": pl.Int32(),
        "raw_payload": pl.Utf8(),
    }
)


class ParquetRawStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    @property
    def glob(self) -> str:
        """A glob DuckDB understands (forward slashes, also on Windows)."""
        return (self.root / "**" / "*.parquet").as_posix()

    def files(self) -> list[Path]:
        return sorted(self.root.rglob("*.parquet")) if self.root.exists() else []

    def partitions(self) -> list[Path]:
        """The ``dt=.../hour=...`` directories, sorted."""
        if not self.root.exists():
            return []
        return sorted(p for p in self.root.glob("dt=*/hour=*") if p.is_dir())

    def write_batch(self, raws: list[RawEvent]) -> list[Path]:
        """Append one micro-batch. Returns the files written: one per hour partition touched."""
        groups: dict[tuple[str, str], list[RawEvent]] = defaultdict(list)
        for raw in raws:
            ts = raw.received_at.astimezone(UTC)
            groups[(ts.strftime("%Y-%m-%d"), ts.strftime("%H"))].append(raw)
        written: list[Path] = []
        for (dt, hour), rows in sorted(groups.items()):
            first_ms = int(min(r.received_at for r in rows).timestamp() * 1000)
            name = f"part-{first_ms}-{uuid.uuid4().hex[:8]}.parquet"
            target = self.root / f"dt={dt}" / f"hour={hour}" / name
            write_frame(target, to_frame(rows))
            written.append(target)
        return written


def to_frame(rows: list[RawEvent]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ingest_id": [str(r.ingest_id) for r in rows],
            "source": [r.source for r in rows],
            "received_at": [r.received_at.astimezone(UTC) for r in rows],
            "schema_version": [r.schema_version for r in rows],
            "raw_payload": [r.raw_payload for r in rows],
        },
        schema=SCHEMA,
    )


def write_frame(target: Path, frame: pl.DataFrame) -> None:
    """Write atomically: ``.tmp`` sibling first, then an in-place rename."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    frame.write_parquet(tmp, compression="zstd")
    os.replace(tmp, target)
