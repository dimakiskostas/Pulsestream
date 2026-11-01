"""Replay the raw lake through the same pipeline as the live stream.

The lake is the immutable source of truth (HLD §7, ADR 0008); ``ParquetSource``
turns it back into an ``EventSource``, so a replay is the live pipeline with a
different source: same validation, same storage, same aggregates (ADR 0009).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from uuid import UUID

import duckdb

from pulsestream.models import RawEvent
from pulsestream.storage.parquet import ParquetRawStore

_COLUMNS = "ingest_id, source, received_at, schema_version, raw_payload"


class ParquetSource:
    """Stream the lake's raw events in ``received_at`` order, optionally within a window.

    Reads go through a private DuckDB connection; ``[since, until)`` filters on
    ``received_at`` are pushed down to Parquet row-group statistics. Rows are fetched
    in chunks on a worker thread so a large replay keeps memory flat and the event
    loop free.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        chunk_size: int = 1000,
    ) -> None:
        self._store = ParquetRawStore(root)
        self._since = since
        self._until = until
        self._chunk_size = chunk_size

    async def stream(self) -> AsyncIterator[RawEvent]:
        if not self._store.files():
            return
        glob = self._store.glob.replace("'", "''")
        clauses: list[str] = []
        params: list[datetime] = []
        if self._since is not None:
            clauses.append("received_at >= ?")
            params.append(self._since)
        if self._until is not None:
            clauses.append("received_at < ?")
            params.append(self._until)
        where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
        sql = (
            f"SELECT {_COLUMNS} FROM read_parquet('{glob}', hive_partitioning=false) "
            f"{where}ORDER BY received_at, ingest_id"
        )
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'")
            cur = con.execute(sql, params)
            while rows := await asyncio.to_thread(cur.fetchmany, self._chunk_size):
                for ingest_id, source, received_at, schema_version, raw_payload in rows:
                    yield RawEvent(
                        ingest_id=UUID(ingest_id),
                        source=source,
                        received_at=received_at,
                        schema_version=schema_version,
                        raw_payload=raw_payload,
                    )
        finally:
            con.close()
