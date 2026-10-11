# 3. DuckDB as the analytical store; one writer process; UTC sessions

## Status

Accepted (Phase 1).

## Context

The HLD (D3) chooses DuckDB over a server warehouse: embedded, zero ops,
excellent single-node analytics, with the known limits of no concurrency
across processes and a practical ceiling well below our data volume. Two
operational facts shape how it is used here:

1. **One read-write process.** A database file opened read-write holds an
   exclusive lock; other processes may open it `read_only=True` only while
   no writer has it open. Within the writer process, a connection may be
   shared across threads only through `con.cursor()`.
2. **Session time zone.** `TIMESTAMPTZ` values are rendered in the
   connection's `TimeZone` setting, which defaults to the host's local
   zone. On the development machine a value stored as 09:18 UTC came back
   as 12:18 Europe/Athens: the same instant, a different wall clock. The
   conversion also needs the `pytz` package at run time, which is why it is
   an explicit dependency although nothing imports it.

## Decision

- `wiki_events` lives in a DuckDB file (`PULSE_DB_PATH`, default
  `pulsestream.duckdb`); the consumer task is the only writer.
- Every connection is opened through `storage.duck.connect()`, which runs
  `SET TimeZone = 'UTC'` and, for writers, creates the schema. The project
  convention is "all timestamps are UTC and timezone-aware" (HLD §8); the
  session setting makes that true at the boundary rather than at every call site.
- The API serves reads from the same process as the writer in Phase 1 (one
  connection on the event loop; queries are tiny). When reads become heavy
  they move to `asyncio.to_thread` with a per-request `con.cursor()`.

## Alternatives considered

**PostgreSQL / a server warehouse.** Concurrency and multi-process access
for the price of running a database server. Out of scope for a single-node
project whose analytical workload DuckDB handles better anyway.

**SQLite.** Transactional, but row-oriented: the windowed aggregations and
Parquet-backed queries of Phase 2 are exactly what a columnar engine exists for.

**Converting timestamps in Python instead of pinning the session.** Works
until one call site forgets. A session-level setting cannot be forgotten.

## Consequences

- The Phase 5 split must keep the DuckDB writer and the API in one process
  (or give the API read-only access only when no writer is open). This is a
  real constraint on the service topology and is recorded here so it is not
  rediscovered the hard way.
- Tests use `connect(":memory:")` and get the same schema and session.

## Revisit if

- Several processes need to write (a second consumer, a backfill job running
  alongside the live consumer): then either serialise writes through one
  process or move the derived tables to a server database.
