# 4. Micro-batch writes, flushed by size or time

## Status

Accepted (Phase 1).

## Context

The HLD (D4, §7 `DECIDE: micro-batch cadence`) prefers micro-batches over
per-event writes: columnar stores and Parquet files pay a fixed cost per
write, so one row per write destroys throughput and, in Phase 2, produces
thousands of tiny Parquet files. The tutorial's first consumer flushed only
when the batch was full, which means a quiet stream could hold events in
memory indefinitely.

## Decision

The consumer flushes a batch when **either** condition holds:

- the batch has reached `PULSE_BATCH_SIZE` events (default 50), or
- `PULSE_FLUSH_INTERVAL` seconds (default 2.0) have passed since the last
  flush and the batch is non-empty.

Implementation: `queue.get()` is wrapped in `asyncio.wait_for` with the time
remaining until the next deadline; a `TimeoutError` triggers a flush. A
`None` sentinel on the queue flushes and stops the consumer (used by tests
and by the API's shutdown). Empty batches are never written: DuckDB rejects
an empty `executemany`, and the first version of the time-based flush crashed
the consumer on an idle stream for exactly that reason.

Defaults: at the observed 50–150 events/s a 50-row batch fills in well under
a second, so the size rule dominates in normal operation and the 2 s rule
bounds latency when the stream is quiet. Both are settings, not constants.

## Alternatives considered

**A separate flusher task on a timer.** Equivalent behaviour with a second
task to coordinate (shared batch, locking). The timeout-on-`get` form keeps
one owner for the batch.

**Per-event writes.** Simplest code, worst I/O profile, and incompatible
with the Parquet raw store of Phase 2.

**Larger batches (500+).** Better I/O still, but visible latency on the
dashboard and more events at risk in a crash before the broker exists.

## Consequences

- End-to-end latency is bounded by `flush_interval` plus the write.
- Every flush is one DuckDB statement; the batch size is the knob that
  trades latency for write cost and later Parquet file size.
- Tests assert exact batch boundaries ([5, 5, 2] for 12 events at size 5)
  so a regression in the flush logic cannot hide behind idempotent writes.
