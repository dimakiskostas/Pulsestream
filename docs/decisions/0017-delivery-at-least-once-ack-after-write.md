# 17. Delivery across the bus: at-least-once, acknowledge after the write, replay the recent window from the bus

## Status

Accepted (Phase 5). Extends ADR 0005 (at-least-once with idempotent writes) to
the split pipeline.

## Context

The tutorial's Phase 5 gotcha is "decide your delivery semantics explicitly",
and the HLD's D2 and non-goals rule out exactly-once. With a broker between
two services the failure points multiply: the ingest service can die with
events in its queue, the broker can be away, the processor can die between
reading a batch and writing it, and an acknowledgement can fail after the
write. Each needs an answer, and the answers must add up to "no row lost
under normal operation, no row doubled".

## Decision

**Publishing.** The ingest service publishes micro-batches. A failed publish
is retried with backoff while the batch is held; the queue behind it fills and
the source blocks (the Phase 1 backpressure), so a broker outage drops
nothing. An ingest crash loses what was in its queue (at most
`PULSE_QUEUE_MAXSIZE` events) and its in-memory resume cursor; v1 accepts
that, and persisting the Wikimedia `Last-Event-ID` is the obvious follow-up.

**Consuming.** The API process reads through the consumer group and
acknowledges a batch only after its rows, features and findings are written,
never before. A crash in between leaves the batch pending; the restarted
process, under the same consumer name, re-reads its own pending entries
first, and a consumer that never comes back has its entries claimed by the
next one after `PULSE_BUS_CLAIM_IDLE_SECONDS` (60). The redelivered rows are
no-ops: `wiki_events` has a PRIMARY KEY on `event_id`, anomaly ids are
deterministic, feature rows are replaced. An acknowledgement that fails is
kept and retried with the next one, because an unacknowledged entry costs a
redelivery while a dead consumer costs the pipeline.

**The consumer name is fixed in deployments** (`PULSE_BUS_CONSUMER=api` in
compose): a recreated container must find its own pending entries at once,
not after the claim timeout.

**Replay.** `pulsestream replay --from-bus` reads a `[since, until)` window
of the stream by entry id, outside any group, into a fresh database; the lake
serves anything older than the bus retention. A replay never acknowledges
anything.

**Not exactly-once.** No transactional outbox, no two-phase anything: DuckDB
and Redis do not share a transaction, and the idempotent writes make the
duplicates harmless where it matters.

## Alternatives considered

**Acknowledge on read (at-most-once).** Simplest and wrong: a crash after the
read loses the batch for good, exactly what Phase 5 exists to prevent.

**Exactly-once through an outbox or offsets stored in DuckDB.** Would need
the acknowledgement and the rows in one transaction across two systems.
Idempotent writes give the same observable result for a fraction of the
machinery.

**One consumer per CPU.** Would parallelise processing, and split a wiki's
events across consumers, breaking the per-entity windows (ADR 0010). One
consumer per group; scale-out is a partitioned stream (ADR 0016).

## Consequences

- The raw lake is append-only, so a redelivered batch is written to Parquet
  twice; `raw_events` can hold duplicate `ingest_id`s after a crash, and a
  lake replay dedupes on write like everything else. The
  `pulsestream_bus_redeliveries_total` counter says how often it happened.
- `pending` above zero for longer than a batch takes means a stalled
  processor; the `PulseStreamBusPendingHigh` rule says so. `lag` is the
  backlog the processor has not been handed yet.
- Detector state (open windows, baselines) still lives in memory and is
  lost on a restart, with or without the bus. The bus fixes data loss, not
  state loss; a state snapshot is a separate piece of work.
- A second API process against the same group is a configuration error,
  not a scale-out.
