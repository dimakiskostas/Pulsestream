# 2. In-process queue first, broker later; backpressure by blocking

## Status

Accepted (Phase 1). The broker arrives in Phase 5 and will supersede the
*transport* half of this record, not the backpressure policy.

## Context

The HLD (D1) prescribes an `asyncio.Queue` between ingestion and processing
for v1 and a real broker (Redpanda or Redis Streams) for v2. It also leaves
the backpressure policy open (§7, `DECIDE`): when the consumer is slower than
the source, do we block the producer or drop the oldest events?

The Wikimedia stream runs at roughly 50–150 events/s. DuckDB inserts of a
50-row batch take single-digit milliseconds, so in normal operation the
consumer is far faster than the source and the queue sits near empty. The
policy only matters during a stall (a slow query on the same connection, a
disk hiccup, a GC pause).

## Decision

**Transport:** one process, `asyncio.Queue(maxsize=PULSE_QUEUE_MAXSIZE)`
(default 1000). The producer task awaits `queue.put`, the consumer task
awaits `queue.get` with a timeout (see ADR 0004).

**Backpressure: block.** A full queue makes `put` wait, which pauses the
SSE read loop, which makes the TCP window close, which makes Wikimedia's
edge buffer on its side. Nothing is dropped in-process. If the stall lasts
long enough for the server to disconnect us, the source reconnects with
`Last-Event-ID` and the server replays from where we stopped (ADR 0005).

## Alternatives considered

**Drop oldest.** Keeps the pipeline "live" at the cost of silent data loss
exactly when something is wrong, with no record of what was lost. Rejected:
the HLD goal is "no data loss under normal operation", and lost events would
corrupt every rate-based feature the detector relies on.

**Unbounded queue.** Hides the stall until memory runs out. Rejected.

**Broker from day one.** Correct eventually, but it moves broker operations
(a second container, consumer groups, offsets) in front of the first working
end-to-end slice. Rejected for v1 as the HLD argues.

## Consequences

- Queue depth is a first-class health signal: `/health` reports it now and
  it becomes a Prometheus gauge in Phase 4.
- The queue boundary is the seam the broker will replace: `run_producer`
  and `run_consumer` only know `put`/`get`, not each other.
- A permanently slow consumer shows up as a steadily growing `lag_seconds`,
  not as dropped events.

## Revisit if

- Processing becomes CPU-bound (model inference in the consumer): blocking
  the reader would then throttle ingestion; the broker in Phase 5 decouples
  them properly.
