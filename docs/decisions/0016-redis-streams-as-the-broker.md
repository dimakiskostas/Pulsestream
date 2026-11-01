# 16. Redis Streams as the broker between ingestion and processing

## Status

Accepted (Phase 5). Builds on ADR 0002 (in-process queue first, broker later).

## Context

The HLD's v2 buffer (§3, §7, §13 Phase 5) is "Redpanda or Redis Streams":
split ingestion and processing into separate services, with consumer groups
and replay, and decide the partition key and the retention. Phase 5's finish
line is concrete: killing and restarting ingestion must not stop processing,
and a past window must be replayable from the broker.

What has to flow is small: about 30 events a second at Wikimedia's busiest
(1.3 KB each), consumed by exactly one process, the DuckDB writer (ADR 0003).
There is one producer and one consumer, and the pipeline's shape (source →
queue → micro-batches → processor) already isolates the boundary.

## Decision

- **Redis Streams.** One stream, `pulsestream:events`, carries every
  envelope in publish order; one consumer group, `processors`, with one
  consumer, the API process, reads it. Redis runs with AOF persistence so a
  restart of the broker keeps the stream.
- **Retention is by time, not size**: every publish trims entries older than
  `PULSE_BUS_RETENTION_MINUTES` (360) with `MINID`. Six hours covers a
  processor outage and the "what happened between 10:00 and 10:30" replay;
  the lake remains the long-term source (ADR 0008).
- **No partition key, by design.** Redis Streams have no partitions; one
  stream keeps total order, which the detectors rely on per entity, and the
  single writer could not use more parallelism anyway. The documented path to
  scale out is N streams keyed by `hash(entity) % N`, one consumer each,
  which keeps per-entity order; nothing in the code assumes one stream.
- **The seam stays abstract.** `EventBus` (publish, subscribe, ack, replay,
  stats) is the only shape the services depend on; `RedisStreamsBus` is one
  implementation, tested on fakeredis.

## Alternatives considered

**Redpanda (Kafka API).** The industry answer: partitions, offsets, size
and time retention, a connector ecosystem. For one producer, one consumer
and 30 events a second it is a 500 MB image, a topic to manage and a client
library whose idioms (rebalances, commits) buy nothing here. If a Kafka
ecosystem becomes the goal, the `EventBus` seam contains the swap to one
module.

**NATS JetStream.** A good technical fit (streams, durable consumers,
replay by time) and lighter than Redpanda; chosen against because Redis is
the more widely known tool and its streams model maps one-to-one onto the
needs: pending entries, claims, time-indexed ids.

**RabbitMQ.** Queues, not logs: a consumed message is gone, so no replay by
offset or time.

**Keep the in-process queue.** No decoupling: an ingestion crash is a
processing crash. The whole point of Phase 5.

## Consequences

- Redis is a new runtime dependency and the stream lives in memory: at the
  defaults, six hours of the busiest stream is roughly
  6 h × 30/s × 1.3 KB ≈ 0.8 GB. Retention is the knob; `/bus` and the
  `pulsestream_bus_stream_length` gauge show the size.
- Exactly one consumer per group. A second API process joining `processors`
  would split entities between the two and break the per-entity windows;
  scale-out is the N-streams path above, never a second consumer.
- The tests need no broker: fakeredis implements every command used
  (`XADD` with `MINID`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`,
  `XRANGE`, `XINFO`). Found while testing: fakeredis answers a blocking
  read at once, which turned the subscribe loop into a busy loop that
  starved the worker thread writing DuckDB; an idle read now pauses 10 ms,
  which a real broker's `BLOCK` makes irrelevant.

## Revisit if

- The event rate grows by an order of magnitude (memory), or more than one
  processing service is needed (partitioned streams, or Redpanda).
- A Kafka-connected ecosystem (connectors, schema registry) becomes a goal.
