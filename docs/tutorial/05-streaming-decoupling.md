# 05 · Streaming decoupling (Phase 5)

> Goal (HLD §13, tutorial Phase 5, "the real milestone"): survive an ingestion
> crash without losing the processing pipeline, and be able to replay. Put a
> broker between ingestion and processing; ingestion becomes a producer,
> processing a consumer group; decide the delivery semantics explicitly.

Branch: `phase-5-streaming-decoupling`, 8 commits on top of Phase 4. Steps
below are the commits in order (`git log --oneline main..phase-5-streaming-decoupling`
once Phase 4 is merged).

## The system after Phase 5

```
pulsestream ingest                                  Redis Stream                 pulsestream api
┌────────────────────────────────────────┐          pulsestream:events           ┌──────────────────────────────────────────────┐
│ WikimediaSource ─▶ queue ─▶ Publisher ─┼─XADD────▶ ┃ id = publish time ┃ ◀──XREADGROUP (group processors, consumer "api")  │
│   (reconnect, resume)   (retry+backoff)│          ┃ MINID trim: 6 h   ┃      BusSource ─▶ queue ─▶ consumer ─▶ StreamProcessor │
│ /health/live /health/ready /metrics    │          ┃ AOF persistence   ┃                                  └─▶ XACK after the write │
│ /stats                                 │          ┗━━━━━━━━━━━━━━━━━━━┛      lake, DuckDB, detectors, scheduler, every endpoint │
└────────────────────────────────────────┘                   ▲                 + /bus                                              │
                                                              │                └──────────────────────────────────────────────┘
                                    pulsestream replay --from-bus (XRANGE by time, no group)
```

`PULSE_BUS=none` (the default outside containers) keeps the Phase 1–4
single process; `PULSE_BUS=redis` is what `docker compose` runs. New modules:
`bus/` (`base.py`, `redis.py`, `monitor.py`), `ingest/` (`app.py`,
`publisher.py`), `sources/bus.py`, `wiring.py`, `common/backoff.py`. New
command: `pulsestream ingest`; new flag: `pulsestream replay --from-bus`.

## Decisions made here

- [ADR 0016](../decisions/0016-redis-streams-as-the-broker.md): Redis Streams
  over Redpanda, NATS or RabbitMQ; one stream, one group, one consumer;
  retention by time (`MINID`, six hours); no partition key, by design, with
  the N-streams path documented.
- [ADR 0017](../decisions/0017-delivery-at-least-once-ack-after-write.md):
  at-least-once; acknowledge after the write, never before; retried
  publishes and acknowledgements; a fixed consumer name in deployments;
  replay the recent window from the bus and anything older from the lake.

## Steps

### Step: *build: add redis and fakeredis; the EventBus protocol and its wire format*

What both sides agree on before any broker is chosen: `publish`,
`subscribe(group, consumer)`, `ack`, `replay(since, until)`, `stats`. The
wire format is the `RawEvent`'s five fields as strings, so any broker with
string fields carries it unchanged. The backoff helper moves out of the
Wikimedia source: the bus reconnects with the same policy.

### Step: *feat(bus): Redis Streams bus with consumer groups, acks, pending recovery and replay*

The whole broker story is in this module: entries delivered to a consumer
stay pending until acknowledged; a consumer that comes back under the same
name re-reads its own pending entries first; entries another consumer left
idle for 60 s are taken over through `XPENDING` and `XCLAIM`, never its own;
an entry that is not an envelope is acknowledged and skipped rather than
poisoning the group; a window is an `XRANGE` because entry ids are
millisecond timestamps.

**Gotcha: `XREADGROUP` with an explicit id is exclusive.** The re-read of a
consumer's pending entries walks them with a cursor. The first version set
the cursor to the id *after* the last entry, as `XRANGE` would want, and
skipped every second pending entry, then waited forever for the rest. The
restart test (three delivered, one acknowledged, a crash, a restart) caught
it; the cursor is the last id itself.

### Step: *feat(ingest): `pulsestream ingest` publishes the source to the bus, with probes and metrics*

The Phase 1 producer with a publisher behind the queue instead of the
processor. The micro-batch loop moves out of `run_consumer` into
`run_batches`, shared by both sides; a failed publish is retried while the
batch is held, the queue fills behind it and the source blocks: the Phase 1
backpressure, unchanged, now protecting the broker boundary. The service has
the same probes and `/metrics` as the API, plus `/stats`.

### Step: *feat(pipeline): the API consumes the bus when PULSE_BUS=redis and acknowledges after the write*

`BusSource` is an `EventSource` over the consumer group, so the producer task
reads it and nothing downstream changes. `run_consumer` gains an
`after_batch` hook, called once a batch's rows are written; that is where the
batch is acknowledged. The test that matters: a processor that dies on its
second batch leaves six entries pending, and the restarted consumer writes
exactly the six missing rows, the PRIMARY KEY turning the redelivery into
no-ops.

**Gotcha: a fake broker never blocks.** The end-to-end test (ingest service
and API on one fakeredis) hung with 500 entries queued and zero rows for as
long as the producer ran. A blocking group read returns at once on fakeredis,
so the subscribe loop spun the event loop and the worker thread writing the
batches never got the GIL back; the moment the producer was cancelled all
ten batches went through in two seconds. An idle read now pauses 10 ms, which
a real broker's `BLOCK` makes irrelevant. Worth knowing in general: a hot
`await` loop that never actually suspends starves everything else.

### Step: *feat(replay): replay a time window from the bus (`pulsestream replay --from-bus`)*

`BusReplaySource` walks `[since, until)` outside any group: nothing is
delivered, pending or acknowledged. `replay()` takes any `EventSource` and
keeps the lake as its default; the lake is still the long-term source, the
bus holds what the processor missed while it was down and the lake was
therefore not written.

### Step: *build(compose): redis, the ingest service and the API in bus mode; bus panels and alerts*

`docker compose up` now runs the split: `redis` (AOF, healthcheck), `ingest`
(its own image target), `api` under the fixed consumer name `api`. Three
alert rules watch the seam (ingest down, bus backlog, pending not
acknowledged) and the Grafana dashboard gains the stream's length, lag and
pending, the publish rate and the redelivery count.

### Step: *fix(api): /bus and the ingest /stats answer 503 with the reason when the broker is away*

Found on the live run, below.

### Step: *docs: ADRs 0016–0017, tutorial chapter 05, README, operations for Phase 5*

This chapter, the two decision records, and the bus sections of the runbook.

## Running it

```bash
docker compose up --build -d                   # redis + ingest + api + dashboard
curl -s localhost:8001/stats                   # what the ingest service published
curl -s localhost:8000/bus                     # the stream and the consumer group
docker compose kill ingest && sleep 60 && docker compose start ingest   # the API keeps serving
docker compose kill api && sleep 60 && docker compose start api         # the backlog drains
uv run pulsestream replay --from-bus --since 2026-11-01T16:47:30 --until 2026-11-01T16:49:30 --db window.duckdb
```

Without Docker: a Redis anywhere (`docker run -p 6379:6379 redis:8-alpine`),
then `PULSE_BUS=redis uv run pulsestream ingest` and
`PULSE_BUS=redis uv run pulsestream api` in two terminals.

### What the live run showed

Both services started on 2026-11-01 against a Redis that was not there yet,
and ran for 12 minutes against the live Wikimedia stream once it was.

**The broker comes up late.** The ingest service retried its first batch nine
times (`publish.retry attempt=9 delay=23.53 events=37`, backoff capped at
30 s with jitter) and the API reconnected ten times (`bus.reconnect
attempt=10`). Nothing was dropped: 100 s after Redis came up,

```
ingest /stats   published_total=10099 batches=204 publish_failures=9 queue_depth=0 stream_length=10099
api /bus        length=10099 pending=0 lag=0 delivered_total=10099 acked_total=10099
api /stats/latest   events_total=5397 events_last_min=1349 lag_seconds=2.9
```

**Kill the API.** With 11 899 entries all acknowledged and 6 372 rows written,
the API process was killed while the ingest service kept publishing (13 718
entries 45 s later, `ready: true`). Restarted under the same consumer name
2.5 minutes later:

| after restart | lag   | pending | delivered | acked | rows   |
| ------------- | ----- | ------- | --------- | ----- | ------ |
| 8 s           | 6 740 | 1 150   | 1 101     | 50    |        |
| 30 s          | 0     | 34      | 8 884     | 8 850 | 10 333 |

The 1 150 pending at 8 s is the queue (1 000) plus a batch: the reads block
while the queue is full, the backpressure of Phase 1 again. Redeliveries: 0
(nothing had been pending at the kill). Rows went from 6 372 to 10 333 with
no duplicates possible.

**Kill the ingest service.** With 22 698 entries published, the ingest process
was killed. The API stayed `ready` (36 s of silence is inside the 120 s
window), `lag=0`, and kept serving. Restarted 35 s later, it published 780
entries in its first 25 s and the API had consumed them (`pending=41`, one
batch in flight, `lag=0`). The 35 s gap is gone: the resume cursor lives in
the ingest process's memory, ADR 0017 says so, and persisting the Wikimedia
`Last-Event-ID` is the obvious follow-up.

**Replay a window from the bus.** Two minutes from the middle of the run,
into a fresh database:

```
$ uv run pulsestream replay --from-bus --since 2026-11-01T16:47:30 --until 2026-11-01T16:49:30 --db window.duckdb
replayed 5536 raw events from the bus: 2653 kept, 2653 rows in window.duckdb, 186 minute buckets aggregated, 0 anomalies
```

The broker's own `XRANGE` for the same ids returns 5 536 entries; the rows are
2 653 distinct `event_id`s; `received_at` spans 16:47:29.4 to 16:49:29.0,
half a second before the window on each side, which is the micro-batch wait
between an event being received by the ingest service and published.

**Gotcha: WSL shuts its VM down between commands.** The first attempt ran
both services against a Redis container that had died with the WSL VM a
minute after it was started (WSL stops the VM when its last session ends and
Docker's containers go with it). The services behaved exactly as designed,
retrying with backoff; what the attempt found was that `/bus` and the ingest
`/stats` answered 500 while the probes already said `bus: false`. That is the
fix commit above: a broker that is away is an expected state, 503 with the
reason.

## Definition of done

- Ingestion and processing run as separate containers (`ingest`, `api`)
  that meet in a Redis Stream; killing and restarting either does not stop
  the other, and what reached the bus is processed when the API is back.
- A past window is replayable from the broker (`pulsestream replay
  --from-bus`), and the count matches the broker's own range.
- Delivery semantics are explicit and tested: at-least-once, acknowledge
  after the write, idempotent rows, redelivery on restart, claims from dead
  consumers, retried publishes and acknowledgements.
- Every commit on the branch passes ruff, mypy, bandit and pytest on its own.

## Deliberately left for later

- The ingest service's resume cursor (`Last-Event-ID`) is in memory: an
  ingest restart loses the events of its downtime.
- Detector state (open windows, baselines) is in memory: an API restart warms
  up again. The bus fixes data loss, not state loss.
- One consumer per group; scale-out is N streams keyed by entity (ADR 0016).
- The raw lake can hold a redelivered batch twice; the counter
  `pulsestream_bus_redeliveries_total` says how often, the lake replay dedupes.
- No Redpanda. The `EventBus` seam is where it would go.

That is the end of the HLD's roadmap. The stretch goals (§14) start from a
system that is deployed, observed and decoupled.
