# 01 · Vertical slice (Phase 1)

> Goal (HLD §13, tutorial Parts 1–2): real events from the wild land in DuckDB
> and `/stats/latest` returns honest numbers. One process, in-process queue,
> no broker yet. Then make it trustworthy: replay instead of the live source
> in tests.

Branch: `phase-1-vertical-slice`. The first half of the slice (models, the
SSE source, the consumer, DuckDB storage) was built in August 2026 as PRs
#2–#4 on `main` plus an unmerged `feat/api` branch; this branch merges that
work, fixes what running it revealed, and completes the phase. Steps below
are the branch's commits in order (`git log --oneline main..phase-1-vertical-slice`).

## The slice in one picture

```
WikimediaSource.stream()  ──RawEvent──▶  asyncio.Queue(maxsize=1000)  ──▶  run_consumer
  SSE, reconnect, resume                    full queue blocks put()           WikiEvent.from_raw
                                            (= backpressure)                  micro-batch → DuckDB
                                                                                    │
FastAPI lifespan owns all three tasks/handles ◀───────────────────────────────────────┘
  GET /health · /stats/latest · /timeseries?bucket=10s&minutes=10
```

Files: `sources/base.py` (the `EventSource` protocol), `models.py`,
`sources/wikimedia.py`, `pipeline.py`, `storage/duck.py`, `config.py`,
`api/app.py`, `api/schemas.py`, `sources/fake.py`, `capture.py`, `cli.py`.

## Steps

### Step: *Merge branch 'feat/api'*

What August left: `RawEvent`/`WikiEvent` with `from_raw` (filter `edit`/`new`,
`entity = wiki`, see [ADR 0001](../decisions/0001-event-filter-entity-and-headline-anomaly.md)),
a reconnecting SSE source, a consumer with a time-based flush, a `wiki_events`
table with a primary key and `ON CONFLICT DO NOTHING`, and on `feat/api` the
FastAPI lifespan with `/health`, `/stats/latest`, `/timeseries`. The merge
drops a `pulsestream.duckdb.wal` that had been committed by accident.

### Step: *fix(pipeline): clear the batch after a size flush; never write an empty batch*

Two bugs that only show under load or when idle:

- After a size-triggered flush the batch was written but **not cleared**, so
  every later flush re-inserted the whole history. The primary key hid the
  duplicates; the cost grew with every flush.
- The time-based flush called `write_batch` with an empty batch. DuckDB raises
  on an empty `executemany`, so on a quiet stream the consumer task **died at
  the first idle interval**, silently. (`/health` now reports a dead consumer;
  see the API step.)

The consumer has one `flush()` used by the size rule, the timer and the
sentinel. The tests record the batches handed to `write_batch` and pin exact
boundaries: `[5, 5, 2]` for 12 events at `batch_size=5`
([ADR 0004](../decisions/0004-micro-batch-writes.md)).

### Steps: *chore: normalise line endings…* and *chore(pre-commit): route every hook through uv…*

Covered in [00 · Skeleton](00-skeleton.md#adjustments-made-later-on-phase-1-vertical-slice):
LF everywhere, the `pytz` surprise, and pre-commit hooks that run through uv.

### Step: *feat(sources): FakeSource replays recorded events; load_fixture reads JSONL*

Tutorial Part 2.2. `FakeSource(events, delay=0)` satisfies the `EventSource`
protocol, so it drops into the pipeline unchanged. `delay` paces a replay for
demos ([ADR 0006](../decisions/0006-replay-fixtures-for-tests.md)).

### Step: *feat(cli): `pulsestream api|capture`; capture() records a replay fixture*

`scripts/capture_sample.py` was an empty file. The logic lives in
`capture(source, n, path)` and the uv-generated "Hello" entry point becomes a
real CLI with one sub-command per tool, so there is one obvious front door:

```bash
uv run pulsestream api          # serve, pipeline inside
uv run pulsestream capture -n 500
```

**Gotcha: closing an async generator you stop early.** `capture` breaks out
of `async for` after `n` events. The generator (and its HTTP response) would
only be closed when garbage collected; the code closes it explicitly. The
`EventSource` protocol is typed as a plain `AsyncIterator`, which has no
`aclose`, so `contextlib.aclosing` fails mypy; a small `getattr` helper does it.

### Step: *feat(storage): connect() pins the DuckDB session to UTC; guard empty batches*

**Gotcha: DuckDB renders TIMESTAMPTZ in the session's time zone**, which
defaults to the host's. On the development box a value stored as 09:18 UTC
came back as 12:18 Europe/Athens. `connect()` runs `SET TimeZone = 'UTC'` on
every connection and creates the schema for writers
([ADR 0003](../decisions/0003-duckdb-single-writer-utc.md)). A test pins the
round-trip and the setting.

### Step: *feat(sources): identify to Wikimedia, fail fast on 4xx, resume on reconnect*

This step exists because the first live capture **hung for ten minutes**.

**Gotcha 1: Wikimedia requires a descriptive User-Agent.** The default
`python-httpx/…` UA gets HTTP 403 from the edge (`curl` with a custom UA had
worked, which is why the difference took a while to see).

**Gotcha 2: the 403 was retried forever, silently.** httpx-sse raises
`SSEError` when the response is not `text/event-stream`, and `SSEError`
subclasses `httpx.RequestError`, the very exception the reconnect loop
treats as transient. A permanent refusal looked like a flaky network.

The source now sends `PulseStream/<version> (<repo URL>)`, checks the status
*before* reading (4xx other than 429 → `SourceFatalError`, 429 → honour
`Retry-After`, 5xx → back off), reconnects after drops and timeouts with
jittered exponential backoff (1 s, 2 s, 4 s … capped at 30 s), **resumes with
`Last-Event-ID`** so a blip does not lose events, and uses a 60 s read
timeout instead of `timeout=None` so a dead connection is noticed. It logs
`source.connected` / `source.reconnect` through structlog.

The tests never touch the network: `client_factory`, `sleep` and `rng` are
injectable, and an httpx `MockTransport` plays the edge (403, 429, 503,
a connection that drops after two events and must be resumed from `id-2`).

### Step: *test(fixtures): commit a 500-event replay sample and the integration test*

`pulsestream capture -n 500` recorded ~20 s of the stream on 2026-10-11:

| raw events | edit | new | categorize | log | kept by `from_raw` | bots | top wiki        |
| ---------- | ---- | --- | ---------- | --- | ------------------ | ---- | --------------- |
| 500        | 295  | 11  | 164        | 30  | **306**            | 128  | wikidatawiki 112 |

Those numbers are pinned in `tests/test_integration_pipeline.py`, which runs
`FakeSource → queue → consumer → DuckDB` and also asserts idempotent double
replay ([ADR 0005](../decisions/0005-at-least-once-idempotent-writes.md)) and
full delivery through an 8-slot queue (backpressure,
[ADR 0002](../decisions/0002-in-process-queue-and-backpressure.md)).

### Step: *feat(config): pipeline, source and stream settings via PULSE_* env vars*

`Settings` gains the batch/flush knobs, `source=wikimedia|fake` with a fixture
path and delay, and the stream URL, UA and read timeout. `.env.example` lists
every key.

### Step: *refactor(api): create_app() factory, injectable source, honest stats*

`create_app(cfg, source)` makes the app testable; `build_source()` is the one
place that maps settings to a concrete source (tutorial Part 1.1's promise:
swapping sources is a one-line wiring change). Shutdown is ordered: cancel the
producer, hand the consumer the sentinel so it flushes, bounded waits, close
DuckDB. `/health` reports task states and queue depth and says `degraded` when
the consumer has died; `/stats/latest` adds `events_total` and `lag_seconds`;
`/timeseries` had a window bug (`minutes × bucket` instead of minutes of wall
clock) that is fixed, and its bucket is an allow-listed literal, never user
text in SQL.

### Step: *test(api): contract tests through httpx's ASGI transport*

**Gotcha: `httpx.ASGITransport` does not run lifespan events.** The pipeline
starts in the lifespan, so the tests enter it explicitly:

```python
async with app.router.lifespan_context(app):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        ...
```

The replay source is finite; the fixture polls `/stats/latest` until the
consumer has flushed, then the tests assert the contract (including 422s and
the models present in `/openapi.json`) and that shutdown flushes a partial
batch to disk.

### Step: *fix(capture): write LF line endings on every platform*

Python's text mode wrote CRLF on Windows; git normalised it but warned.
`newline="\n"` makes a capture byte-identical everywhere.

### Step: *docs: README, Makefile, ADRs 0002–0006, tutorial chapters 00–01*

This chapter, the decision records for D1–D4 and the replay strategy, the
HLD and tutorial PDFs under `docs/`, and a Makefile whose targets are all
one-line `uv run` commands.

## Running it

```bash
uv run pulsestream api
curl -s localhost:8000/health
curl -s localhost:8000/stats/latest
curl -s "localhost:8000/timeseries?bucket=10s&minutes=2"
```

A real run on 2026-10-11, sampled 6 s, 18 s and 30 s after start:

```
$ curl -s localhost:8011/health
{"status":"ok","pipeline":{"producer":"running","consumer":"running","queue_depth":0}}

$ curl -s localhost:8011/stats/latest            # t = 6 s
{"events_total":29,"events_last_min":29,"latest_event_ts":"2026-10-11T10:54:01Z","lag_seconds":3.300461}
$ curl -s localhost:8011/stats/latest            # t = 18 s
{"events_total":276,"events_last_min":276,"latest_event_ts":"2026-10-11T10:54:12Z","lag_seconds":4.562775}
$ curl -s localhost:8011/stats/latest            # t = 30 s
{"events_total":522,"events_last_min":521,"latest_event_ts":"2026-10-11T10:54:26Z","lag_seconds":2.814765}

$ curl -s "localhost:8011/timeseries?bucket=10s&minutes=2"
{"bucket":"10s","minutes":2,"points":[
  {"time":"2026-10-11T10:53:20Z","count":1},
  {"time":"2026-10-11T10:53:50Z","count":13},
  {"time":"2026-10-11T10:54:00Z","count":216},
  {"time":"2026-10-11T10:54:10Z","count":186},
  {"time":"2026-10-11T10:54:20Z","count":106}]}
```

Roughly 17 kept events per second; `lag_seconds` of 3–5 s is the distance
between an edit's own timestamp (whole seconds, assigned by Wikimedia) and the
moment it was queryable here. The first two buckets are events that happened
before the connection opened and arrived in the initial burst: `event_ts` is
when it happened, `received_at` is when we got it, and both are stored.

## Definition of done

- Live Wikipedia edits land in DuckDB; `/stats/latest` returns a non-zero,
  increasing `events_total` with a fresh `latest_event_ts` and a lag of a few
  seconds (run above).
- `uv run pytest` is green against the fixture, with unit tests pinning
  `from_raw`'s filtering and `byte_delta` math and an integration test
  asserting exact counts.
- Every commit on the branch passes ruff, mypy and pytest on its own.

## What is deliberately left for later

- DuckDB calls run synchronously on the event loop. Queries are tiny in
  Phase 1; Phase 2 moves writes and heavier reads to `asyncio.to_thread`
  with per-request cursors.
- `/health` is liveness only. Phase 4 splits liveness from readiness
  (source connected, store reachable) and adds Prometheus metrics.
- Raw events are not persisted yet; Phase 2 appends them to partitioned
  Parquet so the whole pipeline becomes replayable from disk.

Next: 02 · Storage and analytics.
