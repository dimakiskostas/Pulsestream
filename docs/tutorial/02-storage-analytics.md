# 02 · Storage and analytics (Phase 2)

> Goal (HLD §13, tutorial Part 3 / Phase 2): durable raw events and fast
> charts. Append events to partitioned Parquet as the immutable source of
> truth, let DuckDB query it, build derived tables, add a Streamlit dashboard
> that reads through the API, and a scheduled compaction job.

Branch: `phase-2-storage-analytics`, 14 commits on top of Phase 1. Steps
below are the commits in order (`git log --oneline --no-merges <merge>^1..<merge>^2`).

## The system after Phase 2

```
Wikimedia SSE ─▶ WikimediaSource ─▶ Queue ─▶ consumer ─▶ StreamProcessor ──▶ Parquet lake  data/raw/dt=…/hour=…/
                                              (to_thread)      │               (raw, unfiltered, compacted hourly)
                                                               └─ validate ─▶ DuckDB wiki_events ─▶ agg_minutely
                                                                                   ▲                  ▲
APScheduler: refresh_aggregates every 60 s, compact_lake every 10 min ─────────────┘──────────────────┘
FastAPI: /health /stats/latest /timeseries /aggregates/minutely /entities/top /lake /jobs
Streamlit dashboard (separate process) ──HTTP──▶ FastAPI          pulsestream replay: lake ─▶ fresh DuckDB
```

New modules: `processing/processor.py`, `storage/parquet.py`,
`storage/aggregates.py`, `scheduler/`, `sources/lake.py`, `replay.py`,
`dashboard/`, `quality.py`. New commands: `pulsestream replay`,
`pulsestream dashboard`, `pulsestream check`.

## Steps

### Step: *build: add polars, apscheduler and streamlit; pandera[polars]; mypy override*

Dependencies arrive when first used. APScheduler 3.x ships no type hints,
so it gets the only `ignore_missing_imports` override in the project.

### Step: *refactor(pipeline): the consumer hands raw micro-batches to a Processor*

Before writing the lake, the consumer needed a seam. It now owns *when*
(batching, flush rules, the sentinel) and hands each batch of **raw**
events to a `Processor` that owns *what*. `StreamProcessor` validates and
persists; everything Phase 2 and 3 add plugs in behind that one method.

### Step: *feat(storage): partitioned Parquet raw store written per micro-batch*

`ParquetRawStore` writes one zstd Parquet file per micro-batch under
`dt=YYYY-MM-DD/hour=HH/`, partitioned by `received_at` in UTC, via a `.tmp`
sibling and a rename ([ADR 0008](../decisions/0008-raw-lake-layout-and-compaction.md)).

**Gotcha: the write path is Polars, and Python types matter.** A schema
mixing dtype classes (`pl.Utf8`) with instances (`pl.Datetime("us", "UTC")`)
is `dict[str, object]` to mypy; use instances throughout.

### Step: *feat(api): wire the Parquet lake into the pipeline (PULSE_LAKE_ROOT)*

The raw batch is written **before** validation, so the lake holds exactly
what arrived, dropped `log`/`categorize` events included. That is what makes
a filter change a replay rather than a re-ingest.

### Step: *feat(storage): raw_events view over the lake and GET /lake*

**Gotcha: DuckDB 1.5.5 internal error on hive columns.** `min(dt)` over
`read_parquet(..., hive_partitioning=true)` raises *"Attempted to access
index 5 within vector of size 5"* when the glob matches exactly one file.
The `raw_events` view derives `dt` and `hour` from `received_at` instead;
the values are identical by construction. The view can only be created once
a file exists (DuckDB binds the query at `CREATE VIEW`).

### Step: *feat(storage): compaction merges settled files per hour partition*

Files older than a grace period (120 s) are merged per hour partition with a
streaming Polars scan, written temp + rename, then deleted. Merged files take
part in later rounds, so a partition converges to one file. In the live run
below, 18 micro-batch files became one within a minute.

### Step: *feat(storage): agg_minutely derived table with an idempotent window refresh*

`INSERT OR REPLACE` over the last N minutes of `wiki_events`, keyed by
`(bucket_ts, entity)`: idempotent, late events update their bucket, buckets
that leave the window stay as they were. Aggregates come from the validated
table, not the raw lake, so the filter has one home
([ADR 0009](../decisions/0009-derived-tables-from-validated-events.md)).

### Step: *feat(scheduler): APScheduler jobs for aggregate refresh and lake compaction*

`Maintenance` holds the job bodies; each run opens its own DuckDB cursor
because jobs execute in worker threads. `run()` records runs, failures,
duration, last result and error, and never raises. `GET /jobs` shows it;
`POST /jobs/{name}/run` triggers a job on demand (the tests use it).

### Step: *perf: run DuckDB and Parquet work on worker threads, not the event loop*

The tutorial's Part 1.5 warning, applied: the consumer processes batches via
`asyncio.to_thread`, API reads use `aquery()` with a per-request cursor. A
test proves `/health` answers while a 1-second blocking batch is in flight.

**Gotcha: cursors do not inherit `SET TimeZone`.** The first worker-thread
read returned `latest_event_ts` as `+03:00`. `connect()` now issues
`SET GLOBAL TimeZone = 'UTC'`, which every cursor inherits
([ADR 0003](../decisions/0003-duckdb-single-writer-utc.md) updated).

### Step: *feat(api): /aggregates/minutely and /entities/top for charts*

Chart-shaped endpoints over `agg_minutely`: combined per-minute points
(entity `*`) or one wiki's, and the busiest wikis with their bot share.
Before the first refresh they are empty; that is the documented contract.

### Step: *feat(replay): ParquetSource streams the lake; `pulsestream replay` rebuilds a DB*

`ParquetSource` is an `EventSource` over the Parquet files, ordered by
`received_at` with an optional `[since, until)` window. `replay()` drives the
real consumer and processor into a target DuckDB with the raw store switched
off (a replay reads the lake, never writes it) and refreshes the aggregates
over everything replayed. Replaying the committed fixture gives 500 raw →
306 rows, twice.

### Step: *feat(dashboard): Streamlit page over the API; `pulsestream dashboard`*

Metrics, edits per minute with bot share, busiest wikis, lake size, job
status. The data layer (`dashboard/data.py`) returns typed Polars frames and
is tested with an httpx mock transport; the page is tested headlessly with
`streamlit.testing.v1.AppTest` ([ADR 0007](../decisions/0007-dashboard-reads-through-the-api.md)).

### Step: *feat(quality): pandera gates for the lake, wiki_events and agg_minutely; `pulsestream check`*

Types, nullability, ranges and cross-column facts for the three datasets,
validated with `lazy=True` so one run lists every violation. The fixture
passes every gate; broken frames fail naming the right columns. DuckDB
`INTEGER` columns arrive in Polars as `Int32`, which the schemas reflect.

### Step: *fix(scheduler): report next_run_at in UTC*

Spotted in the live run: APScheduler's `IntervalTrigger` uses the host's
zone unless told otherwise.

### Step: *docs: ADRs 0007–0009, tutorial chapter 02, README*

This chapter and the decision records for the dashboard boundary, the lake
layout and the derived tables.

## Running it

```bash
uv run pulsestream api                   # API + pipeline + scheduler
uv run pulsestream dashboard             # http://localhost:8501, reads the API
uv run pulsestream check --db pulsestream.duckdb --lake data/raw
uv run pulsestream replay --lake data/raw --db replay.duckdb --since 2026-10-11T12:00:00
```

A real run on 2026-10-11 with fast intervals (`PULSE_AGGREGATE_INTERVAL_SECONDS=20`,
`PULSE_COMPACTION_INTERVAL_SECONDS=40`, `PULSE_COMPACTION_GRACE_SECONDS=10`),
sampled 55 s after start:

```
$ curl -s localhost:8011/stats/latest
{"events_total":731,"events_last_min":729,"latest_event_ts":"2026-10-11T12:03:01Z","lag_seconds":3.261346}

$ curl -s localhost:8011/lake
{"enabled":true,"root":".../p2-lake","files":15,"partitions":1,"bytes":373139,"rows":1503,
 "first_received_at":"2026-10-11T12:02:15.245039Z","last_received_at":"2026-10-11T12:03:04.334307Z"}

$ curl -s localhost:8011/jobs
{"scheduler_enabled":true,"jobs":[
  {"name":"refresh_aggregates","runs":2,"failures":0,"last_duration_seconds":0.062,"last_result":"57 buckets",...},
  {"name":"compact_lake","runs":1,"failures":0,"last_duration_seconds":0.562,"last_result":"1 partitions, 18 files merged",...}]}

$ curl -s "localhost:8011/aggregates/minutely?minutes=5"
{"minutes":5,"entity":null,"points":[
  {"bucket_ts":"2026-10-11T12:01:00Z","entity":"*","count":1,"bot_count":1,"avg_value":6777.0,"max_value":6777,"anomaly_count":0},
  {"bucket_ts":"2026-10-11T12:02:00Z","entity":"*","count":543,"bot_count":274,"avg_value":900.09,"max_value":8066,"anomaly_count":0}]}

$ curl -s "localhost:8011/entities/top?minutes=5&limit=5"
{"minutes":5,"entities":[{"entity":"commonswiki","count":199,"bot_count":161},{"entity":"wikidatawiki","count":90,"bot_count":26},
 {"entity":"enwiki","count":57,"bot_count":12},{"entity":"zhwiki","count":29,"bot_count":11},{"entity":"frwiki","count":18,"bot_count":5}]}
```

1 503 raw rows in the lake versus 731 stored events: the lake keeps the
`categorize` and `log` events the filter drops. The dashboard, started with
`PULSE_API_URL=http://127.0.0.1:8011`, showed 3 033 events stored, 804 in
the last minute, 2.8 s lag and the per-minute chart after five minutes.

## Definition of done

- A chart of edits per minute over the last hour, served from the API
  (`/aggregates/minutely`), backed by aggregates DuckDB computed from
  validated events (`agg_minutely`), on a Streamlit page.
- Raw events in partitioned Parquet, compacted on a schedule, queryable as
  `raw_events` and replayable with `pulsestream replay`.
- Every commit on the branch passes ruff, mypy and pytest on its own.

## Deliberately left for later

- No retention for the lake or the tables (a scheduler job for Phase 4/5).
- `anomaly_count` is always zero until Phase 3 joins the anomalies table.
- Structured JSON logging with levels: the processor's debug lines reach
  stdout today because structlog is unconfigured; Phase 4 configures it.
- The dashboard is functional, not designed (HLD non-goal).

Next: 03 · Anomaly detection v1.
