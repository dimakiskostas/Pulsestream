# PulseStream

[![ci](https://github.com/dimakiskostas/Pulsestream/actions/workflows/ci.yml/badge.svg)](https://github.com/dimakiskostas/Pulsestream/actions/workflows/ci.yml)

Near-real-time event analytics pipeline with embedded anomaly detection.

Live edits from every Wikimedia project stream in over SSE, are validated into
typed events, land in DuckDB in micro-batches and in a partitioned Parquet lake,
and are served by a typed FastAPI API and a Streamlit dashboard. A statistical
anomaly detector, Prometheus metrics, Docker/CI/CD and a broker-based split follow
phase by phase. The design is in [`docs/hld.pdf`](docs/hld.pdf); the build is
written up as a tutorial in [`docs/tutorial/`](docs/tutorial/README.md), one git
branch per phase.

## Status

| Phase | Increment                                                        | State   |
| ----- | ---------------------------------------------------------------- | ------- |
| 0     | Skeleton: uv, Ruff, mypy (strict), pytest, pre-commit, green CI  | done    |
| 1     | Vertical slice: live source → queue → validate → DuckDB → API    | done    |
| 2     | Storage + analytics: Parquet lake, aggregates, scheduler, dashboard, replay, quality gates | done |
| 3     | Anomaly detection v1: rolling features + EWMA control band       | next    |
| 4     | CI/CD + observability: containers, GHCR, metrics, health         | planned |
| 5     | Streaming decoupling: broker, separate services, replay          | planned |

## How it works (Phase 2)

```
Wikimedia EventStreams ──SSE──▶ WikimediaSource ──▶ asyncio.Queue ──▶ consumer ──▶ StreamProcessor
   (recentchange)              reconnect + resume    (bounded =        (worker        │        │
                                                      backpressure)     thread)       ▼        ▼
                                                                          Parquet lake      DuckDB wiki_events
                                                                          data/raw/dt=/hour=   └▶ agg_minutely
                                   APScheduler: refresh aggregates (60 s), compact lake (10 min)
                                   FastAPI: /health /stats/latest /timeseries /aggregates/minutely
                                            /entities/top /lake /jobs            ◀──HTTP── Streamlit dashboard
```

One API process owns the pipeline, the DuckDB writer and the scheduler. The
dashboard is a second process that reads only the API. `EventSource` is a
protocol, so the live source, the replay `FakeSource` and the lake's
`ParquetSource` are interchangeable.

## Quickstart

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/). `make` is optional:
every target is a one-line `uv run` command (see the [Makefile](Makefile)).

```bash
uv sync --dev                 # .venv from uv.lock
uv run pre-commit install     # hooks run ruff + mypy via uv on every commit
uv run pytest                 # the whole suite is offline, ~1 min
```

Run the API with the live stream, the lake and the scheduler inside it, and the
dashboard next to it:

```bash
uv run pulsestream api        # http://127.0.0.1:8000  (OpenAPI docs at /docs)
```

```bash
uv run pulsestream dashboard  # http://localhost:8501, reads the API
```

```bash
curl -s localhost:8000/stats/latest
curl -s "localhost:8000/aggregates/minutely?minutes=60"
curl -s "localhost:8000/entities/top?minutes=60&limit=10"
curl -s localhost:8000/lake
curl -s localhost:8000/jobs
```

`events_total` climbs within seconds; `/aggregates/minutely` fills after the
first refresh job (every minute, or `POST /jobs/refresh_aggregates/run`).

Other commands:

```bash
PULSE_SOURCE=fake PULSE_FIXTURE_DELAY=0.05 uv run pulsestream api     # offline replay of the fixture
uv run pulsestream replay --lake data/raw --db replay.duckdb          # rebuild a DB from the lake
uv run pulsestream check --db pulsestream.duckdb --lake data/raw      # pandera data-quality gates
uv run pulsestream capture -n 500 -o tests/fixtures/sample_events.jsonl
```

## Configuration

Settings are typed (`pydantic-settings`), prefixed `PULSE_`, and can come from the
environment or a `.env` file. [`.env.example`](.env.example) lists every key;
the defaults live in [`src/pulsestream/config.py`](src/pulsestream/config.py).

| Key                                 | Default              | Meaning                                  |
| ----------------------------------- | -------------------- | ---------------------------------------- |
| `PULSE_DB_PATH`                     | `pulsestream.duckdb` | DuckDB file (one writer process)         |
| `PULSE_LAKE_ROOT` / `_ENABLED`      | `data/raw` / `true`  | Parquet raw store                        |
| `PULSE_QUEUE_MAXSIZE`               | `1000`               | bounded queue = backpressure             |
| `PULSE_BATCH_SIZE` / `_FLUSH_INTERVAL` | `50` / `2.0`      | micro-batch size / seconds               |
| `PULSE_SOURCE`                      | `wikimedia`          | `wikimedia` (live) or `fake` (replay)    |
| `PULSE_USER_AGENT`                  | `PulseStream/<version> (...)` | Wikimedia requires a descriptive UA |
| `PULSE_AGGREGATE_INTERVAL_SECONDS`  | `60`                 | agg_minutely refresh cadence             |
| `PULSE_COMPACTION_INTERVAL_SECONDS` | `600`                | lake compaction cadence                  |
| `PULSE_API_URL`                     | `http://127.0.0.1:8000` | what the dashboard reads              |

## Development

```bash
make check        # = uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
make precommit    # = uv run pre-commit run --all-files
```

Tests never touch the network: the live source is tested against an httpx mock
transport, the pipeline/API tests replay `tests/fixtures/sample_events.jsonl`
(500 real events captured on 2026-10-11), and the dashboard runs headlessly under
Streamlit's `AppTest`. CI runs the same commands on Python 3.12 and 3.13.

## Repository layout

```
src/pulsestream/
  cli.py              pulsestream api | capture | replay | dashboard | check
  config.py           typed settings (PULSE_*)
  models.py           RawEvent envelope, WikiEvent domain event (+ from_raw filter)
  pipeline.py         producer / consumer over a bounded asyncio.Queue; Processor protocol
  processing/         StreamProcessor: raw lake -> validate -> DuckDB
  sources/            EventSource protocol; Wikimedia (SSE), Fake (replay), Parquet (lake)
  storage/            duck.py (schema, UTC, cursors), parquet.py (lake + compaction), aggregates.py
  scheduler/          APScheduler jobs: refresh aggregates, compact lake
  api/                FastAPI app factory and response schemas
  dashboard/          Streamlit page + its HTTP data layer
  quality.py          pandera gates for the lake and the tables
  replay.py, capture.py
tests/                unit, integration (fixture replay), API contract, dashboard, quality
docs/
  hld.pdf, tutorial.pdf      the design and the guided tutorial the build follows
  tutorial/                  the worked tutorial for this repository, one chapter per phase
  decisions/                 architecture decision records (ADR 0001-0009)
```

## Reading the history

Each phase was built on its own branch and merged into `main` with a merge
commit, so `git log --oneline --first-parent main` shows one line per phase and
`git log --oneline --no-merges <merge>^1..<merge>^2` lists that phase's individual
steps. The tutorial chapters walk through those steps in order and explain the
decisions behind them.
