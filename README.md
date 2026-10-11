# PulseStream

[![ci](https://github.com/dimakiskostas/Pulsestream/actions/workflows/ci.yml/badge.svg)](https://github.com/dimakiskostas/Pulsestream/actions/workflows/ci.yml)

Near-real-time event analytics pipeline with embedded anomaly detection.

Live edits from every Wikimedia project stream in over SSE, are validated into
typed events, land in DuckDB in micro-batches, and are served by a typed FastAPI
API. A statistical anomaly detector, a Parquet raw store, a dashboard, Prometheus
metrics, Docker/CI/CD and a broker-based split follow phase by phase. The design
is in [`docs/hld.pdf`](docs/hld.pdf); the build is written up as a tutorial in
[`docs/tutorial/`](docs/tutorial/README.md), one git branch per phase.

## Status

| Phase | Increment                                                        | State   |
| ----- | ---------------------------------------------------------------- | ------- |
| 0     | Skeleton: uv, Ruff, mypy (strict), pytest, pre-commit, green CI  | done    |
| 1     | Vertical slice: live source → queue → validate → DuckDB → API    | done    |
| 2     | Storage + analytics: partitioned Parquet, aggregates, dashboard  | next    |
| 3     | Anomaly detection v1: rolling features + EWMA control band       | planned |
| 4     | CI/CD + observability: containers, GHCR, metrics, health         | planned |
| 5     | Streaming decoupling: broker, separate services, replay          | planned |

## How it works (Phase 1)

```
Wikimedia EventStreams ──SSE──▶ WikimediaSource ──▶ asyncio.Queue ──▶ consumer ──▶ DuckDB
   (recentchange)              reconnect + resume    (bounded =        validate,     wiki_events
                                                      backpressure)    micro-batch        │
                                                                                          ▼
                                                        FastAPI  /health  /stats/latest  /timeseries
```

One process: the FastAPI lifespan starts the producer and consumer tasks and owns
the DuckDB connection. `EventSource` is a protocol, so the live source and the
replay `FakeSource` are interchangeable.

## Quickstart

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/). `make` is optional:
every target is a one-line `uv run` command (see the [Makefile](Makefile)).

```bash
uv sync --dev                 # .venv from uv.lock
uv run pre-commit install     # hooks run ruff + mypy via uv on every commit
uv run pytest                 # the whole suite is offline and takes ~20 s
```

Run the API with the live stream inside it:

```bash
uv run pulsestream api        # http://127.0.0.1:8000  (docs at /docs)
```

```bash
curl -s localhost:8000/health
curl -s localhost:8000/stats/latest
curl -s "localhost:8000/timeseries?bucket=10s&minutes=10"
```

`events_total` climbs within a couple of seconds; `lag_seconds` is the distance
between the newest event's own timestamp and now.

No network? Replay the committed fixture at human speed:

```bash
PULSE_SOURCE=fake PULSE_FIXTURE_DELAY=0.05 uv run pulsestream api
```

Record a fresh replay sample (500 events, ~10 s):

```bash
uv run pulsestream capture -n 500 -o tests/fixtures/sample_events.jsonl
```

## Configuration

Settings are typed (`pydantic-settings`), prefixed `PULSE_`, and can come from the
environment or a `.env` file. [`.env.example`](.env.example) lists every key;
the defaults live in [`src/pulsestream/config.py`](src/pulsestream/config.py).

| Key                    | Default                      | Meaning                                  |
| ---------------------- | ---------------------------- | ---------------------------------------- |
| `PULSE_DB_PATH`        | `pulsestream.duckdb`         | DuckDB file                              |
| `PULSE_QUEUE_MAXSIZE`  | `1000`                       | bounded queue = backpressure             |
| `PULSE_BATCH_SIZE`     | `50`                         | micro-batch size                         |
| `PULSE_FLUSH_INTERVAL` | `2.0`                        | seconds a partial batch may wait         |
| `PULSE_SOURCE`         | `wikimedia`                  | `wikimedia` (live) or `fake` (replay)    |
| `PULSE_FIXTURE_PATH`   | `tests/fixtures/sample_events.jsonl` | replay file for `fake`           |
| `PULSE_USER_AGENT`     | `PulseStream/<version> (...)`| Wikimedia requires a descriptive UA      |
| `PULSE_READ_TIMEOUT`   | `60`                         | seconds without bytes = dead stream      |

## Development

```bash
make check        # = uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
make precommit    # = uv run pre-commit run --all-files
```

Tests never touch the network: the live source is tested against an httpx mock
transport, and the pipeline/API tests replay `tests/fixtures/sample_events.jsonl`
(500 real events captured on 2026-10-11). CI runs the same commands on Python
3.12 and 3.13.

## Repository layout

```
src/pulsestream/
  cli.py            pulsestream api | capture
  config.py         typed settings (PULSE_*)
  models.py         RawEvent envelope, WikiEvent domain event (+ from_raw filter)
  pipeline.py       producer / consumer over a bounded asyncio.Queue
  capture.py        record live events to JSONL
  sources/          EventSource protocol, WikimediaSource (SSE), FakeSource (replay)
  storage/duck.py   DuckDB schema, UTC sessions, idempotent micro-batch writes
  api/              FastAPI app factory and response schemas
tests/              unit, integration (fixture replay) and API contract tests
docs/
  hld.pdf           the high-level design this build follows
  tutorial.pdf      the guided-build tutorial it started from
  tutorial/         the worked tutorial for this repository, one chapter per phase
  decisions/        architecture decision records (ADR 0001-0006 so far)
```

## Reading the history

Each phase was built on its own branch and merged into `main` with a merge
commit, so `git log --oneline --first-parent main` shows one line per phase and
`git log --oneline <merge>^1..<merge>^2` lists that phase's individual steps.
The tutorial chapters walk through those steps in order and explain the
decisions behind them.
