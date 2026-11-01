# PulseStream

[![ci](https://github.com/dimakiskostas/Pulsestream/actions/workflows/ci.yml/badge.svg)](https://github.com/dimakiskostas/Pulsestream/actions/workflows/ci.yml)

Near-real-time event analytics pipeline with embedded anomaly detection.

Live edits from every Wikimedia project stream in over SSE, are validated into
typed events, land in DuckDB in micro-batches and in a partitioned Parquet lake,
and are served by a typed FastAPI API and a Streamlit dashboard. A statistical
anomaly detector, Prometheus metrics, Docker/CI/CD and a broker-based split were
added phase by phase. The design is in [`docs/hld.pdf`](docs/hld.pdf); the build is
written up as a tutorial in [`docs/tutorial/`](docs/tutorial/README.md), one git
branch per phase.

## How it works

```
Wikimedia EventStreams ──SSE──▶ WikimediaSource ──▶ asyncio.Queue ──▶ consumer ──▶ StreamProcessor
   (recentchange)              reconnect + resume    (bounded =        (worker        │   │   │
                                                      backpressure)     thread)       ▼   │   ▼
                                                                  Parquet lake        │  DuckDB wiki_events
                                                                  data/raw/dt=/hour=  │     └▶ agg_minutely
                                                                                      ▼
                                                   detection: TumblingCounter (per wiki, 60 s, event time)
                                                              EwmaBandDetector (count vs EWMA ± 3·std)
                                                              ByteDeltaDetector (|delta| vs median/MAD)
                                                              ──▶ features_rolling, anomalies
                                   APScheduler: refresh aggregates (60 s), compact lake (10 min)
                                   FastAPI: /health/live /health/ready /metrics /stats/latest /timeseries
                                            /aggregates/minutely /entities/top /lake /jobs /anomalies
                                            /anomalies/summary /features     ◀──HTTP── Streamlit dashboard
                                   structlog ──▶ JSON lines     prometheus_client ◀──scrape── Prometheus ──▶ Grafana
```

One API process owns the pipeline, the DuckDB writer, the detectors and the
scheduler. The dashboard is a second process that reads only the API.
`EventSource` is a protocol, so the live source, the replay `FakeSource`, the
lake's `ParquetSource` and the bus's `BusSource` are interchangeable, and a
replay from the lake reproduces the live findings exactly.

With `PULSE_BUS=redis` (what `docker compose` runs) ingestion and processing
are separate services that meet in a Redis Stream:

```
pulsestream ingest:  WikimediaSource ─▶ queue ─▶ publisher ──XADD──▶ Redis Stream pulsestream:events
                     /health/live /health/ready /metrics /stats          (6 h retention, AOF)
pulsestream api:     BusSource ◀──XREADGROUP (group "processors")──┘
                        └─▶ queue ─▶ consumer ─▶ StreamProcessor ─▶ XACK after the write
                     everything above, plus /bus;  pulsestream replay --from-bus for a past window
```

Killing and restarting either service loses nothing that reached the bus: an
unacknowledged batch is redelivered, and the idempotent writes make the
redelivery a no-op (at-least-once, [ADR 0017](docs/decisions/0017-delivery-at-least-once-ack-after-write.md)).
`/health/live` is the container healthcheck, `/health/ready` says whether a
service is doing useful work, and `/metrics` feeds Prometheus.
[`docs/operations.md`](docs/operations.md) is the runbook.

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
curl -s "localhost:8000/anomalies/summary?minutes=30"
curl -s "localhost:8000/anomalies?minutes=30&severity=critical&limit=5"
curl -s "localhost:8000/features?entity=commonswiki&minutes=30"
```

`events_total` climbs within seconds; `/aggregates/minutely` fills after the
first refresh job (every minute, or `POST /jobs/refresh_aggregates/run`);
findings appear once a wiki has ten minutes of history.

Other commands:

```bash
PULSE_SOURCE=fake PULSE_FIXTURE_DELAY=0.05 uv run pulsestream api     # offline replay of the fixture
uv run pulsestream replay --lake data/raw --db replay.duckdb          # rebuild a DB (and its findings) from the lake
uv run pulsestream replay --from-bus --since 2026-11-01T16:47:30 --until 2026-11-01T16:49:30 --db window.duckdb   # a window from the bus
uv run pulsestream check --db pulsestream.duckdb --lake data/raw      # pandera data-quality gates
uv run pulsestream eval                                               # detector precision/recall on synthetic spikes
uv run pulsestream capture -n 500 -o tests/fixtures/sample_events.jsonl
```

### With Docker

Requires Docker with BuildKit and the Compose plugin.

```bash
docker compose up --build -d                        # redis, ingest :8001, api :8000, dashboard :8501 (make up)
```

```bash
docker compose --profile monitoring up --build -d   # + Prometheus :9090, Grafana :3000 (make monitoring)
```

```bash
PULSE_SOURCE=fake docker compose up --build -d       # offline, replaying the committed fixture
```

The images log JSON, run as a non-root user and keep the database and the
lake in the `pulsestream-data` volume; the stream lives in the `redis-data`
volume. `curl localhost:8000/bus` shows the stream and the consumer group,
`curl localhost:8001/stats` what the ingest service published. Grafana shows the provisioned
"PulseStream operations" dashboard; Prometheus evaluates the alert rules in
[`docker/prometheus-rules.yml`](docker/prometheus-rules.yml).

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
| `PULSE_DETECTION_ENABLED`           | `true`               | run the detectors in the pipeline        |
| `PULSE_WINDOW_SECONDS` / `_LATENESS_SECONDS` | `60` / `10` | tumbling window and allowed lateness    |
| `PULSE_BAND_K` / `_WARMUP_WINDOWS`  | `3.0` / `10`         | EWMA ± k·std; windows before judging     |
| `PULSE_BYTE_DELTA_MIN_BYTES`        | `50000`              | smaller edits are never findings         |
| `PULSE_API_URL`                     | `http://127.0.0.1:8000` | what the dashboard reads              |
| `PULSE_LOG_LEVEL` / `_LOG_FORMAT`   | `INFO` / `console`   | `json` in containers                     |
| `PULSE_READY_MAX_SILENCE_SECONDS`   | `120`                | `/health/ready` is 503 after this long without a batch |
| `PULSE_BUS`                         | `none`               | `redis` splits ingestion (`pulsestream ingest`) from processing (`pulsestream api`) |
| `PULSE_REDIS_URL`                   | `redis://localhost:6379/0` | the bus                            |
| `PULSE_BUS_CONSUMER`                | host name            | this process's consumer name; fixed in deployments |
| `PULSE_BUS_RETENTION_MINUTES`       | `360`                | how far back the stream keeps entries    |

## Development

```bash
make check        # = ruff check, ruff format --check, mypy src, bandit, pytest (what CI runs)
make precommit    # = uv run pre-commit run --all-files
make image        # = docker build the three targets of docker/Dockerfile
```

Tests never touch the network: the live source is tested against an httpx mock
transport, the bus against fakeredis, the pipeline/API tests replay
`tests/fixtures/sample_events.jsonl` (500 real events captured on 2026-10-11),
the detectors are scored against synthetic series with injected anomalies, and
the dashboard runs headlessly under Streamlit's `AppTest`. CI runs the same
commands on Python 3.12 and 3.13, then builds, smoke-tests and scans the three
images on every change, pushes them to GHCR from `main` and from `v*` tags, and
deploys a tag over SSH when a server is configured
([`docs/operations.md`](docs/operations.md)).

## Repository layout

```
src/pulsestream/
  cli.py              pulsestream api | ingest | capture | replay | dashboard | check | eval
  config.py           typed settings (PULSE_*)
  wiring.py           which concrete source and bus the settings select
  common/             logging.py (structlog + stdlib, console or JSON), metrics.py (Prometheus series), backoff.py
  models.py           RawEvent, WikiEvent (+ from_raw filter), FeatureSnapshot, Anomaly
  pipeline.py         producer / micro-batch loop over a bounded asyncio.Queue; consumer with an ack hook
  bus/                EventBus protocol + wire format, RedisStreamsBus (groups, acks, claims, replay), monitor
  ingest/             the ingest service: publisher with retries, probes, /stats
  processing/         StreamProcessor (lake -> validate -> DuckDB -> detection),
                      windows.py (event-time tumbling counter), detection.py (the stage)
  detection/          features.py (EWMA, rolling std, MAD), rate.py (control band),
                      bytes.py (byte-delta MAD), eval.py (synthetic-anomaly evaluation)
  sources/            EventSource protocol; Wikimedia (SSE), Fake (replay), Parquet (lake), Bus (consumer group, bus replay)
  storage/            duck.py (schema, UTC, cursors), parquet.py (lake + compaction), aggregates.py
  scheduler/          APScheduler jobs: refresh aggregates, compact lake
  api/                FastAPI app factory and response schemas
  dashboard/          Streamlit page + its HTTP data layer
  quality.py          pandera gates for the lake and the tables
  replay.py, capture.py
tests/                unit, integration (fixture replay), API contract, detector evaluation, dashboard, quality,
                      logging, metrics, instrumentation, probes, bus (fakeredis), ingest, split pipeline, bus replay
docker/               Dockerfile (targets api, dashboard, ingest), Prometheus config + alert rules, Grafana provisioning
docker-compose.yml    redis + ingest + api + dashboard; --profile monitoring adds Prometheus and Grafana
.github/workflows/    ci.yml: test matrix, image build + scan + push, deploy on v* tags
docs/
  hld.pdf, tutorial.pdf      the design and the guided tutorial the build follows
  tutorial/                  the worked tutorial for this repository, one chapter per phase
  decisions/                 architecture decision records (ADR 0001-0017)
  operations.md              runbook: run with Docker, what to watch, release and deploy
```
