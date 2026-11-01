# Operations runbook

Running PulseStream in containers, what to watch, and how a release reaches a
server. The decisions behind this page:
[ADR 0013](decisions/0013-deploy-target-vps-compose-over-ssh.md) (deploy
target), [ADR 0014](decisions/0014-observability-logs-metrics-probes.md)
(logs, metrics, probes) and
[ADR 0015](decisions/0015-images-built-in-ci-released-by-tag.md) (CI and
releases).

## Run it with Docker

Requires Docker with BuildKit and the Compose plugin: Docker Desktop, or Docker
Engine with the `docker-buildx` and `docker-compose-v2` packages.

```bash
docker compose up --build -d                        # api on :8000, dashboard on :8501
```

```bash
docker compose --profile monitoring up --build -d   # plus Prometheus on :9090 and Grafana on :3000
```

```bash
PULSE_SOURCE=fake docker compose up --build -d       # offline: replays tests/fixtures/sample_events.jsonl
```

```bash
docker compose --profile monitoring down            # stop everything; the volumes are kept
```

`make up`, `make monitoring`, `make down` and `make logs` are the same
commands. A `.env` file next to `docker-compose.yml` supplies the `${...}`
values (`PULSE_SOURCE`, `PULSE_LOG_LEVEL`, `PULSE_FIXTURE_DELAY`,
`GRAFANA_ADMIN_PASSWORD`, `IMAGE_TAG`); any other `PULSE_*` setting goes under
the `api` service's `environment`. Inside the images the database and the lake
live under `/data` (the `pulsestream-data` volume), logs are JSON on stderr,
and the process runs as uid 10001.

| URL                           | what                                                                            |
| ----------------------------- | ------------------------------------------------------------------------------- |
| http://localhost:8000/docs    | the API (OpenAPI)                                                               |
| http://localhost:8501         | the dashboard                                                                   |
| http://localhost:8000/metrics | Prometheus text exposition                                                      |
| http://localhost:9090/alerts  | Prometheus and the alert rules (monitoring profile)                             |
| http://localhost:3000         | Grafana: anonymous viewer, or admin / `GRAFANA_ADMIN_PASSWORD` (default `admin`) |

Grafana shows the "PulseStream operations" dashboard in its PulseStream
folder, provisioned from `docker/grafana/dashboards/pulsestream.json`. Edits
made in the UI are not written back (read-only mount, `disableDeletion`):
change the file and restart Grafana.

## What to watch

### Probes

| endpoint        | question               | 200 when                                                                                                                                       |
| --------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `/health/live`  | is the process up?     | always, while the server answers; this is the container `HEALTHCHECK`                                                                          |
| `/health/ready` | can it do useful work? | consumer running, producer not failed, store answers `SELECT 1`, source connected, a batch processed within `PULSE_READY_MAX_SILENCE_SECONDS` |

`/health/ready` answers 503 with each failing check named, so the reason is in
the response:

```json
{"ready": false,
 "checks": {"consumer": true, "producer": true, "store": true, "source": false, "fresh": false},
 "last_processed_at": "2026-11-01T13:04:12.418000Z", "silence_seconds": 184.2}
```

### Metrics

| series                                    | type      | watch for                                                           |
| ----------------------------------------- | --------- | ------------------------------------------------------------------- |
| `pulsestream_ingest_events_total{stage}`  | counter   | `kept` rate per minute flat at zero: the source or the pipeline stopped |
| `pulsestream_errors_total{stage}`         | counter   | any increase; `source` = reconnects' causes, `processing` = a batch failed, `job` = a maintenance job failed |
| `pulsestream_processing_lag_seconds`      | gauge     | a sustained rise (normal is 1 to 3 s: Wikimedia's own delay plus the micro-batch wait) |
| `pulsestream_queue_depth`                 | gauge     | near `PULSE_QUEUE_MAXSIZE` (1000): the consumer is slower than the stream and backpressure will block the source |
| `pulsestream_processing_duration_seconds` | histogram | p95 per micro-batch creeping up as the database grows               |
| `pulsestream_anomalies_detected_total{metric,severity}` | counter | a flood (hundreds an hour) means a detector needs recalibrating (ADR 0012) |
| `pulsestream_source_reconnects_total`     | counter   | more than a few an hour                                             |
| `pulsestream_job_runs_total{job,outcome}` | counter   | any `failed`                                                        |

### Alert rules

`docker/prometheus-rules.yml`, evaluated by the monitoring profile's
Prometheus and shown on its Alerts page; wiring an Alertmanager is a
deployment's choice.

| alert                     | fires when                                               |
| ------------------------- | -------------------------------------------------------- |
| `PulseStreamTargetDown`   | `/metrics` unreachable for 2 minutes                     |
| `PulseStreamNoEvents`     | no validated events for 10 minutes                       |
| `PulseStreamLagHigh`      | lag above 60 s for 5 minutes                             |
| `PulseStreamQueueNearFull`| queue depth above 900 for 2 minutes                      |
| `PulseStreamErrors`       | any error in the last 10 minutes                         |
| `PulseStreamJobFailing`   | a maintenance job failed in the last 30 minutes          |

### Logs

One JSON object per line on stderr (`PULSE_LOG_FORMAT=json`, the image's
default); `docker compose logs -f api` follows them:

```json
{"ingest_id": "0d2d1b4e-...", "batch": 50, "event": "batch processed", "kept": 47, "anomalies": 0, "logger": "pulsestream.processing.processor", "level": "info", "timestamp": "2026-11-01T13:04:12.418091Z"}
```

Every line emitted while a batch is being processed carries that batch's
`ingest_id` (its first event) and `batch` size, so
`docker compose logs api | grep '"ingest_id": "0d2d1b4e'` is the trace of one
batch through the lake, validation, DuckDB and the detectors. uvicorn,
APScheduler and httpx lines are in the same format.

## Releasing and deploying

### Once: the server

1. A Linux host with Docker Engine and the Compose plugin; a user (say
   `deploy`) in the `docker` group; its SSH public key in
   `~deploy/.ssh/authorized_keys`.
2. The repository checked out at the deploy path:
   `git clone https://github.com/dimakiskostas/Pulsestream /opt/pulsestream`.
   The compose file mounts `docker/prometheus.yml`, `docker/prometheus-rules.yml`,
   `docker/grafana/` and `tests/fixtures/` from it.
3. `/opt/pulsestream/.env` with `GRAFANA_ADMIN_PASSWORD` and any `PULSE_*`
   overrides. Do not set `IMAGE_TAG` there: the deploy job sets it per release.
4. Ports 8000 and 8501 (and 3000) reachable, or a reverse proxy with TLS in
   front of them.

### Once: GitHub

1. Settings → Environments → `production`, optionally with required
   reviewers. Secrets: `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY` (the
   private key, PEM; its public half is on the server).
2. Settings → Secrets and variables → Actions → Variables: `DEPLOY_ENABLED`
   = `true`, and optionally `DEPLOY_PATH` (default `/opt/pulsestream`).
3. Packages: the deploy job logs the host in to GHCR with its own
   `GITHUB_TOKEN` (read-only, expires with the job), so private images pull
   without a long-lived token on the server. Public packages need nothing.

Without step 2 the deploy job is skipped and the rest of the workflow still
runs, including the image push for the tag.

### Each release

```bash
git tag -a v0.4.0 -m "Phase 4: CI/CD and observability"
```

```bash
git push origin v0.4.0
```

The workflow runs test → image (pushes `v0.4.0` and `0.4.0` for both images)
→ deploy (`IMAGE_TAG=v0.4.0 docker compose pull && docker compose up -d`
over SSH). Then:

```bash
curl -s http://<host>:8000/health/ready
```

and on the host, `docker compose ps` and `docker compose logs --since 5m api`.

### Roll back

On the host, the previous release's images are still on GHCR:

```bash
IMAGE_TAG=<previous tag> docker compose up -d
```

### Back up the data

The volume holds the DuckDB file and the lake. Stop the API first for a
consistent copy (`docker volume ls` shows the exact volume name; compose
prefixes it with the project name):

```bash
docker compose stop api && docker run --rm -v pulsestream_pulsestream-data:/data -v "$PWD":/backup alpine tar czf /backup/pulsestream-data.tgz -C /data . && docker compose start api
```

## Troubleshooting

| symptom                                                   | cause and fix                                                                                                             |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `403` from `stream.wikimedia.org` in the logs             | Wikimedia rejects generic User-Agents; set `PULSE_USER_AGENT` to something descriptive with a contact URL                 |
| `/health/ready` is 503 with `fresh: false`                | nothing processed for `PULSE_READY_MAX_SILENCE_SECONDS`: the source is down (see `source`), or a replay has finished (expected) |
| the dashboard cannot reach the API                        | `PULSE_API_URL` must be reachable from the dashboard container (`http://api:8000` inside compose)                         |
| `docker build` fails on `RUN --mount=type=cache`          | the legacy builder is in use; install buildx (BuildKit), or build in CI                                                   |
| Grafana panels are empty                                  | Prometheus needs a scrape or two (15 s); check http://localhost:9090/targets                                              |
| CI's Trivy step fails on a base-image CVE                 | rebuild (the base image tag moved) or `uv lock --upgrade-package <name>`; `ignore-unfixed` already skips CVEs without a fix |
