# 04 · CI/CD and observability (Phase 4)

> Goal (HLD §13, Phase 4): make the system operable. Structured logs,
> Prometheus metrics, liveness and readiness probes, container images for the
> API and the dashboard, a compose stack with Prometheus and Grafana, and a
> pipeline that tests, builds, scans and publishes the images and deploys a
> tagged release.

Branch: `phase-4-cicd-observability`, 11 commits on top of Phase 3. Steps
below are the commits in order (`git log --oneline main..phase-4-cicd-observability`
once Phase 3 is merged).

## The system after Phase 4

```
                ┌─────────────── api container (uid 10001, /data volume) ────────────────┐
Wikimedia ─SSE─▶│ pipeline ─▶ lake + DuckDB ─▶ detectors        structlog ─▶ JSON lines  │─▶ docker logs
                │ /health/live  /health/ready  /metrics  + every Phase 1–3 endpoint       │
                └─────────────────────────────────────────────────────────────────────────┘
                        ▲ scrape (15 s)                ▲ HTTP                ▲ HEALTHCHECK
                  Prometheus + alert rules        dashboard container     docker / compose
                        ▲
                  Grafana (provisioned dashboard)

GitHub Actions:  push / PR ─▶ test (3.12, 3.13: ruff, mypy, bandit, pytest, eval)
                           ─▶ image ×2 (build, smoke, compose config, trivy) ─▶ GHCR (main, v* only)
                 tag v*    ─▶ deploy (SSH: IMAGE_TAG=v… docker compose pull && up -d)
```

New modules: `common/logging.py`, `common/metrics.py`. New files: `docker/`
(Dockerfile, Prometheus config and rules, Grafana provisioning and
dashboard), `docker-compose.yml`, `.dockerignore`, the rewritten
`.github/workflows/ci.yml`, and [`docs/operations.md`](../operations.md),
the runbook.

## Decisions made here

- [ADR 0013](../decisions/0013-deploy-target-vps-compose-over-ssh.md): the
  deploy target is one VPS running the compose stack, updated over SSH when
  a `v*` tag is pushed; secrets and the gate live in GitHub.
- [ADR 0014](../decisions/0014-observability-logs-metrics-probes.md): one
  log format for every logger, a fixed list of Prometheus series without an
  entity label, and two probes with different questions.
- [ADR 0015](../decisions/0015-images-built-in-ci-released-by-tag.md): CI is
  the only builder; images are built and scanned on every change and pushed
  only from `main` and tags; a tag is a release.

## Steps

### Step: *feat(observability): one logging configuration, console or JSON, with bound context*

structlog had been used unconfigured since Phase 1. `configure_logging()`
sets up structlog and the standard library together: one `ProcessorFormatter`
renders everything, the loggers that uvicorn, APScheduler and Streamlit
configure for themselves are stripped of their handlers and routed through
it, chatty libraries are capped at WARNING, and `PULSE_LOG_FORMAT` picks
`console` (readable) or `json` (containers). `bind_context()` attaches keys to
every line logged while bound; the processor binds each batch's first
`ingest_id` and its size, so one filter finds a batch's lines across the
lake, validation, DuckDB and the detectors.

**Gotcha: Streamlit's test harness hijacks the uvicorn loggers.** Running the
dashboard tests in the same process silently diverted uvicorn's stdlib lines:
`AppTest` configures those loggers with their own handlers and
`propagate=False`. Re-routing the known loggers on every
`configure_logging()` call makes the setup robust against any library that
does this.

### Step: *build: bandit code scan with a documented skip list; drop the assert in the counter*

Bandit's first pass raised twelve issues. One was real: an `assert` in
`TumblingCounter` that `python -O` would strip, now an explicit check. The
rest are accepted with the reason next to the skip list in `pyproject.toml`
(SQL assembled from fixed literals with values bound as parameters, `random`
for backoff jitter and synthetic series, one subprocess call with a fixed
argv and no shell). `make security` and CI run it.

### Step: *feat(observability): Prometheus metrics at /metrics, counted where the work happens*

Eight series (ADR 0014 has the table), incremented in the code that does the
work: the processor counts raw and kept events, the lag, the batch duration,
findings and failures; the consumer sets the queue depth; the source counts
reconnects and errors; the maintenance jobs count outcomes. Every label
combination is initialised at import so a panel shows 0, not "no data".
`GET /metrics` renders the registry and stays out of the OpenAPI schema.

**Gotcha: singletons make tests order-dependent.** The registry is
process-wide, so a test that asserted a counter *equals* 0 passed alone and
failed after any test that had processed a batch. The tests assert deltas
on the live registry and the presence of each series, never absolute values.

### Step: *feat(api): liveness and readiness probes*

`/health/live` answers 200 whenever the process serves and touches nothing:
that is what the container `HEALTHCHECK` watches. `/health/ready` answers
the question an operator and a load balancer need: consumer running,
producer not failed, store answering `SELECT 1` on its own cursor, source
connected (the live source tracks a `connected` flag across drops and
resumes), and a batch processed within `PULSE_READY_MAX_SILENCE_SECONDS`.
503 names the failing checks. Tests cover each way readiness fails.

### Step: *fix(logging): keep the formatter's internal keys out of rendered lines*

Found by the container smoke test, not by the test suite: every uvicorn line
in the JSON log carried `"_record": "<LogRecord ...>"` and
`"_from_structlog": false`, the keys `ProcessorFormatter` adds to carry a
stdlib record through the processor chain. `remove_processors_meta` has to
run before the renderer. The test had checked the stdlib line's `event`,
`level` and `logger` and never the whole line; it now asserts that no
rendered line has a key starting with `_`.

### Step: *build(docker): one multi-stage Dockerfile for the api and dashboard images*

Three stages. `builder` is the uv image: `uv sync --frozen --no-dev
--no-install-project` first (the dependency layer survives source changes),
then the project with `--no-editable` so the virtualenv is self-contained,
with bytecode compiled. `runtime` is `python:3.12-slim-bookworm` plus that
venv, a non-root user (uid 10001), `/data` for the database and the lake,
and the container defaults (`PULSE_LOG_FORMAT=json`, `/data` paths). The
`api` and `dashboard` targets add their port, `HEALTHCHECK` and command.
The healthchecks use `urllib` because the slim image has no curl.

**Gotcha: BuildKit cache mounts need buildx.** The `RUN --mount=type=cache`
lines keep uv's download cache between builds, and Ubuntu's `docker.io`
package in WSL ships no buildx, so `docker build` fails before reading the
file. The local verification used the legacy builder on a copy with those two
flags removed (the only difference); CI builds with buildx.

What the smoke test showed, running the api image with the fixture mounted
and `PULSE_SOURCE=fake`:

```
$ docker run --rm pulsestream-api:dev --version
pulsestream 0.1.0
$ docker exec ps-smoke id
uid=10001(app) gid=10001(app) groups=10001(app)
$ curl -s localhost:8014/health/ready
{"ready":true,"checks":{"consumer":true,"producer":true,"store":true,"source":true,"fresh":true},
 "last_processed_at":"2026-11-01T15:22:55.510060Z","silence_seconds":10.885}
$ curl -s localhost:8014/metrics | grep -E "^pulsestream_(ingest_events_total|queue_depth)"
pulsestream_ingest_events_total{stage="raw"} 500.0
pulsestream_ingest_events_total{stage="kept"} 306.0
pulsestream_queue_depth 0.0
$ docker inspect --format '{{.State.Health.Status}}' ps-smoke ps-dash
healthy
healthy
```

The images are 1.14 GB each, 703 MB of it the virtualenv (polars, pyarrow,
pandas, streamlit, duckdb, numpy). The api image carries the dashboard's
packages because they are one dependency set; moving the dashboard-only
packages to an extra is the obvious next cut.

### Step: *build(compose): api + dashboard stack with a monitoring profile*

`docker compose up --build` runs both images with the data volume; the
committed fixture is mounted read-only so `PULSE_SOURCE=fake` works offline.
The image names are the GHCR ones with `${IMAGE_TAG:-latest}`, so the same
file pulls what CI pushed on a server. `--profile monitoring` adds
Prometheus 3.15 (scraping `/metrics` every 15 s, six alert rules) and
Grafana 13.3 with the datasource and the "PulseStream operations" dashboard
provisioned from `docker/grafana/`: events per minute kept vs raw, lag,
queue depth, anomalies by severity, errors by stage, batch p95, reconnects
and job outcomes. `make up`, `make monitoring`, `make down`, `make logs`.

### Step: *ci: build, scan and publish the images; deploy on version tags*

The `test` job gains bandit and `pulsestream eval`, so the detector
scorecard is printed in every run. The `image` job builds both targets with
buildx and the Actions cache, loads them into the runner, runs `--version`,
validates `docker-compose.yml` with `docker compose config`, and scans with
Trivy (CRITICAL and HIGH, fixed vulnerabilities only, failing the job). The
push happens only from `main` (`latest`, `main`, `sha-…`) and from `v*` tags.
The `deploy` job runs for a tag when the `DEPLOY_ENABLED` variable is set,
in the `production` environment, over SSH (ADR 0013).

**Two details found while writing it.** The deploy job needs
`packages: read` for the token the host logs in with (the workflow's default
is `contents: read` only), and the metadata action's semver tag is `0.4.0`
while the deploy sets `IMAGE_TAG` to the ref name `v0.4.0`, so the tag name
is published as well; `docker compose pull` would otherwise look for an
image that does not exist.

### Step: *docs: ADRs 0013–0015, tutorial chapter 04, operations runbook, README*

This chapter, the three decision records, and
[`docs/operations.md`](../operations.md): run it with Docker, what to watch
(probes, every metric and what it means, the alert rules, reading the JSON
logs), the one-time server and GitHub setup for the deploy, releasing,
rolling back, backing up the volume, troubleshooting.

### Step: *ci: the trivy action's tags carry a v prefix*

The first run of the new workflow failed in "Set up job" of both image jobs:
`Unable to resolve action aquasecurity/trivy-action@0.36.0`. The action's
releases are tagged `v0.36.0`, and the runner resolves every action a job
uses before its first step, so a wrong version fails the job without running
anything. The `test` jobs were green; one character fixed the rest.

### Step: *build: upgrade certifi, pillow and streamlit past the CVEs trivy found*

The second run got past "Set up job" and failed on the scan itself: Trivy
reported one critical fixable vulnerability in certifi 2026.7.22 and 28 in
pillow 12.3.0, both transitive dependencies (Streamlit pulls pillow; nothing
in PulseStream imports it). `uv lock --upgrade-package` moved certifi;
pillow 13 was blocked by Streamlit 1.65's constraint and needed Streamlit
1.66 with it. The image matrix no longer fails fast, so both scans are
reported. This is the gate [ADR 0015](../decisions/0015-images-built-in-ci-released-by-tag.md)
describes, working as intended: red without a code change, green with a lock
update.

## Running it

```bash
docker compose up --build -d                        # api :8000, dashboard :8501
docker compose --profile monitoring up --build -d   # + Prometheus :9090, Grafana :3000
PULSE_SOURCE=fake docker compose up --build -d       # offline, from the committed fixture
curl -s localhost:8000/health/ready
curl -s localhost:8000/metrics | grep pulsestream_
docker compose logs -f api                          # JSON lines; grep an ingest_id to follow a batch
```

Without Docker, the same process serves everything:

```bash
PULSE_LOG_FORMAT=json uv run pulsestream api
```

## Definition of done

- `/metrics` exposes the eight series; `/health/live` and `/health/ready`
  answer the two questions, and readiness fails for each reason (silence,
  disconnected source, unreachable store, dead producer) in tests.
- Both images build from one Dockerfile, run as uid 10001, pass their
  `HEALTHCHECK`, and the api image replays the fixture to a ready state.
- The compose stack brings up the API, the dashboard and, with the profile,
  Prometheus with alert rules and Grafana with the dashboard.
- The workflow tests on two Python versions, builds, smoke-tests and scans
  both images on every change, pushes from `main` and tags, and deploys from
  a tag when enabled.
- Every commit on the branch passes ruff, mypy, bandit and pytest on its own.

## Deliberately left for later

- Image size: 1.14 GB each; a `dashboard` extra would take streamlit, pandas
  and pyarrow out of the api image.
- Alert rules without an Alertmanager: they show on Prometheus's Alerts page
  and nowhere else until a deployment wires one.
- The source's resume cursor (`Last-Event-ID`) lives in memory: a restart
  skips the events emitted while the process was down. Phase 5's broker is
  the remedy.
- Digest-pinned base images and action SHAs, with Dependabot to move them;
  an SBOM and image signing.
- The deploy job was written against the runbook, not run against a server:
  no VPS was available during this phase. It is gated by `DEPLOY_ENABLED`
  and skipped until one is.

Next: 05 · Streaming decoupling.
