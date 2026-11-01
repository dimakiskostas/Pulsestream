# 14. Observability: one log format, a fixed set of Prometheus series, two probes

## Status

Accepted (Phase 4).

## Context

Until Phase 4 the process logged through an unconfigured structlog (every
line, every level, to stdout, in a format uvicorn and APScheduler did not
share), exposed no metrics, and answered `/health` with "ok" whenever it was
running. The HLD (§11) asks for structured logs, metrics for ingest rate,
processing lag, queue depth, errors and anomalies, and health endpoints.
Running in containers (ADR 0013) sharpens the requirements: logs are read by
a collector, not a person; something external decides when to restart the
container and when to send it traffic; and the dashboards must be
reproducible from the repository.

## Decision

**Logs.** `common/logging.py` configures structlog and the standard library
together, once, from the CLI: structlog renders every line, and the loggers
that uvicorn, APScheduler and Streamlit set up for themselves are stripped of
their handlers and routed through the same `ProcessorFormatter`.
`PULSE_LOG_FORMAT=console` while developing, `json` in containers;
`PULSE_LOG_LEVEL` filters both. Each micro-batch is processed inside a bound
context (`ingest_id` of its first event, `batch` size), so every line from
every stage of one batch can be found with one filter.

**Metrics.** `prometheus_client`, pull model, served by the API process at
`GET /metrics` (outside the OpenAPI contract). The series are a fixed, short
list, each incremented where the work happens:

| series                                    | type      | labels                        |
| ----------------------------------------- | --------- | ----------------------------- |
| `pulsestream_ingest_events_total`         | counter   | `stage` raw / kept            |
| `pulsestream_errors_total`                | counter   | `stage` source / processing / job |
| `pulsestream_processing_lag_seconds`      | gauge     |                               |
| `pulsestream_queue_depth`                 | gauge     |                               |
| `pulsestream_processing_duration_seconds` | histogram |                               |
| `pulsestream_anomalies_detected_total`    | counter   | `metric`, `severity`          |
| `pulsestream_source_reconnects_total`     | counter   |                               |
| `pulsestream_job_runs_total`              | counter   | `job`, `outcome`              |

Every label combination is initialised at import, so a series exists (at
zero) from the first scrape and a panel shows "0" rather than "no data".
There is **no `entity` label**: hundreds of wikis times severities would
multiply the series for no operational value, and per-wiki questions are
answered by DuckDB through `/anomalies` and `/features`.

**Probes.** Two endpoints with different questions. `/health/live`: is the
process up; touches nothing; 200 while the server answers. `/health/ready`:
can the pipeline do useful work; the consumer task is running, the producer
has not failed, the store answers `SELECT 1`, the source reports itself
connected, and a batch was processed within
`PULSE_READY_MAX_SILENCE_SECONDS`; 503 with the failing checks named. The
container `HEALTHCHECK` uses liveness; readiness is for a load balancer and
for alerting.

**Dashboards as code.** The Prometheus scrape config, its alert rules and
the Grafana datasource and dashboard are files under `docker/`, provisioned
at start-up by the compose `monitoring` profile.

## Alternatives considered

**OpenTelemetry for logs, metrics and traces.** The complete answer, and a
collector, an exporter and a backend more than this system needs. Traces add
little to a single process whose stages run in one thread per batch; they
become interesting at Phase 5, when a batch crosses a broker between two
services, and the bound log context is the seam a trace id would go through.

**Push metrics (StatsD, Pushgateway).** Pull suits a long-running server
with a stable address; push suits short-lived jobs. The maintenance jobs run
inside the server process, so there are no short-lived jobs.

**One `/health` with a body.** Kept for compatibility, but an orchestrator
needs the two answers separately: a stalled pipeline must stop receiving
traffic without being restarted into losing its state, and a hung process
must be restarted even though its last answer looked fine.

## Consequences

- Metrics are per process. After the Phase 5 split, each service exposes its
  own `/metrics` and Prometheus scrapes both; the series names stay.
- `pulsestream_processing_lag_seconds` measures event time to processing
  time, so it includes Wikimedia's own publication delay (about a second) and
  the micro-batch wait (up to `PULSE_FLUSH_INTERVAL`). A sustained rise is
  the signal, not the absolute value.
- The readiness "fresh" check assumes a stream that never pauses, which the
  `recentchange` stream does not. A replay deployment or a quiet custom
  source raises `PULSE_READY_MAX_SILENCE_SECONDS`.
- Logs go to stderr only; rotation and shipping are the container runtime's
  job (`docker compose logs`, or a log driver).
- Found while testing: Streamlit's test harness configures the uvicorn
  loggers with `propagate=False`, which silently diverted stdlib lines in any
  process that had imported it. Re-routing those loggers on every
  `configure_logging()` call makes the configuration robust against libraries
  that do this.
