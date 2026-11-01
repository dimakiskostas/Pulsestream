# 7. The dashboard reads through the API, not the database

## Status

Accepted (Phase 2).

## Context

The HLD (D5, §7) asks for a functional dashboard, Streamlit or Grafana, and
recommends reading through the API where practical rather than straight from
the store. With DuckDB there is also a hard reason: one process holds the
read-write lock on the database file, and a second process cannot open it
read-only while the writer is up (ADR 0003). A Streamlit app is a second
process.

## Decision

- **Streamlit**, run as its own process (`pulsestream dashboard`), reading
  only HTTP endpoints of the API: `/stats/latest`, `/aggregates/minutely`,
  `/entities/top`, `/lake` and, from Phase 3, `/anomalies`.
- The API URL comes from `PULSE_API_URL` (default `http://127.0.0.1:8000`).
- Data access lives in `dashboard/data.py` as plain functions returning
  Polars frames, tested against an httpx mock transport; `dashboard/app.py`
  only renders. Streamlit is never imported by the tests.
- Refresh is a checkbox that re-runs the script every 10 s; a functional
  dashboard, not a designer-grade one (HLD non-goals).

## Alternatives considered

**Streamlit querying DuckDB directly.** Impossible while the API process
holds the writer lock, and it would duplicate every aggregation in a second
code base.

**Grafana over DuckDB.** Ops-realistic, but needs a DuckDB data-source plugin
and the same lock problem applies; Grafana arrives in Phase 4 on top of
Prometheus metrics, where it belongs.

**Server-side rendering inside FastAPI (templates + a charting library).**
No second process, but a frontend to maintain; Streamlit gives charts in a
few lines.

## Consequences

- Every number on the dashboard is available to any other client, and the
  OpenAPI contract is exercised by a real consumer.
- The API gains endpoints shaped for charts (`/aggregates/minutely`,
  `/entities/top`).
- Two processes to run locally; `make up` / docker-compose make that one
  command in Phase 4.
