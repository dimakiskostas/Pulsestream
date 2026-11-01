# 9. Derived tables are computed from validated events, not from raw Parquet

## Status

Accepted (Phase 2).

## Context

The tutorial suggests building `agg_minutely` directly over the Parquet lake
with `read_parquet(...)`. The lake, however, holds the raw envelope: the
payload is a JSON string, and the rules that turn it into a domain event
(which types to keep, how to derive `byte_delta`, what the entity is, ADR 0001)
live in `WikiEvent.from_raw`. Aggregating over raw files would mean writing
those rules a second time in SQL (`json_extract`, type filters) and keeping the
two in step forever.

## Decision

- `raw_events` is a DuckDB **view** over the lake, for ad-hoc SQL and for the
  replay path. Nothing derived is computed from it.
- `wiki_events` (validated by the single Python filter) is the base for every
  derived table.
- `agg_minutely` (`bucket_ts`, `entity`, `count`, `avg_value`, `max_value`,
  `anomaly_count`) is **recomputed over a sliding refresh window** (the last
  N minutes, default 180) with `INSERT OR REPLACE` on its primary key
  `(bucket_ts, entity)`, on a schedule (every 60 s). Recomputing instead of
  incrementing keeps it idempotent: a replayed or re-delivered batch cannot
  double-count.
- The replay path is `ParquetSource -> queue -> consumer -> StreamProcessor`,
  i.e. the live pipeline with a different source. One place validates; the
  lake, the table and the aggregates always agree.

## Alternatives considered

**SQL over raw JSON.** Duplicates the filter; the first divergence would be a
silent data-quality bug.

**Increment aggregates on the write path.** Lowest latency, but not idempotent
and impossible to backfill without special code.

**Materialised views.** DuckDB has none; a scheduled `INSERT OR REPLACE` is
the equivalent, with an explicit window.

## Consequences

- A change in the filter requires a replay from the lake into a fresh
  database; that is a feature, and `pulsestream replay` exists for it.
- Charts lag the stream by at most the refresh interval; `/timeseries` still
  answers from `wiki_events` for finer buckets.
- `anomaly_count` is zero until Phase 3 joins the anomalies table into the
  same refresh statement.
