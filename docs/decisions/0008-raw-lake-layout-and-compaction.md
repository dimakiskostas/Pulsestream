# 8. Raw lake layout and compaction

## Status

Accepted (Phase 2).

## Context

The HLD (§7, D4) wants raw events appended to partitioned Parquet as the
immutable source of truth, written in micro-batches, with a scheduled job that
compacts small files. The tutorial's gotcha is blunt: one Parquet file per
event destroys read performance. Three things had to be decided: the
partition key, the file granularity, and how compaction stays safe while a
writer keeps adding files.

At the observed rate (50–150 events/s, 50-event batches) the writer produces
one to three files per second: 100 000+ files a day without compaction.

## Decision

**Layout.** Hive-style directories readable by DuckDB with
`hive_partitioning=true`:

```
<root>/dt=YYYY-MM-DD/hour=HH/part-<received_ms>-<uid>.parquet    one per micro-batch
<root>/dt=YYYY-MM-DD/hour=HH/merged-<received_ms>-<uid>.parquet  produced by compaction
```

**Partition key = `received_at` in UTC**, i.e. when we got the event. It is the
write-time clock: monotone, under our control, and it makes "this partition is
closed" a statement about time having passed. `event_ts` (when the edit
happened) can be late and belongs to the data, not to the layout.

**Granularity.** One zstd Parquet file per micro-batch, written to a `.tmp`
sibling and renamed into place, so a reader globbing `*.parquet` never sees a
partial file. A batch that straddles an hour boundary is split per partition.

**Compaction.** Per hour partition, merge every file whose modification time
is older than a grace period (120 s) when there are at least two of them, into
one `merged-*` file written temp + rename; then delete the inputs. Merged
files take part in later rounds, so a partition converges to one file and the
work per round is bounded by one hour of data. The job runs every 10 minutes
from the scheduler. A reader that globs during the delete window may see a
vanished file; readers treat that as transient and retry.

**Schema** is pinned: `ingest_id`, `source`, `received_at` (Datetime µs UTC),
`schema_version`, `raw_payload`. The payload stays verbatim JSON; parsing is
the pipeline's job, not the lake's.

**The `raw_events` view derives `dt` and `hour` from `received_at`** instead
of reading them as hive partition columns. The values are identical by
construction, filters on `received_at` still prune Parquet row groups, and it
avoids a DuckDB 1.5.5 internal error ("Attempted to access index 5 within
vector of size 5") that occurs when aggregating hive columns over a glob that
matches exactly one file.

## Alternatives considered

**Partition by `event_ts`.** Natural for analysis, wrong for a lake fed by a
live stream: a late event lands in a partition that compaction already closed,
and "is this partition complete?" becomes unanswerable.

**Daily partitions only.** Simpler tree, but compaction would rewrite a whole
day each round.

**Bigger write batches instead of compaction.** Trades latency for file size
and still leaves the number of files unbounded over time.

**An open table format (Iceberg / Delta).** Gives ACID and time travel, but
adds a catalogue and a library for a problem this project does not have yet.
Listed as a stretch goal (HLD S3).

## Consequences

- The lake holds everything that arrived, including events the filter drops,
  so a filter change is a replay from disk rather than a re-ingest.
- File counts stay bounded; `GET /lake` shows files, partitions, bytes and rows.
- There is no retention yet. A lake that runs for weeks grows by roughly the
  compressed size of the stream; retention is a scheduler job for a later phase.
- Replaying needs to read files in `received_at` order across partitions;
  DuckDB's `ORDER BY` over the glob does that.
