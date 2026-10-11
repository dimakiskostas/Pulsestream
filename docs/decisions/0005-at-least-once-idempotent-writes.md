# 5. At-least-once delivery with idempotent writes

## Status

Accepted (Phase 1). Phase 5 extends it to the broker (acks after write).

## Context

The HLD (D2, §4 non-goals) rules out exactly-once delivery: it is expensive
to build and rarely worth it for a solo project. The alternative it names is
at-least-once delivery plus idempotent writes, deduplicating on `event_id`.
Duplicates have two concrete sources in Phase 1:

- a reconnect that resumes with `Last-Event-ID`: the server replays from
  the last id it saw, which may overlap events already processed;
- a replay of a fixture or (later) of a Parquet partition into a database
  that already holds some of those events.

`meta.id` in the recentchange schema is a UUID that is unique across all
wikis; the numeric `id` is only unique per wiki, so it is not usable as a key.

## Decision

- `WikiEvent.event_id` is `meta.id`.
- `wiki_events.event_id` is the PRIMARY KEY and every write is
  `INSERT ... ON CONFLICT (event_id) DO NOTHING`, so re-delivering a batch
  changes nothing.
- The source resumes with the `Last-Event-ID` header after a drop, accepting
  the resulting overlap.

## Alternatives considered

**Exactly-once via transactional offsets.** Needs a broker with transactional
producers and a store that participates in the same transaction. Not
available with DuckDB and not needed.

**Dedupe in memory (a set of seen ids).** Lost on restart, grows without
bound. The database already has an index on the key; use it.

**At-most-once (never resume, accept gaps).** Simpler, but gaps corrupt the
rate features the anomaly detector depends on; a missing minute looks like a
dip in activity.

## Consequences

- Replaying anything is always safe; the integration test replays the
  fixture twice and asserts the row count is unchanged.
- The PRIMARY KEY adds an ART index that DuckDB maintains on every insert.
  At our volume (hundreds of rows per second at most) the cost is
  negligible; at millions of rows per batch it would need revisiting.
- Downstream aggregates derived from `wiki_events` inherit idempotency as
  long as they are recomputed from the table rather than incremented on
  the write path.
