# 1. Event filter, entity grain, and headline anomaly

## Status

Accepted

## Context

PulseStream ingests the Wikimedia EventStreams `recentchange` feed. Every
downstream component — rolling features, the anomaly detector, the API and the
dashboard — depends on three questions that must be answered together, because
each constrains the others:

1. **Which event types become domain events?** The feed carries `edit`, `new`,
   `log`, `categorize` and others. `categorize` is high-volume, almost entirely
   bot-generated category-membership bookkeeping. `log` records administrative
   actions. Neither is a content change.
2. **What is the `entity` grain?** `entity` is the grouping key for all rolling
   features (rate, EWMA, rolling std). The HLD sketch suggested `wiki:title`.
3. **What is the headline anomaly?** The HLD requires this to be stated
   explicitly (§5), and it determines what the detector is actually modelling.

Two observations from real captured payloads drove the decision:

- `log` and `categorize` events have **no `length` key at all**, so `byte_delta`
  is not merely zero for them — it is not applicable.
- The feed is global across ~900 wikis and all namespaces, so any grouping key
  choice has a large effect on per-entity sample size.

## Decision

**Event filter.** `from_raw` keeps only `type` in (`edit`, `new`) and returns
`None` for everything else. Filtering is explicit — it is *not* delegated to an
exception handler, so that a deliberate drop and a parser bug remain
distinguishable.

**Entity grain.** `entity` is the **wiki** (`enwiki`, `cewiki`, …). `title` and
`namespace` are retained as context columns but are not grouping keys.

**Headline anomaly.** *An unusual spike in the edit rate of a single wiki*,
detected with an EWMA control band (EWMA ± k·rolling std, k = 3 initially).

`type` and `is_bot` are stored even though `type` is filtered to two values,
because "a surge of page creations" and "a surge of bot activity" are distinct
signals available at no additional ingest cost.

## Alternatives considered

**Keep all event types, discriminate at query time (ELT).** Rejected. The
approach is sound in general and preserves optionality, but that optionality is
already provided by the immutable partitioned Parquet raw store (Phase 2), which
retains every event unfiltered. Applying it a second time in the domain table
would force `byte_delta` to become nullable — conflating "an edit that changed
nothing" (0) with "byte change is not applicable" (NULL) — and would mean every
downstream query begins with a `WHERE` clause that will eventually be forgotten.

**`entity` = `wiki:title`.** Rejected. Almost every article receives a single
edit within any realistic window, so per-entity n ≈ 1. A rolling mean and
standard deviation over one observation has no meaning, and a control band
cannot be computed. Per-wiki grouping yields tens of active entities with
thousands of events each — a distribution that can actually be modelled.

**Bot-ratio shift as the headline anomaly.** Deferred, not rejected. A ratio is
normalised against overall volume and is therefore robust to diurnal and weekly
swings, which makes it statistically the more attractive signal. It is deferred
because a single detector with a rigorous synthetic-anomaly evaluation is worth
more than two detectors with none, and because it reuses the same rolling
features and can be added later at low cost.

## Consequences

### Positive

- `byte_delta` remains non-nullable `int`. `new` events are treated as a change
  from 0 to `length.new`, which is semantically honest.
- The `events` table means exactly one thing: content changes on Wikimedia. No
  implicit filter is required in downstream queries.
- The detector has a defensible population. "Edits per minute per wiki" is a
  statement that can be explained and defended; "events per minute" over a
  mixture of human edits and bot bookkeeping in unknown proportion is not.
- Per-entity sample size is large enough for EWMA and rolling standard deviation
  to be meaningful.

### Negative

- Article-level anomalies (edit wars, repeated vandalism of one page) are not
  detectable at this grain.
- Bot and administrative behaviour is invisible to the domain table during
  Phase 1, when no raw Parquet store exists yet. Phase 1 data is treated as
  disposable, so this is accepted.
- A wiki with a genuinely low edit rate has a wide relative control band and will
  under-report. Not addressed in v1.

### Revisit if

- Bot-behaviour anomalies become a project goal — reprocess from the Parquet raw
  store with an expanded filter; no re-ingestion is required.
- Article-level detection becomes desirable — this requires a second, finer
  feature table rather than a change to this grain, since the two have different
  statistical properties.
- The proportion of `edit` versus `new` events turns out to be strongly skewed,
  which would argue for separating them rather than pooling them into one rate.
