# 03 · Anomaly detection v1 (Phase 3)

> Goal (HLD §13, tutorial Part 3 / Phase 3): flag unusual activity in near
> real time. Maintain rolling features per entity (rate, EWMA, rolling std),
> flag points outside EWMA ± k·std, write them to an anomalies table, expose
> `/anomalies`, show them on the dashboard, and evaluate the detector with
> synthetic anomalies because the stream has no labels.

Branch: `phase-3-anomaly-detection`, 11 commits on top of Phase 2. Steps
below are the commits in order (`git log --oneline main..phase-3-anomaly-detection`).

## The system after Phase 3

```
events ─▶ StreamProcessor ─▶ DuckDB wiki_events
             │
             ├─▶ ByteDeltaDetector (per edit: |delta| vs median/MAD of the wiki's last 200 edits)
             │
             └─▶ TumblingCounter (per wiki, 60 s windows, event time + watermark)
                      │  closed windows (zeros included)
                      ▼
                 EwmaBandDetector (count vs EWMA ± 3·std, warm-up, floors, winsorised)
                      │
                      ├─▶ features_rolling  (every window with the baseline that judged it)
                      └─▶ anomalies         (deterministic ids: a replay never duplicates one)
                                   ▲
            agg_minutely.anomaly_count ◀── join ──┘        /anomalies  /anomalies/summary  /features
                                                           dashboard: findings panel + control-band chart
                                                           pulsestream eval: precision/recall table
```

New modules: `detection/features.py`, `detection/rate.py`,
`detection/bytes.py`, `detection/eval.py`, `processing/windows.py`,
`processing/detection.py`. New command: `pulsestream eval`.

## Decisions made here

- [ADR 0010](../decisions/0010-rate-detector-windows-and-guards.md): tumbling
  60-second windows in event time, EWMA (α 0.3) ± 3·std over 30 windows, four
  guards (warm-up, std floor and minimum deviation, winsorised updates, reset
  on discontinuity), drops count.
- [ADR 0011](../decisions/0011-detector-evaluation-by-injection.md): quality
  is measured by injecting synthetic spikes and reporting precision and recall.
- [ADR 0012](../decisions/0012-byte-delta-detector-calibration.md): the
  byte-delta detector is a large-edit detector (50 KB floor, z above 8,
  severity by size), calibrated on the first live run.

## Steps

### Step: *feat(detection): pure feature math (EWMA, rolling mean/std, MAD z-score)*

Statistics before models (HLD D6). `ewma_update`, `RollingStats`,
`median_abs_deviation`, `modified_zscore`, `zscore`, `clip`: no I/O, every
number pinned by a test. The modified z-score scales the MAD by 0.6745 so it
compares with an ordinary z-score, and is undefined, not infinite, when the
MAD is zero.

### Step: *feat(storage): FeatureSnapshot and Anomaly records, their tables, anomaly_count*

Two records and two tables. The decision that matters: **anomaly ids are
deterministic** (`uuid5` over method, entity, metric and the window start or
event id). A replay of the same data produces the same ids and the insert is
skipped rather than doubled, the same idempotency the events table has
([ADR 0005](../decisions/0005-at-least-once-idempotent-writes.md)).
`agg_minutely.anomaly_count` is finally real: the refresh joins the anomalies
table by minute and entity.

### Step: *feat(processing): event-time tumbling windows per entity with a watermark*

Counting edits per wiki per minute is the hard part of this phase, not the
statistics. `TumblingCounter` uses event time with a 10-second lateness
allowance, so a replay reproduces the live windows exactly; a window closes
when the watermark passes its end; an event for a closed window is counted as
late and dropped; a wiki that goes quiet keeps producing zero windows (a drop
to zero is a signal), and after an hour of silence it is forgotten.

**Gotcha: zeros must be emitted as soon as they are known.** The first version
only filled a quiet wiki's gap when its next event arrived. Tracing the tests
by hand showed the zero windows have to go out as the watermark passes them,
driven by *other* wikis' events, or the rate detector never sees a drop.

### Step: *feat(detection): EWMA control-band detector for per-entity edit rates*

The verdict uses the baseline *as it stood before the window*, then the
baseline is updated. The four guards are what make this usable on 900 wikis
at once; without the std floor and the minimum absolute deviation, a wiki
averaging one edit a minute raises a "critical" on four edits.

**Winsorised update.** A flagged count is clipped to the band before it
enters the baseline. The evaluation step shows why.

### Step: *feat(detection): MAD-based byte-delta detector for individual edits*

The secondary signal from [ADR 0001](../decisions/0001-event-filter-entity-and-headline-anomaly.md):
an edit far larger than the wiki's recent edits. Median and MAD, because a few
huge edits would drag a mean and a standard deviation around. A 1 000-byte
minimum keeps tiny wikis quiet.

### Step: *feat(processing): run the detectors inside the pipeline; persist features and findings*

`DetectionStage` feeds each validated batch, in event-time order, to both
detectors and the counter; `StreamProcessor` persists what comes back and
counts it (`BatchResult.anomalies`). `detected_at` is left to the detectors
(window end, edit receipt), so a replay reproduces the live findings exactly.
`finish()` closes the open windows at the end of a replay only: live, open
windows are partial and are not written. Settings gain the detection knobs
(`PULSE_WINDOW_SECONDS`, `PULSE_BAND_K`, `PULSE_WARMUP_WINDOWS`, ...).

### Step: *feat(api): /anomalies, /anomalies/summary and /features*

Findings newest first with filters on entity, metric and severity (the
`Literal` types from the models, so an unknown value is a 422); a summary by
severity, metric and entity; and `/features?entity=` returning every closed
window with the band it was judged against, so a chart can draw the band and
mark the flagged windows.

### Step: *feat(dashboard): anomalies panel and a control-band chart*

Findings count, critical count, metric split, the recent findings table, and
a control-band chart (count, EWMA, upper, lower) for the busiest wiki or any
wiki typed into the sidebar.

### Step: *feat(detection): evaluate the rate detector against injected anomalies*

No labels, so: a Poisson baseline (20/min) with a slow daily swing, twelve
×4 spikes at well-separated positions, and a run of four equal spikes at 3.5×
the baseline. `pulsestream eval` prints:

```
scenario                                     precision  recall     f1   tp   fp   fn
------------------------------------------------------------------------------------
12 isolated spikes x4, k=2.5                      0.50    1.00   0.67   12   12    0
12 isolated spikes x4, k=3                        0.75    1.00   0.86   12    4    0
12 isolated spikes x4, k=4                        1.00    0.92   0.96   11    0    1
4 consecutive spikes x3.5, winsorised baseline      0.40    1.00   0.57    4    6    0
4 consecutive spikes x3.5, plain baseline         0.25    0.50   0.33    2    6    2
```

Reading it: `k = 3` is the trade-off (`k = 2.5` doubles the false alarms for
no extra recall, `k = 4` starts missing spikes). With a plain EWMA update the
run of equal spikes is half missed: each spike lifts the baseline and widens
the band, so the third and fourth sit inside it, exactly the failure the HLD's
figure 3 illustrates. Winsorising keeps all four visible. The six false
positives in the last two rows are the same Poisson noise as the four in the
`k = 3` row plus the drift the run leaves behind.

**Gotcha: the first version of this scenario proved nothing.** It multiplied
each *local* count by four, so the third spike happened to be the largest and
even the inflated plain band caught it. Equal spikes are what expose the
masking. A trace of that run also showed winsorising's price: a run of clipped
spikes still lifts the EWMA by up to k·std per window, and the normal minute
after it registered as a *drop* (z = −3.5). ADR 0010 records the caveat.

### Step: *fix(detection): calibrate the byte-delta detector on a live run*

See "What the first live run taught" below: the defaults that behaved on
synthetic tests flooded on Commons; the run's own data set the new ones.

### Step: *docs: ADRs 0010–0012, tutorial chapter 03, README*

This chapter and the three decision records.

## Running it

```bash
uv run pulsestream api                     # detection on by default; findings after ~10 min of history
curl -s "localhost:8000/anomalies/summary?minutes=30"
curl -s "localhost:8000/anomalies?minutes=30&severity=critical&limit=5"
curl -s "localhost:8000/features?entity=commonswiki&minutes=30"
uv run pulsestream eval                    # the table above
uv run pulsestream replay --lake data/raw --db replay.duckdb   # findings reproduced from the lake
```

### What the first live run taught

A 13-minute run on 2026-11-01 (23 267 events from 144 wikis, 1 662 events in
the last minute, 1.0 s lag) with the detectors on, sampled after the ten-minute
warm-up:

```
$ curl -s "localhost:8012/anomalies/summary?minutes=30"
{"minutes":30,"total":1511,"by_severity":{"warn":215,"high":236,"critical":1060},
 "by_metric":{"edit_rate":4,"byte_delta":1507},
 "top_entities":[{"entity":"commonswiki","count":833},{"entity":"wikidatawiki","count":452},{"entity":"enwiki","count":71},...]}
```

Four edit-rate findings in 13 minutes across 144 wikis read well:

| window | wiki         | count | expected | z    | severity |
| ------ | ------------ | ----- | -------- | ---- | -------- |
| 13:03  | enwikisource | 25    | 0.7      | 3.7  | warn     |
| 13:03  | hewiki       | 13    | 5.1      | 5.3  | high     |
| 13:04  | enwikisource | 34    | 6.6      | 3.4  | warn     |
| 13:04  | fawiki       | 4     | 20.7     | −3.9 | warn (a drop) |

**Gotcha: 1 507 byte-delta findings did not.** 833 were on commonswiki,
where the usual edit is 437 bytes with a MAD of 610, so a routine 6 KB
file-page edit scored z = 8 and was "critical". The question "far larger than
this wiki's usual edit?" is the wrong one when the usual edit is tiny. The
run's own database gave the calibration: 85 edits of 10 KB or more, 23 of
20 KB, 4 of 50 KB, across every wiki. The step *fix(detection): calibrate the
byte-delta detector on a live run* and
[ADR 0012](../decisions/0012-byte-delta-detector-calibration.md) make it a
large-edit detector: a 50 KB floor, z above 8, severity by size.

The band for the busiest wiki at that moment, `/features?entity=enwiktionary`
(a bot run of ~450 edits a minute; the first three windows are warm-up, no band):

```
2026-11-01T13:02:00Z count=440 ewma=451.9 std=104.4 z=-0.11 band=[138.8, 765.0]
2026-11-01T13:03:00Z count=445 ewma=448.3 std= 99.0 z=-0.03 band=[151.2, 745.4]
2026-11-01T13:04:00Z count=457 ewma=447.3 std= 94.5 z= 0.10 band=[164.0, 730.7]
```

**Replay reproduces the findings.** The run's lake (98 956 raw events in
Parquet) replayed with the recalibrated detector into a fresh database:

```
$ uv run pulsestream replay --lake data/raw --db replay.duckdb
replayed 98956 raw events ...: 23267 kept, 23267 rows, 962 minute buckets aggregated, 6 anomalies
```

The same four edit-rate findings with the same ids as the live run (4 of 4,
none extra), plus two byte-delta findings worth seeing: a 169 569-byte
removal from a user page on zhwiki and the same content put back a minute
later, both "high". The replay has 56 more feature rows than the live run
because `finish()` closed the last open window of every wiki.

## Definition of done

- A synthetic-anomaly test asserts the detector catches injected spikes
  (recall 1.0 on isolated ×4 spikes, precision ≥ 0.6) and that winsorising
  keeps consecutive spikes visible where a plain baseline hides them.
- An anomalies panel and a control-band chart on the dashboard; `/anomalies`,
  `/anomalies/summary` and `/features` in the API; `anomaly_count` in the
  chart table.
- Every commit on the branch passes ruff, mypy and pytest on its own.

## Deliberately left for later

- Detector state lives in memory: a restart loses the open windows and the
  baselines and every wiki warms up again. Phase 5's broker replay is the
  intended remedy.
- No evaluation harness for the byte-delta detector yet.
- No model (IsolationForest, HLD stretch S1); the features table and the
  evaluation harness are what a model would be measured against.

Next: 04 · CI/CD and observability.
