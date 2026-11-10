# 10. The rate detector: tumbling minutes, an EWMA band, and four guards

## Status

Accepted (Phase 3).

## Context

The HLD leaves three things to decide for anomaly detection v1 (§7, §12):
the window size and whether windows tumble or slide, the threshold `k` of
the EWMA ± k·std band, and how to keep a statistical detector honest with no
labels. ADR 0001 fixed the grain (entity = wiki) and the headline anomaly
(an edit-rate spike per wiki). Statistics come before any model (D6).

Two facts about the stream shape the choices. Most wikis are quiet: a few
edits an hour, with minutes of nothing. A few are busy: tens to hundreds of
edits a minute. One detector has to make sense of both.

## Decision

**Windows: tumbling, 60 seconds, event time.** Counts per wiki per minute
line up with `agg_minutely` and with the dashboard's charts. Tumbling rather
than sliding windows keep every edit in exactly one observation, which makes
the series the EWMA sees independent and the statistics defensible. Windows
are driven by `event_ts` with a 10-second lateness allowance (ADR 0008's
reasoning: a replay must reproduce the live windows exactly); a quiet wiki
keeps producing zero windows so a drop is observable, and after an hour of
silence it is forgotten and warms up again when it reappears.

**Baseline: EWMA (α = 0.3) and a rolling sample std over 30 windows**, both
updated after the verdict, never before. `k = 3`: the evaluation (ADR 0011)
shows k = 2.5 doubling the false alarms for no extra recall and k = 4
starting to miss spikes.

**Four guards.**

1. *Warm-up:* ten windows before an entity is judged.
2. *A std floor of one event and a minimum absolute deviation of five*: a
   wiki averaging one edit a minute cannot raise a finding on four edits,
   whatever the z-score says.
3. *Winsorised updates:* a flagged count is clipped to the band before it
   enters the baseline. The HLD's figure 3 shows what happens otherwise: a
   spike widens the band and hides the next one.
4. *Reset on discontinuity:* when the counter has forgotten an entity, its
   old statistics do not judge the fresh series.

**Drops count.** A busy wiki going silent is an anomaly worth surfacing
(an outage, a bot stopping), scored negative; it can be switched off.

**Severity** follows the magnitude of the z-score: warn (3–5), high (5–8),
critical (≥ 8).

## Alternatives considered

**Sliding windows.** Lower detection latency, but overlapping observations
are correlated and the band's std no longer means what the formula says.

**Per-article grain.** Rejected in ADR 0001: n ≈ 1 per window.

**Seasonal baselines (same minute last week).** Better for strongly periodic
wikis, but needs a week of history per wiki and a store for it; the EWMA
tracks the diurnal swing well enough at α = 0.3, as the evaluation's drifting
baseline shows.

**IsolationForest.** The HLD's v2; a model needs the features this detector
produces and the evaluation harness this phase builds. Stretch goal S1.

## Consequences

- Detection latency is one minute plus the lateness allowance plus a flush.
- Every closed window is written to `features_rolling` with the baseline that
  judged it, so a finding can be explained after the fact and the dashboard
  can draw the band.
- State is in memory: a restart loses the open windows and the baselines, and
  every wiki warms up again (ten minutes). Phase 5's broker replay is the
  intended remedy.
- Small wikis are judged conservatively by design; article-level vandalism
  is invisible at this grain (ADR 0001).
- Winsorising has a price: a run of clipped spikes still lifts the EWMA by up
  to k·std per window, so for a few minutes after the run the band sits high
  and a normal minute can register as a drop. The minimum absolute deviation
  limits how often; the Phase 3 live trace showed one such drop finding.
