# 11. Detector quality is measured by injecting synthetic anomalies

## Status

Accepted (Phase 3).

## Context

There are no labels: nobody tells us which minutes on which wikis were
anomalous. The HLD (§10, §12) proposes the only honest alternative: inject
synthetic anomalies into a replayed or synthetic stream and measure recall
and precision against them. Without such a number, "the detector works"
would be an opinion.

## Decision

- `detection/eval.py` builds a per-minute Poisson series around a baseline
  with a slow daily swing, injects spikes (a multiple of the local count) at
  deterministic, well-separated positions after warm-up, runs a fresh
  detector over the series, and scores flagged windows against injected
  ones: true/false positives, false negatives, precision, recall, F1.
- `pulsestream eval` prints a fixed table: isolated ×4 spikes at
  k = 2.5 / 3 / 4, and a run of consecutive spikes with and without the
  winsorised baseline. The table is the regression check for any change to
  the detector or its defaults, and the tests pin its two claims (every
  isolated spike caught with precision ≥ 0.6; consecutive spikes stay visible
  with winsorising where a plain update hides the later ones).
- The synthetic baseline, not the real fixture, is the ground truth: the
  fixture is 20 seconds long, and a real minute of a real wiki cannot be
  labelled either way.

## Alternatives considered

**Hand-labelled incidents** (known bot runs, vandalism waves). Valuable as a
spot check, not reproducible and not available on day one.

**Comparing detectors against each other.** Measures agreement, not truth.

**Injecting into the real fixture only.** Too short for warm-up and too
narrow (one 20-second slice) to say anything about false alarms.

## Consequences

- Precision and recall are reported for an idealised Poisson world; real
  wikis are burstier, so expect more false alarms in production than the
  table shows, and treat the table as a lower bound on alarms, not an upper one.
- Any detector change must keep the table's tests green; a deliberate
  trade-off (say, k = 4 for fewer alarms) is a one-line change plus a note
  in this record.
- Byte-delta findings are not covered by the harness yet; they are judged by
  the MAD threshold and the minimum size, and remain an open evaluation item.
