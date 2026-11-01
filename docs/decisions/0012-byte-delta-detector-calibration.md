# 12. The byte-delta detector is a large-edit detector, calibrated on live data

## Status

Accepted (Phase 3). Supersedes the first defaults of the byte-delta detector.

## Context

ADR 0001 names "large byte deltas" as the secondary anomaly signal. The first
implementation scored an edit's size with a modified z-score (median and MAD)
over the wiki's last 200 edits, with a 1 000-byte floor and a threshold of
3.5. On synthetic tests it behaved. On the first live run with detection on
(13 minutes, 23 267 events, 144 wikis) it raised 1 512 findings:

| wiki         | edits | median size | MAD | p99     | findings |
| ------------ | ----- | ----------- | --- | ------- | -------- |
| commonswiki  | 4 223 | 437 B       | 610 | 7.9 KB  | 833      |
| wikidatawiki | 5 363 | 96 B        |     | 2.6 KB  | 452      |
| enwiki       | 1 432 | 21 B        |     | 6.0 KB  | 71       |

A routine 6 KB file-page edit on Commons scores z = 8 against a median of
437 bytes and was "critical". The question "is this edit far larger than this
wiki's usual edit?" is the wrong one when the usual edit is tiny and large
edits are routine. Across all wikis the same run had 85 edits of at least
10 KB, 23 of at least 20 KB and 4 of at least 50 KB.

## Decision

- The floor becomes **50 000 bytes**: a smaller edit is never a finding,
  whatever its z-score.
- The modified z threshold becomes **8**, so on wikis whose edits are large
  to begin with the MAD still governs, and on wikis of tiny edits the floor
  does.
- **Severity follows size**, not z-score: warn from the floor, high from
  2.5× (125 KB), critical from 5× (250 KB). A z-score is astronomical for
  any large edit on a wiki of tiny edits and carried no information.
- Expected volume at the defaults: a few findings an hour across all wikis,
  the same order of magnitude as the rate detector, so neither drowns the
  other on the dashboard.

## Alternatives considered

**Keep the relative test and drop the floor.** Precisely the failure seen.

**Per-wiki learned floors (e.g. p99.9 of the last day).** Better in
principle; needs a day of history per wiki and a place to keep it, and the
first week's worth of evidence says a global floor already separates the
routine from the remarkable.

**Rate-limit findings per wiki per minute.** Treats the symptom; a flood of
ordinary edits should produce zero findings, not three.

## Consequences

- The detector now answers "which edits are large in absolute terms, and
  unusual for their wiki?", which is what an operator looking at the panel
  wants to see.
- The floor is a setting (`PULSE_BYTE_DELTA_MIN_BYTES`); a deployment that
  cares about smaller edits on a specific wiki lowers it and accepts the
  volume.
- The evaluation harness still covers only the rate detector; the byte-delta
  detector's calibration rests on this one live run and should be revisited
  with a longer one.
