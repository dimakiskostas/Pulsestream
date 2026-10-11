# 6. Replay fixtures instead of the live source in tests

## Status

Accepted (Phase 1).

## Context

The HLD (§10) states the testing problem: the source is live and
non-deterministic, and CI must not touch the network. Its answer is replay:
capture real events once, commit them, and run every test against a source
that replays the capture.

## Decision

- `pulsestream capture -n 500` records raw events (the `RawEvent` envelope,
  one JSON per line) to `tests/fixtures/sample_events.jsonl`. The committed
  sample is ~20 s of the recentchange stream captured on 2026-10-11:
  500 raw events, 306 of which pass the Phase 1 filter.
- `FakeSource` replays a list of `RawEvent`s through the same `EventSource`
  protocol as the live source, optionally paced for demos.
- The fixture's composition (event types, kept count, bot count, top wiki)
  is pinned in the integration test so a change in the filter or a
  truncated fixture fails loudly.
- Sources themselves are tested against an httpx `MockTransport` that
  scripts the edge's behaviour (403, 429, 5xx, dropped connections).
- The live stream is exercised by humans and smoke runs, never by CI.

## Alternatives considered

**Mock at the HTTP layer only.** Enough for the source, useless for the
pipeline and the API, which need realistic payload variety (the fixture
has 900-wiki diversity, bots, new pages, categorize noise).

**Synthetic generator only.** Deterministic and small, but it encodes the
author's assumptions about the schema; the real sample caught that `log`
and `categorize` events carry no `length` key.

**Record/replay cassettes (VCR-style).** Heavier tooling for the same
outcome; the JSONL envelope is already the project's own format.

## Consequences

- The fixture is 777 KB of text. It is marked `-diff` in `.gitattributes`
  and the pre-commit large-file limit is 1 MB; a bigger sample should be
  split or trimmed rather than committed whole.
- Timestamps in the fixture age: tests that reason about "the last minute"
  create their own events stamped `now` instead of using the fixture.
- Re-capturing changes the pinned numbers; that is intended, and the
  commit that re-captures must update them.
