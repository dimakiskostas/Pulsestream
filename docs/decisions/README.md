# Architecture decision records

Short records of the decisions the HLD (`docs/hld.pdf`, §15 and the `DECIDE`
markers) asks the builder to make and defend. One file per decision, numbered,
never edited after acceptance: a changed mind becomes a new record that
supersedes the old one.

| #    | Decision                                                      | HLD ref      | Phase |
| ---- | ------------------------------------------------------------- | ------------ | ----- |
| 0001 | Event filter, entity grain and headline anomaly               | §5, §8       | 1     |
| 0002 | In-process queue first, broker later; backpressure = block     | D1, §7       | 1     |
| 0003 | DuckDB as the analytical store; one writer; UTC sessions       | D3, §7       | 1     |
| 0004 | Micro-batch writes, flushed by size or time                    | D4, §7       | 1     |
| 0005 | At-least-once delivery with idempotent writes                  | D2, §4       | 1     |
| 0006 | Replay fixtures instead of the live source in tests            | §10          | 1     |
| 0007 | The dashboard reads through the API, not the database          | D5, §7       | 2     |
| 0008 | Raw lake layout (dt/hour by received_at) and compaction policy | §7, D4       | 2     |
| 0009 | Derived tables come from validated events, not raw Parquet     | §7, §8       | 2     |
| 0010 | Rate detector: tumbling minutes, EWMA ± 3·std, four guards     | D6, §7, §12  | 3     |
| 0011 | Detector quality is measured by injecting synthetic anomalies  | §10, §12     | 3     |
| 0012 | Byte-delta detector: a large-edit detector calibrated on live data | §5, §12   | 3     |
| 0013 | Deploy target: one VPS, Docker Compose, SSH deploy from a tag   | §9, §13      | 4     |
| 0014 | Observability: one log format, fixed metric series, two probes | §11          | 4     |
| 0015 | CI builds, scans and publishes images; a git tag is the release | §9, §13      | 4     |

Template: **Status** (proposed / accepted / superseded by NNNN), **Context**,
**Decision**, **Alternatives considered**, **Consequences**, **Revisit if**.
