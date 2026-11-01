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

Template: **Status** (proposed / accepted / superseded by NNNN), **Context**,
**Decision**, **Alternatives considered**, **Consequences**, **Revisit if**.
