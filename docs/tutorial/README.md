# PulseStream: the worked tutorial

The original guided tutorial ([`../tutorial.pdf`](../tutorial.pdf)) is a
scaffold: it gives the tricky patterns and leaves the engineering to the
builder. These chapters are the record of actually doing it, in this
repository, against the real stream, with the detours and gotchas that showed
up along the way. The design rationale stays in the HLD
([`../hld.pdf`](../hld.pdf)); the decisions it asked us to make are in
[`../decisions/`](../decisions/README.md).

## How the history is organised

- **One branch per phase**, named after the HLD roadmap (§13):
  `phase-1-vertical-slice`, `phase-2-storage-analytics`,
  `phase-3-anomaly-detection`, `phase-4-cicd-observability`,
  `phase-5-streaming-decoupling`. Phase 0 and the first half of Phase 1
  predate the branches: they are the original August 2026 pull requests
  #1–#4 on `main`.
- **One commit per step** inside a branch. Every commit passes
  `ruff check`, `ruff format --check`, `mypy src` and `pytest` on its own,
  so you can check out any commit and have a working system at that point.
- **One merge commit per phase** into `main` (`--no-ff`), so the phase
  boundaries stay visible:

```bash
git log --oneline --first-parent main          # one line per phase
git log --oneline --no-merges <merge>^1..<merge>^2   # the steps of that phase
git checkout phase-2-storage-analytics          # the repo as it was at the end of Phase 2
```

Commit messages carry the "why": read them with `git log --format=full` or
`git show <sha>` when a chapter points at one.

## Chapters

| Chapter                                              | HLD phase | Branch                       |
| ---------------------------------------------------- | --------- | ---------------------------- |
| [00 Skeleton](00-skeleton.md)                        | 0         | `main` (PR #1)               |
| [01 Vertical slice](01-vertical-slice.md)            | 1         | `phase-1-vertical-slice`     |
| 02 Storage and analytics                             | 2         | `phase-2-storage-analytics`  |
| 03 Anomaly detection v1                              | 3         | `phase-3-anomaly-detection`  |
| 04 CI/CD and observability                           | 4         | `phase-4-cicd-observability` |
| 05 Streaming decoupling                              | 5         | `phase-5-streaming-decoupling` |

## Conventions used in the chapters

- **Step** — one commit. The heading quotes the commit subject.
- **Why** — the engineering reason, usually a pointer into the HLD or an ADR.
- **Gotcha** — something that bit during the build and how it was resolved.
- **Definition of done** — the check that proved the step, copied from a real run.
