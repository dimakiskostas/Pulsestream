# 15. CI builds, scans and publishes the images; a git tag is the release

## Status

Accepted (Phase 4).

## Context

Phase 0's workflow ran lint, types and tests. Phase 4 adds two images and a
deploy, which raises the questions of where images are built, which commits
produce a published image, what gates a publish, and what triggers a deploy.
The project is reviewed as one pull request per phase and squash-merged to
`main` (tutorial README), so `main` is always a reviewed state.

## Decision

- **Every push and pull request** runs the `test` job on Python 3.12 and
  3.13: `ruff check`, `ruff format --check`, `mypy src`, `bandit`, `pytest`
  and `pulsestream eval` (the detector scorecard, printed into the log so a
  regression is visible in the run).
- **Every push and pull request also builds both images**, with the same
  Dockerfile the release uses, loads them into the runner, smoke-tests the
  entrypoint (`--version`), validates the compose file against them and
  scans them with Trivy. CRITICAL and HIGH vulnerabilities with a fix
  available fail the job; unfixed ones are reported but do not block, since
  nothing can be done about them in this repository.
- **Images are pushed to GHCR only from `main` and from `v*` tags.** `main`
  gets `latest`, `main` and `sha-<short>`; a tag gets `vX.Y.Z` and `X.Y.Z`.
  Pull requests never push, so the registry only ever holds reviewed code.
- **A deploy happens only for a `v*` tag**, only when `DEPLOY_ENABLED` is
  set, through the `production` environment (ADR 0013). Tags are created by
  hand after the merge; nothing in CI tags automatically.
- **Supply chain, within reason.** Base images pinned by tag
  (`ghcr.io/astral-sh/uv:python3.12-bookworm-slim`,
  `python:3.12-slim-bookworm`); dependencies installed with
  `uv sync --frozen` from `uv.lock`; the shipped image contains the
  virtualenv and nothing else (no uv, no sources, no compiler); it runs as
  uid 10001; Bandit on the sources, Trivy on the image; actions pinned to a
  major version.
- **Build cache** per image in the GitHub Actions cache, so a docs-only
  change rebuilds nothing and a dependency change rebuilds one layer.

## Alternatives considered

**Push an image for every build.** Cheap and tempting; it makes `latest`
meaningless and fills the registry with unreviewed images that look
deployable.

**Build on the server.** ADR 0013.

**Automatic versioning (semantic-release, a tag per merge).** Would turn
every merge into a release. The review-then-merge-then-tag rhythm is
deliberate: a release is a decision, taken after the merged state has been
looked at.

**Digest-pinned base images and action SHAs.** Stricter, and only useful
with something (Dependabot, Renovate) to move the pins; left as a follow-up
with that tooling.

## Consequences

- CI can go red without a code change, when a new fixable vulnerability
  appears in a base image or a dependency. That is the point; the fix is a
  rebuild (base image) or a lock update (`uv lock --upgrade-package`).
- Two image builds per pull request, a few minutes with a warm cache.
- The Dockerfile uses BuildKit cache mounts, so `docker build` needs BuildKit
  (any current Docker Desktop or Engine with buildx). An engine without
  buildx can still verify the file with the legacy builder once the
  `--mount` flags are removed, which is how it was first built on this
  machine's WSL.
- `latest` follows `main`; deployments pin `IMAGE_TAG` to a release tag and
  never use `latest`.
