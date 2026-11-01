# 13. Deploy target: one VPS running Docker Compose, updated over SSH from a version tag

## Status

Accepted (Phase 4).

## Context

The HLD (§9, §13 Phase 4) asks for containers, a pipeline that builds, scans
and publishes them, and a deploy step, and leaves the target to the builder:
a single virtual server, a platform service (Fly.io, Render, Railway) or
Kubernetes.

What has to run is small and has one hard constraint. The API process owns
the DuckDB writer, the Parquet lake and the detectors' in-memory state
(ADR 0003, ADR 0010), so exactly one instance of it may run against a data
directory. The dashboard is stateless and reads the API. Prometheus and
Grafana are optional. There is no database server, no object store and no
queue (until Phase 5). The whole system fits on one small machine.

## Decision

- **One Linux host with Docker and the Compose plugin** runs the stack
  described by `docker-compose.yml`: `api`, `dashboard`, and with the
  `monitoring` profile, `prometheus` and `grafana`. The named volume
  `pulsestream-data` holds the DuckDB file and the lake.
- **Images come from GHCR**, built by CI (ADR 0015), never on the host. The
  compose file names them
  `ghcr.io/dimakiskostas/pulsestream-{api,dashboard}:${IMAGE_TAG}`, so the
  same file serves `docker compose up --build` on a laptop and
  `docker compose pull` on the server.
- **A git tag `vX.Y.Z` is a release.** CI builds and pushes the images for
  the tag, then the `deploy` job connects to the host over SSH, logs in to
  GHCR with the job's own token, and runs
  `IMAGE_TAG=vX.Y.Z docker compose pull && docker compose up -d --remove-orphans`
  in the deploy directory.
- **Secrets and the gate live in GitHub.** `DEPLOY_HOST`, `DEPLOY_USER` and
  `DEPLOY_SSH_KEY` are secrets of the `production` environment (which can
  require a reviewer); `DEPLOY_ENABLED=true` and `DEPLOY_PATH` are repository
  variables. Without `DEPLOY_ENABLED` the job is skipped, so the pipeline is
  complete, and green, on a clone that has no server.
- **Rollback is a redeploy of the previous tag**: on the host,
  `IMAGE_TAG=<previous tag> docker compose up -d`, or push a new tag.

## Alternatives considered

**A platform service (Fly.io, Render, Railway).** Would remove the host from
the picture, but each of them wants one service per deploy unit with its own
persistent disk and its own scaling rules, and the one thing this system
needs, a single writer next to its data, is exactly what they make awkward.
Dev/prod parity would also be lost: the compose file would describe a stack
the platform does not run.

**Kubernetes.** The pipeline would become a StatefulSet with one replica and
a PersistentVolumeClaim, which is a long way to say "one container with a
volume". A cluster is justified when there are several services to schedule
(Phase 5) and someone to run the cluster.

**Build on the host** (`git pull && docker compose up --build`). No
immutable artifact, no scan, no record of which commit is running, and the
server needs the build toolchain and network access to PyPI. CI is the only
builder (ADR 0015).

## Consequences

- One host is one failure domain. Acceptable for a near-real-time analytics
  view of a public stream. A restart costs the events emitted while the
  process was down (the source's resume cursor lives in memory), plus the
  open windows and the baselines (ADR 0010's caveat). The lake and the
  database survive in the volume.
- The host needs the repository checked out at the deploy path, because the
  compose file mounts `docker/prometheus.yml`, `docker/grafana/` and the
  replay fixture from it. `docs/operations.md` is the runbook.
- Nothing deletes data. Compaction merges lake files but keeps every row, and
  DuckDB only grows. `GET /lake` reports the size; a retention job is still
  to be written.
- `latest` on GHCR follows `main`, not the last release. The server pins
  `IMAGE_TAG`; a `docker compose pull` on the host without it would fetch
  `main`'s image.

## Revisit if

- A second API instance is wanted: that needs Phase 5's split (ingestion and
  processing as separate services) and a lake on object storage.
- The stack grows past a handful of services, or someone else has to operate
  it. Then Kubernetes, or a platform, earns its keep.
