# syntax=docker/dockerfile:1.7
# One Dockerfile, three images: `--target api`, `--target dashboard`, `--target ingest`.
#
# builder: uv resolves from uv.lock and installs the project into a self-contained
#          virtualenv (dependencies first, for layer caching; then the project).
# runtime: python:slim with only that virtualenv and a non-root user. No uv, no
#          sources, no compilers in the shipped image.

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim-bookworm AS runtime
LABEL org.opencontainers.image.source="https://github.com/dimakiskostas/Pulsestream" \
      org.opencontainers.image.description="PulseStream: near-real-time event analytics with anomaly detection"
RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --create-home app \
    && mkdir -p /data \
    && chown app:app /data
COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PULSE_DB_PATH=/data/pulsestream.duckdb \
    PULSE_LAKE_ROOT=/data/raw \
    PULSE_LOG_FORMAT=json
USER app
WORKDIR /app
VOLUME ["/data"]
ENTRYPOINT ["pulsestream"]

FROM runtime AS api
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=3).status == 200 else 1)"
CMD ["api", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime AS dashboard
ENV PULSE_API_URL=http://api:8000
EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=3).status == 200 else 1)"
CMD ["dashboard", "--port", "8501"]

FROM runtime AS ingest
ENV PULSE_BUS=redis
EXPOSE 8001
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8001/health/live', timeout=3).status == 200 else 1)"
CMD ["ingest", "--host", "0.0.0.0", "--port", "8001"]
