# PulseStream task runner. Every target is a thin alias over `uv run ...`, so on a
# machine without make (Windows) you can type the underlying command instead.
.DEFAULT_GOAL := help
.PHONY: help install lint format typecheck security test check precommit run run-replay dashboard replay capture eval check-data image up monitoring down logs clean

help:            ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

install:         ## create .venv from uv.lock and install the pre-commit hook
	uv sync --dev
	uv run pre-commit install

lint:            ## ruff check + format check
	uv run ruff check .
	uv run ruff format --check .

format:          ## ruff autofix + format
	uv run ruff check --fix .
	uv run ruff format .

typecheck:       ## mypy (strict) on src
	uv run mypy src

security:        ## bandit code scan on src
	uv run bandit -q -c pyproject.toml -r src

test:            ## pytest with coverage
	uv run pytest

check: lint typecheck security test   ## everything CI runs

precommit:       ## run all pre-commit hooks on every file
	uv run pre-commit run --all-files

run:             ## serve the API with the live Wikimedia pipeline inside (http://127.0.0.1:8000)
	uv run pulsestream api

run-replay:      ## serve the API replaying the committed fixture, no network
	PULSE_SOURCE=fake PULSE_FIXTURE_DELAY=0.05 uv run pulsestream api

dashboard:       ## run the Streamlit dashboard against the API (http://127.0.0.1:8501)
	uv run pulsestream dashboard

replay:          ## rebuild replay.duckdb from the Parquet lake in data/raw
	uv run pulsestream replay --lake data/raw --db replay.duckdb

capture:         ## record 500 live events to tests/fixtures/sample_events.jsonl
	uv run pulsestream capture -n 500

eval:            ## score the rate detector against injected synthetic anomalies
	uv run pulsestream eval

check-data:      ## pandera data-quality gates on the local DB and lake
	uv run pulsestream check --db pulsestream.duckdb --lake data/raw

image:           ## build the three images locally (needs BuildKit): pulsestream-{api,dashboard,ingest}:dev
	docker build -f docker/Dockerfile --target api -t pulsestream-api:dev .
	docker build -f docker/Dockerfile --target dashboard -t pulsestream-dashboard:dev .
	docker build -f docker/Dockerfile --target ingest -t pulsestream-ingest:dev .

up:              ## redis + ingest + api + dashboard in containers (:8001, :8000, :8501)
	docker compose up --build -d

monitoring:      ## the same plus Prometheus (:9090) and Grafana (:3000)
	docker compose --profile monitoring up --build -d

down:            ## stop the compose stack; the data volumes are kept
	docker compose --profile monitoring down

logs:            ## follow the ingest and API containers' JSON logs
	docker compose logs -f ingest api

replay-bus:      ## rebuild replay.duckdb from the bus's recent window (the stack's redis on localhost)
	uv run pulsestream replay --from-bus --db replay.duckdb

clean:           ## remove caches and local databases
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage *.duckdb *.duckdb.wal
