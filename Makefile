# PulseStream task runner. Every target is a thin alias over `uv run ...`, so on a
# machine without make (Windows) you can type the underlying command instead.
.DEFAULT_GOAL := help
.PHONY: help install lint format typecheck test check precommit run run-replay capture clean

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

test:            ## pytest with coverage
	uv run pytest

check: lint typecheck test   ## everything CI runs

precommit:       ## run all pre-commit hooks on every file
	uv run pre-commit run --all-files

run:             ## serve the API with the live Wikimedia pipeline inside (http://127.0.0.1:8000)
	uv run pulsestream api

run-replay:      ## serve the API replaying the committed fixture, no network
	PULSE_SOURCE=fake PULSE_FIXTURE_DELAY=0.05 uv run pulsestream api

capture:         ## record 500 live events to tests/fixtures/sample_events.jsonl
	uv run pulsestream capture -n 500

clean:           ## remove caches and local databases
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage *.duckdb *.duckdb.wal
