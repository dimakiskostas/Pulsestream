# 00 · Skeleton (Phase 0)

> Goal (HLD §13, tutorial Part 0): a packaged project with linting, typing,
> tests and a green CI on an app that does nothing yet. Get the machinery
> working before any logic; CI is far cheaper to debug now than later.

Phase 0 was done in August 2026 on `main` ("Setting up", "GitHub Actions",
PR #1). This chapter describes what it established and the adjustments the
Phase 1 branch made to it once the project was used in anger.

## What the skeleton contains

| Piece                | Where                          | Notes                                                             |
| -------------------- | ------------------------------ | ----------------------------------------------------------------- |
| Package + lockfile   | `pyproject.toml`, `uv.lock`    | `uv init --package`, src layout, `uv_build` backend                |
| Interpreter pin      | `.python-version` = 3.12       | uv picks it locally; CI tests 3.12 and 3.13                       |
| Runtime deps         | fastapi, uvicorn, duckdb, pydantic(-settings), httpx, httpx-sse, structlog, prometheus-client | per tutorial 0.1 |
| Dev deps             | pytest, pytest-asyncio, pytest-cov, ruff, mypy, pre-commit, pandera | `[dependency-groups] dev` |
| Lint/format          | `[tool.ruff]` line-length 100, rules E F I UP B | one tool for both                                 |
| Types                | `[tool.mypy] strict = true`    | checked on `src/` only                                            |
| Tests                | `[tool.pytest.ini_options]`    | `asyncio_mode = "auto"`, coverage on by default                   |
| Smoke test           | `tests/test_smoke.py`          | proves the harness runs                                           |
| CI                   | `.github/workflows/ci.yml`     | push to main + PRs; matrix 3.12/3.13; `uv sync --dev`, ruff, mypy, pytest |

The commands that must be green before writing any pipeline logic:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

## Adjustments made later (on `phase-1-vertical-slice`)

These are Phase 0 concerns that only surfaced once the code was exercised.
Each is one commit; `git log --oneline main..phase-1-vertical-slice` lists them.

### Step: *chore: normalise line endings, widen .gitignore, file the ADR*

- `.gitattributes` with `* text=auto eol=lf`. The project is developed on
  Windows with `core.autocrlf=true`; without the attribute, shell scripts,
  Dockerfiles and YAML would reach Linux containers with CRLF.
- `.gitignore` learns `.mypy_cache/`, coverage output, `*.duckdb.wal`
  (one had slipped into a feature branch), `data/`, `logs/`.
- The first ADR had been saved under `.github/workflows/docs/`, where nobody
  looks; it moved to `docs/decisions/`.

**Gotcha: `pytz` looks unused but is required.** Nothing imports it, so the
obvious cleanup is to remove it. Do that and every `TIMESTAMPTZ` read fails:

```
InvalidInputException: Required module 'pytz' failed to import
```

DuckDB imports `pytz` lazily to build tz-aware Python datetimes. It stays, with
a comment in `pyproject.toml` saying why.

### Step: *chore(pre-commit): route every hook through uv so versions come from uv.lock*

The original config took ruff from the `astral-sh/ruff-pre-commit` mirror at a
version one patch ahead of the ruff in `uv.lock`, and let pre-commit build its
own virtualenvs with pip.

**Gotcha: pre-commit's hook install hung.** On the development machine the
pip-based environment build for `pre-commit-hooks` never finished (ten
minutes, killed). Every hook is now a `repo: local` entry run through
`uv run`, with the standard checks coming from the `pre-commit-hooks` PyPI
package in the dev group. One lockfile decides every tool version, CI and the
hooks cannot disagree, and installing the hooks needs no network.

```yaml
- repo: local
  hooks:
    - id: ruff-check
      entry: uv run ruff check --fix
      language: system
      types_or: [python, pyi]
    - id: mypy
      entry: uv run mypy src
      language: system
      pass_filenames: false
```

## Definition of done

- `uv run pytest` green on the empty app (the smoke test).
- A green `ci` run on GitHub for `main` (runs #1–#9 in August were green).
- `uv run pre-commit run --all-files` passes with the uv-routed hooks:

```
trailing whitespace......................Passed
end of file newline......................Passed
check yaml...............................Passed
check toml...............................Passed
check merge conflict markers.............Passed
check added large files (1 MB)...........Passed
detect private key.......................Passed
ruff check (autofix).....................Passed
ruff format..............................Passed
mypy src.................................Passed
```

Next: [01 · Vertical slice](01-vertical-slice.md).
