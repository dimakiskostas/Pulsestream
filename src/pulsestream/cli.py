"""The ``pulsestream`` command: one front door for every service and tool.

    pulsestream api          serve the API; the ingestion pipeline runs inside it
    pulsestream capture      record live events to a JSONL fixture for replay
    pulsestream replay       rebuild a DuckDB file from the Parquet lake
    pulsestream dashboard    run the Streamlit dashboard against a running API

Each later phase adds a sub-command here instead of another loose script.
Heavy imports happen inside the handlers so ``pulsestream --help`` stays instant
and importing the CLI never starts a pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from pulsestream import __version__


def _utc(text: str) -> datetime:
    """Parse an ISO-8601 timestamp; naive input is taken as UTC."""
    value = datetime.fromisoformat(text)
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pulsestream",
        description="Near-real-time event analytics pipeline with embedded anomaly detection.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    api = sub.add_parser("api", help="serve the API (the ingestion pipeline runs inside it)")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument("--reload", action="store_true", help="dev mode: restart on code changes")

    cap = sub.add_parser("capture", help="record N live events to a JSONL fixture for replay")
    cap.add_argument("-n", "--count", type=int, default=500)
    cap.add_argument("-o", "--output", default="tests/fixtures/sample_events.jsonl")

    rep = sub.add_parser("replay", help="rebuild a DuckDB file from the Parquet lake")
    rep.add_argument("--lake", default="data/raw", help="lake root (PULSE_LAKE_ROOT)")
    rep.add_argument("--db", default="replay.duckdb", help="target DuckDB file (created/extended)")
    rep.add_argument("--since", type=_utc, default=None, help="ISO timestamp, inclusive (UTC)")
    rep.add_argument("--until", type=_utc, default=None, help="ISO timestamp, exclusive (UTC)")
    rep.add_argument("--batch-size", type=int, default=500)

    dash = sub.add_parser("dashboard", help="run the Streamlit dashboard (reads the API only)")
    dash.add_argument("--api-url", default=os.environ.get("PULSE_API_URL", "http://127.0.0.1:8000"))
    dash.add_argument("--port", type=int, default=8501)
    return parser


def cmd_api(args: argparse.Namespace) -> int:
    import uvicorn

    uvicorn.run("pulsestream.api.app:app", host=args.host, port=args.port, reload=args.reload)
    return 0


def cmd_capture(args: argparse.Namespace) -> int:
    from pulsestream.capture import capture
    from pulsestream.sources.wikimedia import WikimediaSource

    count = asyncio.run(capture(WikimediaSource(), args.count, args.output))
    print(f"wrote {count} events to {args.output}")
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    from pulsestream.replay import replay

    report = asyncio.run(
        replay(args.lake, args.db, since=args.since, until=args.until, batch_size=args.batch_size)
    )
    print(
        f"replayed {report.raw} raw events from {args.lake}: {report.kept} kept, "
        f"{report.rows} rows in {args.db}, {report.buckets} minute buckets aggregated"
    )
    return 0


def dashboard_command(api_url: str, port: int) -> list[str]:
    script = Path(__file__).with_name("dashboard") / "app.py"
    return [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(script),
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]


def cmd_dashboard(args: argparse.Namespace) -> int:
    env = {**os.environ, "PULSE_API_URL": args.api_url}
    return subprocess.call(dashboard_command(args.api_url, args.port), env=env)


COMMANDS: dict[str, Callable[[argparse.Namespace], int]] = {
    "api": cmd_api,
    "capture": cmd_capture,
    "replay": cmd_replay,
    "dashboard": cmd_dashboard,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return COMMANDS[args.command](args)
