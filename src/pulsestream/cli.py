"""The ``pulsestream`` command: one front door for every service and tool.

    pulsestream api          serve the API; the ingestion pipeline runs inside it
    pulsestream capture      record live events to a JSONL fixture for replay

Each later phase adds a sub-command here instead of another loose script.
Heavy imports happen inside the handlers so ``pulsestream --help`` stays instant
and importing the CLI never starts a pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable

from pulsestream import __version__


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


COMMANDS: dict[str, Callable[[argparse.Namespace], int]] = {
    "api": cmd_api,
    "capture": cmd_capture,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return COMMANDS[args.command](args)
