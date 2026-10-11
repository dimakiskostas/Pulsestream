"""Capture a replayable sample from the live Wikimedia stream (tutorial Part 2.1).

    uv run python scripts/capture_sample.py [COUNT] [PATH]

Equivalent to ``uv run pulsestream capture -n COUNT -o PATH``; kept so the
tutorial's command works verbatim.
"""

import asyncio
import sys

from pulsestream.capture import capture
from pulsestream.sources.wikimedia import WikimediaSource


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    path = sys.argv[2] if len(sys.argv) > 2 else "tests/fixtures/sample_events.jsonl"
    count = asyncio.run(capture(WikimediaSource(), n, path))
    print(f"wrote {count} events to {path}")


if __name__ == "__main__":
    main()
