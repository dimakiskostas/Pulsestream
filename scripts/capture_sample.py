"""Capture a replayable sample from the live Wikimedia stream.

    uv run python scripts/capture_sample.py [COUNT] [PATH]

Equivalent to ``uv run pulsestream capture -n COUNT -o PATH``, kept as a
standalone script.
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
