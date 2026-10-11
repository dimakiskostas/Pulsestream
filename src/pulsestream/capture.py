"""Record a live sample for replay.

Captured once, committed under ``tests/fixtures/`` and replayed forever by
``FakeSource``: this is what keeps the test suite deterministic and offline.
"""

from collections.abc import AsyncIterator
from pathlib import Path

from pulsestream.models import RawEvent
from pulsestream.sources.base import EventSource


async def _aclose(events: AsyncIterator[RawEvent]) -> None:
    """Close an async generator we stopped consuming early.

    ``EventSource.stream`` is typed as a plain ``AsyncIterator`` (the protocol does
    not require ``aclose``), but every real source is an async generator holding a
    live HTTP response. Closing it explicitly releases the connection now instead
    of whenever garbage collection gets around to it.
    """
    aclose = getattr(events, "aclose", None)
    if aclose is not None:
        await aclose()


async def capture(source: EventSource, n: int, path: str | Path) -> int:
    """Write the first ``n`` raw events from ``source`` to ``path`` as JSONL.

    Returns the number of events written (fewer than ``n`` if the source ends first).
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if n <= 0:
        out.write_text("", encoding="utf-8")
        return 0
    count = 0
    events = source.stream()
    try:
        # newline="\n": Python's text mode would otherwise write CRLF on Windows.
        with out.open("w", encoding="utf-8", newline="\n") as f:
            async for raw in events:
                f.write(raw.model_dump_json() + "\n")
                count += 1
                if count >= n:
                    break
    finally:
        await _aclose(events)
    return count
