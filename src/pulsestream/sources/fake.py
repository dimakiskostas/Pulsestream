"""Replay source for deterministic tests and offline demos (HLD §10).

A live stream cannot be in the test suite. ``FakeSource`` satisfies the same
``EventSource`` shape as ``WikimediaSource`` and replays recorded ``RawEvent``
objects, so it drops into the real pipeline with zero wiring changes.
"""

import asyncio
from collections.abc import AsyncIterator, Iterable
from pathlib import Path

from pulsestream.models import RawEvent


class FakeSource:
    """Yield recorded events in order, optionally pacing them ``delay`` seconds apart.

    ``delay=0`` (the default) replays as fast as the consumer can take events, which
    is what tests want; a small delay gives a human-speed replay for demos.
    """

    def __init__(self, events: Iterable[RawEvent], *, delay: float = 0.0) -> None:
        self._events = list(events)
        self._delay = delay

    def __len__(self) -> int:
        return len(self._events)

    async def stream(self) -> AsyncIterator[RawEvent]:
        for raw in self._events:
            if self._delay > 0:
                await asyncio.sleep(self._delay)
            yield raw


def load_fixture(path: str | Path) -> list[RawEvent]:
    """Read a JSONL file of ``RawEvent`` records (one ``model_dump_json()`` per line)."""
    text = Path(path).read_text(encoding="utf-8")
    return [RawEvent.model_validate_json(line) for line in text.splitlines() if line.strip()]
