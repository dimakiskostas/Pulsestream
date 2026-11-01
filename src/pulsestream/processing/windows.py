"""Event-time tumbling windows per entity, closed by a watermark.

Counting edits per entity per minute is trivial until events arrive out of order,
late, or stop arriving for an entity. This module answers those cases:

* Windows are aligned to multiples of ``window_seconds`` in UTC and driven by
  ``event_ts``, so a replay yields exactly the windows the live run did.
* A window closes only when the watermark (the newest event time seen, minus an
  allowed lateness) has passed its end, so slightly late events still count.
* An event for a window that already closed is counted in ``late_events`` and
  dropped: it cannot reopen a window downstream has already consumed.
* An entity that goes quiet keeps producing zero-count windows as the watermark
  advances, so a drop to zero is observable and its baseline keeps moving. Past
  ``max_gap_windows`` of silence the entity is forgotten and starts afresh when
  it reappears; the detector sees the discontinuity and warms up again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class ClosedWindow:
    entity: str
    window_start: datetime
    window_end: datetime
    count: int


@dataclass
class _EntityState:
    open: dict[datetime, int] = field(default_factory=dict)  # window_start -> count
    last_closed: datetime | None = None  # start of the last window emitted


class TumblingCounter:
    def __init__(
        self,
        *,
        window_seconds: int = 60,
        lateness_seconds: int = 10,
        max_gap_windows: int = 60,
    ) -> None:
        if window_seconds <= 0 or lateness_seconds < 0 or max_gap_windows < 0:
            raise ValueError("window_seconds > 0, lateness_seconds >= 0, max_gap_windows >= 0")
        self._window = timedelta(seconds=window_seconds)
        self._lateness = timedelta(seconds=lateness_seconds)
        self._max_gap = max_gap_windows
        self._entities: dict[str, _EntityState] = {}
        self._watermark: datetime | None = None
        self.late_events = 0

    @property
    def watermark(self) -> datetime | None:
        return self._watermark

    @property
    def window(self) -> timedelta:
        return self._window

    def floor(self, ts: datetime) -> datetime:
        """Start of the window containing ``ts`` (UTC, aligned to the window length)."""
        size = int(self._window.total_seconds())
        aligned = int(ts.astimezone(UTC).timestamp()) // size * size
        return datetime.fromtimestamp(aligned, tz=UTC)

    def observe(self, entity: str, event_ts: datetime) -> list[ClosedWindow]:
        """Count one event; return the windows (all entities) the advancing watermark closed."""
        start = self.floor(event_ts)
        state = self._entities.setdefault(entity, _EntityState())
        if state.last_closed is not None and start <= state.last_closed:
            self.late_events += 1
        else:
            state.open[start] = state.open.get(start, 0) + 1
        candidate = event_ts.astimezone(UTC) - self._lateness
        if self._watermark is None or candidate > self._watermark:
            self._watermark = candidate
        return self._close_due()

    def flush(self) -> list[ClosedWindow]:
        """Close every open window regardless of the watermark (shutdown, end of a replay)."""
        closed: list[ClosedWindow] = []
        for entity, state in self._entities.items():
            for start in sorted(state.open):
                closed.extend(self._emit(entity, state, start))
            state.open.clear()
        return sorted(closed, key=lambda w: (w.window_start, w.entity))

    def _close_due(self) -> list[ClosedWindow]:
        assert self._watermark is not None
        closed: list[ClosedWindow] = []
        for entity, state in list(self._entities.items()):
            for start in sorted(s for s in state.open if s + self._window <= self._watermark):
                closed.extend(self._emit(entity, state, start))
                del state.open[start]
            if state.last_closed is None:
                continue
            # Every window between the last emitted one and the watermark is now known
            # to be empty: any event for it would be late. Emit those zeros right away.
            pending = int((self._watermark - (state.last_closed + self._window)) / self._window)
            if pending > self._max_gap:
                if state.open:
                    state.last_closed = None  # history forgotten; what is open still counts
                else:
                    del self._entities[entity]  # nothing open either: forget the entity
                continue
            for _ in range(pending):
                zero_start = state.last_closed + self._window
                if zero_start in state.open:  # cannot happen (that window would be due); be safe
                    break
                closed.extend(self._emit(entity, state, zero_start))
        return sorted(closed, key=lambda w: (w.window_start, w.entity))

    def _emit(self, entity: str, state: _EntityState, start: datetime) -> list[ClosedWindow]:
        """Emit the window at ``start``, preceded by zero windows for any gap since the last one."""
        out: list[ClosedWindow] = []
        if state.last_closed is not None:
            gap = int((start - state.last_closed) / self._window) - 1
            if gap > self._max_gap:
                state.last_closed = None  # too long quiet: no fill, the entity starts afresh
            else:
                for i in range(1, gap + 1):
                    zero_start = state.last_closed + i * self._window
                    out.append(ClosedWindow(entity, zero_start, zero_start + self._window, 0))
        out.append(ClosedWindow(entity, start, start + self._window, state.open.get(start, 0)))
        state.last_closed = start
        return out
