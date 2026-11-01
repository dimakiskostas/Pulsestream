from datetime import UTC, datetime, timedelta, timezone

import pytest

from pulsestream.processing.windows import ClosedWindow, TumblingCounter

T = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)


def at(minute: int, second: int = 0) -> datetime:
    return T + timedelta(minutes=minute, seconds=second)


def counts(windows: list[ClosedWindow]) -> list[tuple[str, int, int]]:
    """(entity, minute offset of window_start, count) for readable assertions."""
    return [(w.entity, int((w.window_start - T).total_seconds() // 60), w.count) for w in windows]


def test_windows_are_aligned_to_the_window_length_in_utc() -> None:
    counter = TumblingCounter(window_seconds=60)
    assert counter.floor(at(0, 59)) == at(0)
    assert counter.floor(at(1, 0)) == at(1)
    athens = timezone(timedelta(hours=3))
    assert counter.floor(datetime(2026, 10, 11, 13, 7, 30, tzinfo=athens)) == at(7)
    assert TumblingCounter(window_seconds=300).floor(at(7, 1)) == at(5)


def test_a_window_closes_once_the_watermark_passes_its_end() -> None:
    counter = TumblingCounter(window_seconds=60, lateness_seconds=10)
    assert counter.observe("a", at(0, 5)) == []
    assert counter.observe("a", at(0, 30)) == []
    assert counter.observe("a", at(1, 5)) == []  # watermark 10:00:55 < 10:01:00 end
    closed = counter.observe("a", at(1, 15))  # watermark 10:01:05

    assert counts(closed) == [("a", 0, 2)]
    assert closed[0].window_end == at(1)
    assert counter.watermark == at(1, 5)


def test_late_events_cannot_reopen_a_closed_window() -> None:
    counter = TumblingCounter(window_seconds=60, lateness_seconds=10)
    counter.observe("a", at(0, 5))
    counter.observe("a", at(1, 15))  # closes 10:00
    assert counter.late_events == 0

    assert counter.observe("a", at(0, 50)) == []  # belongs to the closed 10:00 window

    assert counter.late_events == 1
    assert counts(counter.flush()) == [("a", 1, 1)], "the late event was not counted anywhere"


def test_gaps_are_filled_with_zero_windows() -> None:
    counter = TumblingCounter(window_seconds=60, lateness_seconds=0)
    counter.observe("a", at(0, 10))
    closed = counter.observe("a", at(3, 10))  # closes 10:00 and fills 10:01, 10:02 with zeros

    assert counts(closed) == [("a", 0, 1), ("a", 1, 0), ("a", 2, 0)]


def test_a_quiet_entity_keeps_emitting_zeros_as_others_advance_the_watermark() -> None:
    counter = TumblingCounter(window_seconds=60, lateness_seconds=0)
    counter.observe("a", at(0, 10))
    counter.observe("b", at(0, 20))
    closed = counter.observe("b", at(1, 30))  # closes 10:00 for both
    assert counts(closed) == [("a", 0, 1), ("b", 0, 1)]

    closed = counter.observe("b", at(2, 30))  # closes 10:01: b had one, a none
    assert counts(closed) == [("a", 1, 0), ("b", 1, 1)]

    closed = counter.observe("b", at(3, 30))
    assert counts(closed) == [("a", 2, 0), ("b", 2, 1)]


def test_an_entity_silent_for_too_long_is_forgotten_and_starts_afresh() -> None:
    counter = TumblingCounter(window_seconds=60, lateness_seconds=0, max_gap_windows=3)
    counter.observe("a", at(0, 10))
    counter.observe("b", at(0, 10))
    counter.observe("b", at(1, 10))  # closes 10:00 for both

    closed = counter.observe("b", at(10, 10))  # b jumps nine minutes ahead
    assert [c for c in counts(closed) if c[0] == "a"] == [], "no nine-minute zero run for a"
    assert counts(closed)[-1] == ("b", 1, 1)

    counter.observe("a", at(10, 20))  # a reappears
    closed = counter.observe("b", at(11, 10))  # closes 10:10 for both
    assert [c for c in counts(closed) if c[0] == "a"] == [("a", 10, 1)]


def test_flush_closes_open_windows_without_waiting() -> None:
    counter = TumblingCounter(window_seconds=60, lateness_seconds=10)
    counter.observe("a", at(0, 5))
    counter.observe("b", at(0, 7))
    counter.observe("a", at(0, 9))

    assert counts(counter.flush()) == [("a", 0, 2), ("b", 0, 1)]
    assert counter.flush() == []


def test_output_is_chronological_across_entities() -> None:
    counter = TumblingCounter(window_seconds=60, lateness_seconds=0)
    counter.observe("z", at(0, 1))
    counter.observe("a", at(0, 2))
    closed = counter.observe("m", at(1, 2))  # closes 10:00 for a and z
    closed += counter.observe("a", at(2, 2))  # closes 10:01: a and z empty, m has one

    assert counts(closed) == [("a", 0, 1), ("z", 0, 1), ("a", 1, 0), ("m", 1, 1), ("z", 1, 0)]


def test_configuration_is_validated() -> None:
    with pytest.raises(ValueError):
        TumblingCounter(window_seconds=0)
    with pytest.raises(ValueError):
        TumblingCounter(lateness_seconds=-1)
