from datetime import UTC, datetime, timedelta

import pytest

from pulsestream.detection.bytes import ByteDeltaDetector
from pulsestream.models import WikiEvent
from tests.factories import create_WikiEvent

T = datetime(2026, 10, 11, 10, 0, 30, tzinfo=UTC)


def edit(delta: int, i: int, entity: str = "enwiki") -> WikiEvent:
    return create_WikiEvent(
        event_id=f"{entity}-{i}",
        entity=entity,
        event_ts=T + timedelta(seconds=i),
        received_at=T + timedelta(seconds=i, milliseconds=500),
        byte_delta=delta,
    )


def typical(n: int, entity: str = "enwiki") -> list[WikiEvent]:
    """Edits of 50..150 bytes, alternating sign, a realistic small-edit mix."""
    return [edit((50 + (i * 37) % 101) * (1 if i % 2 else -1), i, entity) for i in range(n)]


def test_no_finding_during_warm_up_even_for_a_huge_edit() -> None:
    detector = ByteDeltaDetector(window=200, warmup=50)
    for e in typical(49):
        assert detector.observe(e) is None
    assert detector.observe(edit(50_000, 49)) is None  # 50th edit: still warming up


def test_a_huge_edit_is_flagged_against_the_median() -> None:
    detector = ByteDeltaDetector(window=200, warmup=50, min_bytes=1000)
    for e in typical(60):
        detector.observe(e)

    big = edit(5_000, 60)
    anomaly = detector.observe(big)

    assert anomaly is not None
    assert anomaly.metric == "byte_delta" and anomaly.method == "mad_zscore"
    assert anomaly.event_id == "enwiki-60" and anomaly.value == 5_000.0
    assert anomaly.expected == pytest.approx(100.0, abs=30)  # median of the typical edits
    assert anomaly.score > 3.5 and anomaly.severity == "critical"
    assert anomaly.window_start == T.replace(second=0) + timedelta(minutes=1)  # the edit's minute
    assert anomaly.window_end == anomaly.window_start + timedelta(minutes=1)
    assert anomaly.detected_at == big.received_at
    assert "bytes vs median" in (anomaly.details or "")


def test_removals_are_scored_by_size_with_a_negative_sign() -> None:
    detector = ByteDeltaDetector(window=200, warmup=50, min_bytes=1000)
    for e in typical(60):
        detector.observe(e)
    anomaly = detector.observe(edit(-8_000, 60))
    assert anomaly is not None and anomaly.score < -3.5 and anomaly.severity == "critical"


def test_small_edits_are_never_findings_whatever_the_score() -> None:
    detector = ByteDeltaDetector(window=200, warmup=10, min_bytes=1000)
    for i in range(20):
        detector.observe(edit(10 + i % 3, i))  # 10..12 bytes
    assert detector.observe(edit(400, 20)) is None  # z is enormous, bytes are not


def test_identical_sizes_give_no_score_rather_than_infinity() -> None:
    detector = ByteDeltaDetector(window=200, warmup=10, min_bytes=0)
    for i in range(20):
        detector.observe(edit(100, i))
    assert detector.observe(edit(5_000, 20)) is None  # MAD is zero


def test_entities_are_independent_and_history_is_bounded() -> None:
    detector = ByteDeltaDetector(window=60, warmup=50, min_bytes=1000)
    for e in typical(60, "enwiki"):
        detector.observe(e)
    assert detector.entities() == 1
    assert detector.observe(edit(5_000, 0, "dewiki")) is None  # dewiki has no history yet
    assert detector.entities() == 2

    for e in typical(60, "dewiki"):
        detector.observe(e)
    assert detector.observe(edit(5_000, 61, "dewiki")) is not None
    assert len(detector._history["dewiki"]) == 60, "bounded by window"


def test_severity_tiers_follow_the_size_floor() -> None:
    detector = ByteDeltaDetector(window=200, warmup=50, min_bytes=1000)
    for e in typical(60):
        detector.observe(e)
    sizes = {1_000: "warn", 2_499: "warn", 2_500: "high", 4_999: "high", 5_000: "critical"}
    for i, (size, expected) in enumerate(sizes.items()):
        anomaly = ByteDeltaDetector(window=200, warmup=50, min_bytes=1000)
        for e in typical(60):
            anomaly.observe(e)
        found = anomaly.observe(edit(size, 100 + i))
        assert found is not None and found.severity == expected, (size, expected)


def test_defaults_are_calibrated_for_large_edits() -> None:
    detector = ByteDeltaDetector()
    for e in typical(60):
        detector.observe(e)
    assert detector.observe(edit(8_000, 60)) is None, "an 8 KB edit is routine on big wikis"
    big = detector.observe(edit(60_000, 61))
    assert big is not None and big.severity == "warn"


def test_configuration_is_validated() -> None:
    with pytest.raises(ValueError):
        ByteDeltaDetector(window=10, warmup=20)
    with pytest.raises(ValueError):
        ByteDeltaDetector(threshold=0)
