from datetime import UTC, datetime, timedelta

import pytest

from pulsestream.detection.rate import EwmaBandDetector
from pulsestream.models import Anomaly, FeatureSnapshot
from pulsestream.processing.windows import ClosedWindow

T = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)
STEADY = [10, 11, 9, 10, 12, 8, 10, 11, 9, 10, 10, 11, 9, 10, 12, 8, 10, 11, 9, 10]


def windows(counts: list[int], entity: str = "enwiki", start: datetime = T) -> list[ClosedWindow]:
    return [
        ClosedWindow(entity, start + timedelta(minutes=i), start + timedelta(minutes=i + 1), c)
        for i, c in enumerate(counts)
    ]


def run(
    detector: EwmaBandDetector, series: list[ClosedWindow]
) -> list[tuple[FeatureSnapshot, Anomaly | None]]:
    return [detector.observe(w) for w in series]


def test_no_verdict_during_warm_up() -> None:
    out = run(EwmaBandDetector(warmup=10), windows(STEADY[:10] + [100]))
    snapshots, anomalies = zip(*out, strict=True)
    assert all(a is None for a in anomalies[:10])
    assert all(s.zscore is None and not s.is_anomaly for s in snapshots[:10])
    assert snapshots[0].ewma is None and snapshots[1].ewma == 10.0  # baseline before each window
    assert anomalies[10] is not None, "the first window after warm-up is judged"


def test_a_spike_is_flagged_with_the_baseline_it_was_judged_against() -> None:
    detector = EwmaBandDetector(warmup=10, k=3.0)
    out = run(detector, windows(STEADY + [40]))
    snapshot, anomaly = out[-1]

    assert anomaly is not None
    assert anomaly.metric == "edit_rate" and anomaly.method == "ewma_band"
    assert anomaly.value == 40.0 and anomaly.expected == pytest.approx(snapshot.ewma)
    assert anomaly.score == pytest.approx(snapshot.zscore) and anomaly.score > 3.0
    assert anomaly.severity in {"high", "critical"}
    assert anomaly.window_start == T + timedelta(minutes=20)
    assert anomaly.detected_at == anomaly.window_end
    assert "outside [" in (anomaly.details or "")
    assert snapshot.is_anomaly and snapshot.rate_per_min == 40.0
    assert all(a is None for _, a in out[:-1]), "the steady series raises nothing"


def test_winsorised_update_keeps_a_second_spike_visible() -> None:
    detector = EwmaBandDetector(warmup=10, k=3.0)
    run(detector, windows(STEADY))
    first = detector.observe(windows([40], start=T + timedelta(minutes=20))[0])[1]
    second = detector.observe(windows([40], start=T + timedelta(minutes=21))[0])[1]
    back_to_normal = detector.observe(windows([10], start=T + timedelta(minutes=22))[0])[1]

    assert first is not None and second is not None, "the first spike did not hide the second"
    assert back_to_normal is None


def test_small_wikis_need_an_absolute_deviation_not_just_a_z_score() -> None:
    quiet = [1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0]
    detector = EwmaBandDetector(warmup=10, min_std=1.0, min_abs=5.0)
    run(detector, windows(quiet))
    assert detector.observe(windows([4], start=T + timedelta(minutes=12))[0])[1] is None
    anomaly = detector.observe(windows([8], start=T + timedelta(minutes=13))[0])[1]
    assert anomaly is not None and anomaly.severity in {"high", "critical"}


def test_a_drop_to_zero_is_an_anomaly_unless_drops_are_disabled() -> None:
    busy = [50, 52, 48, 51, 49, 50, 53, 47, 50, 51, 49, 50]
    detector = EwmaBandDetector(warmup=10)
    run(detector, windows(busy))
    drop = detector.observe(windows([0], start=T + timedelta(minutes=12))[0])[1]
    assert drop is not None and drop.score < -3 and drop.severity == "critical"

    quiet_detector = EwmaBandDetector(warmup=10, detect_drops=False)
    run(quiet_detector, windows(busy))
    assert quiet_detector.observe(windows([0], start=T + timedelta(minutes=12))[0])[1] is None


def test_a_discontinuity_resets_the_entity() -> None:
    detector = EwmaBandDetector(warmup=10)
    run(detector, windows(STEADY))
    jumped = windows([100], start=T + timedelta(hours=3))[0]  # the counter forgot and re-created it

    snapshot, anomaly = detector.observe(jumped)

    assert anomaly is None and snapshot.ewma is None and snapshot.zscore is None


def test_entities_are_independent_and_ids_deterministic() -> None:
    detector = EwmaBandDetector(warmup=10)
    run(detector, windows(STEADY, entity="enwiki"))
    run(detector, windows([1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1], entity="cywiki"))
    assert detector.entities() == 2

    spike = windows([40], start=T + timedelta(minutes=20))[0]
    first = detector.observe(spike)[1]
    again = EwmaBandDetector(warmup=10)
    run(again, windows(STEADY))
    second = again.observe(spike)[1]
    assert first is not None and second is not None
    assert first.anomaly_id == second.anomaly_id, "same window, same finding, same id"


def test_configuration_is_validated() -> None:
    with pytest.raises(ValueError):
        EwmaBandDetector(warmup=1)
    with pytest.raises(ValueError):
        EwmaBandDetector(k=0)
