"""DetectionStage and its wiring into StreamProcessor, driven by synthetic event streams."""

from datetime import UTC, datetime, timedelta

import duckdb

from pulsestream.config import Settings
from pulsestream.detection.bytes import ByteDeltaDetector
from pulsestream.detection.rate import EwmaBandDetector
from pulsestream.models import WikiEvent
from pulsestream.pipeline import BatchResult
from pulsestream.processing.detection import DetectionStage
from pulsestream.processing.processor import StreamProcessor
from pulsestream.processing.windows import TumblingCounter
from tests.factories import _raw, create_payload, create_WikiEvent

T = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)


def minute_of_edits(minute: int, count: int, entity: str = "enwiki") -> list[WikiEvent]:
    """``count`` edits spread through one minute, 20..60 bytes each."""
    step = 60.0 / max(count, 1)
    return [
        create_WikiEvent(
            event_id=f"{entity}-{minute}-{i}",
            entity=entity,
            event_ts=T + timedelta(minutes=minute, seconds=i * step),
            received_at=T + timedelta(minutes=minute, seconds=i * step + 2),
            byte_delta=20 + (i * 7) % 41,
        )
        for i in range(count)
    ]


def stage(warmup: int = 5) -> DetectionStage:
    return DetectionStage(
        TumblingCounter(window_seconds=60, lateness_seconds=5),
        EwmaBandDetector(warmup=warmup, min_abs=5.0),
        ByteDeltaDetector(window=100, warmup=20, min_bytes=500),
    )


def test_stage_closes_windows_and_flags_a_rate_spike() -> None:
    s = stage(warmup=5)
    steady = [10, 11, 9, 10, 12, 8, 10, 11, 9, 10]
    for minute, count in enumerate(steady):
        result = s.ingest(minute_of_edits(minute, count))
        assert result.anomalies == []
    assert s.windows_total == len(steady) - 1, "the last window is still open"

    result = s.ingest(minute_of_edits(10, 45))  # closes minute 9, opens minute 10
    assert [sn.window_start for sn in result.snapshots] == [T + timedelta(minutes=9)]
    assert result.anomalies == []
    result = s.ingest(minute_of_edits(11, 10))  # closes minute 10: the spike
    assert len(result.anomalies) == 1
    anomaly = result.anomalies[0]
    assert anomaly.metric == "edit_rate" and anomaly.value == 45.0
    assert anomaly.window_start == T + timedelta(minutes=10)
    assert anomaly.detected_at == anomaly.window_end
    assert s.anomalies_total == 1 and s.late_events == 0


def test_stage_flags_a_huge_edit_by_size() -> None:
    s = stage()
    for minute in range(3):
        s.ingest(minute_of_edits(minute, 10))  # 30 small edits: byte warm-up done at 20
    huge = create_WikiEvent(
        event_id="huge", entity="enwiki", event_ts=T + timedelta(minutes=3), byte_delta=12_000
    )

    result = s.ingest([huge])

    sizes = [a for a in result.anomalies if a.metric == "byte_delta"]
    assert len(sizes) == 1 and sizes[0].event_id == "huge" and sizes[0].severity == "critical"


def test_stage_orders_a_batch_by_event_time_and_flush_closes_open_windows() -> None:
    s = stage()
    shuffled = minute_of_edits(0, 5) + minute_of_edits(1, 5)
    shuffled.reverse()  # arrives out of order within the batch
    result = s.ingest(shuffled)
    assert [sn.count for sn in result.snapshots] == [5]  # minute 0 closed, minute 1 open
    assert s.late_events == 0

    flushed = s.flush()

    assert [(sn.window_start, sn.count) for sn in flushed.snapshots] == [
        (T + timedelta(minutes=1), 5)
    ]
    assert s.windows_total == 2


def test_from_settings_builds_the_configured_stage() -> None:
    cfg = Settings(_env_file=None, window_seconds=30, warmup_windows=3, byte_delta_enabled=False)
    s = DetectionStage.from_settings(cfg)
    assert s._sizes is None
    assert s._counter.window == timedelta(seconds=30)
    assert DetectionStage.from_settings(Settings(_env_file=None))._sizes is not None


def test_processor_persists_snapshots_and_findings(con: duckdb.DuckDBPyConnection) -> None:
    processor = StreamProcessor(con, None, stage(warmup=5))
    counts = [10, 11, 9, 10, 12, 8, 10, 11, 9, 10, 45, 10]
    results = []
    for minute, count in enumerate(counts):
        raws = [
            _raw(
                create_payload(
                    meta={"id": e.event_id},
                    wiki=e.entity,
                    timestamp=int(e.event_ts.timestamp()),
                    length={"old": 100, "new": 100 + e.byte_delta},
                )
            )
            for e in minute_of_edits(minute, count)
        ]
        results.append(processor.process(raws))

    assert results[0] == BatchResult(raw=10, kept=10, anomalies=0)
    assert sum(r.anomalies for r in results) == 1
    assert processor.anomalies_total == 1
    assert con.execute("SELECT count(*) FROM features_rolling").fetchone() == (11,)  # last open
    (entity, value, severity) = con.execute(
        "SELECT entity, value, severity FROM anomalies"
    ).fetchone() or (None, None, None)
    assert (entity, value) == ("enwiki", 45.0) and severity in {"high", "critical"}
    assert con.execute("SELECT count(*) FROM features_rolling WHERE is_anomaly").fetchone() == (1,)

    final = processor.finish()
    assert final.snapshots and con.execute("SELECT count(*) FROM features_rolling").fetchone() == (
        12,
    )


def test_processor_without_detection_behaves_as_before(con: duckdb.DuckDBPyConnection) -> None:
    processor = StreamProcessor(con)
    raws = [_raw(create_payload(meta={"id": "x"}))]
    assert processor.process(raws) == BatchResult(raw=1, kept=1, anomalies=0)
    assert processor.finish().snapshots == []
    assert con.execute("SELECT count(*) FROM features_rolling").fetchone() == (0,)
