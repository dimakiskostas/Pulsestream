"""The counters move where the work happens, and /metrics renders them."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import httpx
import pytest
from prometheus_client import REGISTRY

from pulsestream.api.app import create_app
from pulsestream.config import Settings
from pulsestream.models import Anomaly, RawEvent, anomaly_id
from pulsestream.processing.detection import DetectionResult
from pulsestream.processing.processor import StreamProcessor
from pulsestream.scheduler.jobs import Maintenance
from pulsestream.sources.fake import FakeSource
from tests.factories import _raw, create_payload
from tests.test_wikimedia import Edge, make_source, ok, take


def sample(name: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, labels or None)
    assert value is not None, (name, labels)
    return value


def test_processor_counts_events_duration_and_lag(con: duckdb.DuckDBPyConnection) -> None:
    now = int(datetime.now(UTC).timestamp())
    raws = [
        _raw(create_payload(meta={"id": "m-1"}, timestamp=now - 3)),
        _raw(create_payload(meta={"id": "m-2"}, timestamp=now - 1)),
        _raw(create_payload(meta={"id": "m-3"}, type="log")),
        RawEvent(source="t", received_at=datetime.now(UTC), schema_version=1, raw_payload="x"),
    ]
    raw_before = sample("pulsestream_ingest_events_total", stage="raw")
    kept_before = sample("pulsestream_ingest_events_total", stage="kept")
    batches_before = sample("pulsestream_processing_duration_seconds_count")

    processor = StreamProcessor(con)
    processor.process(raws)

    assert sample("pulsestream_ingest_events_total", stage="raw") == raw_before + 4
    assert sample("pulsestream_ingest_events_total", stage="kept") == kept_before + 2
    assert sample("pulsestream_processing_duration_seconds_count") == batches_before + 1
    assert 0.0 <= sample("pulsestream_processing_lag_seconds") < 60.0
    assert processor.last_processed_at is not None
    assert processor.last_event_ts == datetime.fromtimestamp(now - 1, tz=UTC)


def test_processing_failures_are_counted_and_re_raised(con: duckdb.DuckDBPyConnection) -> None:
    class BrokenStore:
        def write_batch(self, raws: list[RawEvent]) -> list[Path]:
            raise OSError("disk full")

    errors_before = sample("pulsestream_errors_total", stage="processing")
    with pytest.raises(OSError):
        StreamProcessor(con, BrokenStore()).process([_raw(create_payload())])
    assert sample("pulsestream_errors_total", stage="processing") == errors_before + 1


def test_findings_are_counted_by_metric_and_severity(con: duckdb.DuckDBPyConnection) -> None:
    t0 = datetime(2026, 11, 1, 10, 0, tzinfo=UTC)
    finding = Anomaly(
        anomaly_id=anomaly_id("ewma_band", "enwiki", "edit_rate", "metrics-test"),
        detected_at=t0,
        entity="enwiki",
        metric="edit_rate",
        value=40.0,
        expected=10.0,
        score=6.0,
        method="ewma_band",
        severity="high",
        window_start=t0,
        window_end=t0 + timedelta(minutes=1),
    )

    class StubDetection:
        def ingest(self, events: list[object]) -> DetectionResult:
            return DetectionResult(anomalies=[finding])

        def flush(self) -> DetectionResult:
            return DetectionResult()

    before = sample("pulsestream_anomalies_detected_total", metric="edit_rate", severity="high")
    processor = StreamProcessor(con, None, StubDetection())  # type: ignore[arg-type]
    result = processor.process([_raw(create_payload(meta={"id": "k-1"}))])
    assert result.anomalies == 1
    assert (
        sample("pulsestream_anomalies_detected_total", metric="edit_rate", severity="high")
        == before + 1
    )


def test_job_outcomes_are_counted(con: duckdb.DuckDBPyConnection) -> None:
    ok_before = sample("pulsestream_job_runs_total", job="compact_lake", outcome="ok")
    Maintenance(con, None).run("compact_lake")
    assert sample("pulsestream_job_runs_total", job="compact_lake", outcome="ok") == ok_before + 1

    class Broken:
        def cursor(self) -> None:
            raise RuntimeError("no database")

    failed_before = sample("pulsestream_job_runs_total", job="refresh_aggregates", outcome="failed")
    errors_before = sample("pulsestream_errors_total", stage="job")
    Maintenance(Broken(), None).run("refresh_aggregates")  # type: ignore[arg-type]
    assert (
        sample("pulsestream_job_runs_total", job="refresh_aggregates", outcome="failed")
        == failed_before + 1
    )
    assert sample("pulsestream_errors_total", stage="job") == errors_before + 1


async def test_source_reconnects_are_counted_and_connection_state_tracked() -> None:
    edge = Edge([ok([1, 2], drop=True), ok([3, 4])])
    source, _ = make_source(edge)
    reconnects_before = sample("pulsestream_source_reconnects_total")
    errors_before = sample("pulsestream_errors_total", stage="source")
    assert source.connected is False

    raws = await take(source, 4)

    assert len(raws) == 4
    assert sample("pulsestream_source_reconnects_total") == reconnects_before + 1
    assert sample("pulsestream_errors_total", stage="source") == errors_before + 1
    assert source.connected is True


async def test_metrics_endpoint_renders_the_registry(tmp_path: Path) -> None:
    cfg = Settings(
        _env_file=None,
        db_path=str(tmp_path / "m.duckdb"),
        lake_root=str(tmp_path / "raw"),
        scheduler_enabled=False,
    )
    app = create_app(cfg, source=FakeSource([]))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/metrics")
            schema = (await client.get("/openapi.json")).json()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "pulsestream_ingest_events_total" in response.text
    assert "pulsestream_queue_depth" in response.text
    assert "/metrics" not in schema["paths"], "an operational endpoint, not part of the contract"
