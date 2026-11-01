from prometheus_client import REGISTRY

from pulsestream.common import metrics


def sample(name: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, labels or None)
    assert value is not None, (name, labels)
    return value


def test_every_series_exists_from_the_start() -> None:
    body, content_type = metrics.render()
    text = body.decode()
    assert content_type.startswith("text/plain")
    for name in (
        "pulsestream_ingest_events_total",
        "pulsestream_errors_total",
        "pulsestream_processing_lag_seconds",
        "pulsestream_queue_depth",
        "pulsestream_processing_duration_seconds",
        "pulsestream_anomalies_detected_total",
        "pulsestream_source_reconnects_total",
        "pulsestream_job_runs_total",
    ):
        assert f"# TYPE {name}" in text, name
    # Presence, not value: other tests move these counters, and order must not matter.
    assert 'pulsestream_errors_total{stage="processing"} ' in text
    assert 'pulsestream_anomalies_detected_total{metric="edit_rate",severity="critical"} ' in text


def test_counters_and_gauges_move_as_expected() -> None:
    before = sample("pulsestream_ingest_events_total", stage="raw")
    metrics.INGEST_EVENTS.labels(stage="raw").inc(50)
    assert sample("pulsestream_ingest_events_total", stage="raw") == before + 50

    metrics.PROCESSING_LAG.set(2.5)
    assert sample("pulsestream_processing_lag_seconds") == 2.5

    runs = sample("pulsestream_job_runs_total", job="compact_lake", outcome="ok")
    metrics.JOB_RUNS.labels(job="compact_lake", outcome="ok").inc()
    assert sample("pulsestream_job_runs_total", job="compact_lake", outcome="ok") == runs + 1

    with metrics.PROCESSING_DURATION.time():
        pass
    assert sample("pulsestream_processing_duration_seconds_count") >= 1
