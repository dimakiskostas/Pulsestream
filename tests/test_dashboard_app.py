"""The Streamlit page, run headlessly with AppTest against a stand-in API client."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import polars as pl
import pytest
from streamlit.testing.v1 import AppTest

import pulsestream.dashboard.data as data
from pulsestream.dashboard.data import (
    ANOMALIES_SCHEMA,
    FEATURES_SCHEMA,
    MINUTELY_SCHEMA,
    TOP_SCHEMA,
)

APP = Path(__file__).parents[1] / "src" / "pulsestream" / "dashboard" / "app.py"


class FakeClient:
    """Answers like the API would; records nothing, needs no network."""

    def __init__(self, base_url: str, **_: object) -> None:
        self.base_url = base_url

    def close(self) -> None:
        pass

    def health(self) -> dict[str, object]:
        return {
            "status": "ok",
            "pipeline": {"producer": "running", "consumer": "running", "queue_depth": 3},
        }

    def stats(self) -> dict[str, object]:
        return {
            "events_total": 1234,
            "events_last_min": 56,
            "latest_event_ts": "2026-10-11T10:54:26Z",
            "lag_seconds": 2.5,
        }

    def lake(self) -> dict[str, object]:
        return {
            "enabled": True,
            "root": "data/raw",
            "files": 12,
            "partitions": 2,
            "bytes": 2_500_000,
            "rows": 1234,
            "first_received_at": None,
            "last_received_at": None,
        }

    def jobs(self) -> list[dict[str, object]]:
        return [
            {
                "name": "refresh_aggregates",
                "runs": 3,
                "failures": 0,
                "last_result": "7 buckets",
                "last_error": None,
                "next_run_at": "2026-10-11T11:00:00Z",
            }
        ]

    def minutely(self, minutes: int = 60, entity: str | None = None) -> pl.DataFrame:
        t0 = datetime(2026, 10, 11, 10, 50, tzinfo=UTC)
        rows = [
            {
                "bucket_ts": t0.replace(minute=50 + i),
                "entity": "*",
                "count": 10 + i,
                "bot_count": i,
                "avg_value": 1.0,
                "max_value": 5,
                "anomaly_count": 0,
            }
            for i in range(5)
        ]
        return pl.DataFrame(rows, schema=MINUTELY_SCHEMA)

    def top_entities(self, minutes: int = 60, limit: int = 10) -> pl.DataFrame:
        return pl.DataFrame([{"entity": "enwiki", "count": 30, "bot_count": 4}], schema=TOP_SCHEMA)

    def anomaly_summary(self, minutes: int = 60) -> dict[str, object]:
        return {
            "minutes": minutes,
            "total": 2,
            "by_severity": {"critical": 1, "warn": 1},
            "by_metric": {"edit_rate": 1, "byte_delta": 1},
            "top_entities": [{"entity": "enwiki", "count": 2}],
        }

    def anomalies(self, minutes: int = 60, limit: int = 50) -> pl.DataFrame:
        t0 = datetime(2026, 10, 11, 10, 52, tzinfo=UTC)
        rows = [
            {
                "window_start": t0,
                "entity": "enwiki",
                "metric": "edit_rate",
                "severity": "critical",
                "value": 40.0,
                "expected": 10.0,
                "score": 20.0,
                "details": "count 40 outside [5.5, 14.5]",
            },
            {
                "window_start": t0,
                "entity": "enwiki",
                "metric": "byte_delta",
                "severity": "warn",
                "value": -9000.0,
                "expected": 80.0,
                "score": -4.2,
                "details": "|delta| 9000 bytes vs median 80",
            },
        ]
        return pl.DataFrame(rows, schema=ANOMALIES_SCHEMA)

    def features(self, entity: str, minutes: int = 60) -> pl.DataFrame:
        t0 = datetime(2026, 10, 11, 10, 48, tzinfo=UTC)
        rows = [
            {
                "window_start": t0.replace(minute=48 + i),
                "count": [10, 11, 9, 40, 10][i],
                "ewma": 10.0,
                "upper": 14.5,
                "lower": 5.5,
                "is_anomaly": i == 3,
            }
            for i in range(5)
        ]
        return pl.DataFrame(rows, schema=FEATURES_SCHEMA)


class DownClient(FakeClient):
    def health(self) -> dict[str, object]:
        raise httpx.ConnectError("connection refused")


@pytest.fixture(autouse=True)
def no_auto_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSE_DASHBOARD_AUTOREFRESH", "0")


def test_dashboard_renders_metrics_charts_and_panels(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(data, "ApiClient", FakeClient)

    at = AppTest.from_file(str(APP), default_timeout=60).run()

    assert not at.exception
    assert at.title[0].value == "PulseStream"
    assert [m.value for m in at.metric] == ["1,234", "56", "2.5", "ok", "2", "1", "1 / 1"]
    assert [m.label for m in at.metric][:4] == [
        "Events stored",
        "Last minute",
        "Lag (s)",
        "Pipeline",
    ]
    subheaders = [s.value for s in at.subheader]
    assert subheaders == [
        "Edits per minute, last 60 min (all wikis)",
        "Busiest wikis",
        "Raw lake",
        "Maintenance jobs",
        "Anomalies, last 60 min",
        "Control band: enwiki",
    ]
    assert at.error == [] and at.info == []
    assert any("12 files in 2 partitions" in m.value for m in at.markdown)
    assert at.sidebar.text_input[-1].value == "enwiki", "band chart follows the busiest wiki"


def test_dashboard_shows_an_error_when_the_api_is_down(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(data, "ApiClient", DownClient)

    at = AppTest.from_file(str(APP), default_timeout=60).run()

    assert not at.exception
    assert len(at.error) == 1 and "API not reachable" in at.error[0].value
    assert at.metric == []
