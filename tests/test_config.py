import pytest
from pydantic import ValidationError

from pulsestream.config import Settings


def test_defaults_are_the_phase_1_values() -> None:
    s = Settings(_env_file=None)
    assert s.db_path == "pulsestream.duckdb"
    assert s.queue_maxsize == 1000
    assert (s.batch_size, s.flush_interval) == (50, 2.0)
    assert s.source == "wikimedia"
    assert s.user_agent.startswith("PulseStream/")


def test_environment_overrides_with_pulse_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSE_BATCH_SIZE", "7")
    monkeypatch.setenv("PULSE_SOURCE", "fake")
    monkeypatch.setenv("PULSE_FIXTURE_DELAY", "0.25")
    s = Settings(_env_file=None)
    assert s.batch_size == 7
    assert s.source == "fake"
    assert s.fixture_delay == 0.25


def test_unknown_source_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSE_SOURCE", "kafka")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
