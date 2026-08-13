import json
from datetime import UTC, datetime

from pulsestream.models import RawEvent, WikiEvent


def _raw(payload: dict[str, any]) -> RawEvent:
    return RawEvent(
        source="test",
        received_at=datetime.now(UTC),
        schema_version=1,
        raw_payload=json.dumps(payload),
    )


def test_from_raw_keeps_an_edit() -> None:
    payload = {
        "type": "edit",
        "meta": {"id": "123"},
        "timestamp": datetime.now(UTC).isoformat(),
        "wiki": "test_entity",
        "length": {"new": 100, "old": 50},
        "bot": False,
    }
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.event_id == payload["meta"]["id"]
    assert wiki_event.entity == payload["wiki"]
    assert wiki_event.event_ts == datetime.fromisoformat(
        payload["timestamp"].replace("Z", "+00:00")
    )
    assert wiki_event.byte_delta == payload["length"]["new"] - payload["length"]["old"]
    assert wiki_event.is_bot == payload["bot"]
    assert wiki_event.received_at == raw_event.received_at


def test_timezone_aware_timestamp() -> None:
    payload = {
        "type": "edit",
        "meta": {"id": "123"},
        "timestamp": datetime.now(UTC).isoformat(),
        "wiki": "test_entity",
        "length": {"new": 100, "old": 50},
        "bot": False,
    }
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.event_ts.tzinfo is not None


def test_shrinking_byte_delta() -> None:
    payload = {
        "type": "edit",
        "meta": {"id": "123"},
        "timestamp": datetime.now(UTC).isoformat(),
        "wiki": "test_entity",
        "length": {"new": 50, "old": 100},
        "bot": False,
    }
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.byte_delta == payload["length"]["new"] - payload["length"]["old"]


def test_dropped_type() -> None:
    payload = {
        "type": "delete",
        "meta": {"id": "123"},
        "timestamp": datetime.now(UTC).isoformat(),
        "wiki": "test_entity",
        "length": {"new": 50, "old": 100},
        "bot": False,
    }
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is None
