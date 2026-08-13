import json
from datetime import UTC, datetime
from typing import Any

from pulsestream.models import RawEvent, WikiEvent


def _raw(payload: dict[str, Any]) -> RawEvent:
    return RawEvent(
        source="test",
        received_at=datetime.now(UTC),
        schema_version=1,
        raw_payload=json.dumps(payload),
    )


def create_payload(**kwargs) -> dict[str, Any]:
    base = {
        "type": "edit",
        "meta": {"id": "e49b3bf0-04d9-4104-877f-64cbc2e5e9d9"},
        "timestamp": 1786622924,
        "wiki": "test_entity",
        "length": {"old": 3359, "new": 3409},
        "bot": False,
        "title": "Test Title",
        "namespace": 14,
    }
    return base | kwargs


def test_from_raw_keeps_an_edit() -> None:
    payload = create_payload()
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.event_id == payload["meta"]["id"]
    assert wiki_event.entity == payload["wiki"]
    assert wiki_event.event_ts == datetime.fromtimestamp(payload["timestamp"], tz=UTC)
    assert wiki_event.byte_delta == payload["length"]["new"] - payload["length"]["old"]
    assert wiki_event.is_bot == payload["bot"]
    assert wiki_event.received_at == raw_event.received_at


def test_timezone_aware_timestamp() -> None:
    payload = create_payload()
    payload["timestamp"] = datetime.now(UTC).isoformat()
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.event_ts.tzinfo is not None


def test_shrinking_byte_delta() -> None:
    payload = create_payload()
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.byte_delta == payload["length"]["new"] - payload["length"]["old"]


def test_dropped_type() -> None:
    payload = create_payload()
    payload["type"] = "categorize"
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is None
