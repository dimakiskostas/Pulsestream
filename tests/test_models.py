import json
from datetime import UTC, datetime

from pulsestream.models import RawEvent, WikiEvent


def _raw(payload: dict) -> RawEvent:
    return RawEvent(
        source="test",
        received_at=datetime.now(UTC),
        schema_version=1,
        raw_payload=json.dumps(payload),
    )


def test_from_raw_keeps_an_edit() -> None:
    payload = {
        "type": "edit",
        "meta": {"id": "123", "dt": "2023-01-01T00:00:00Z"},
        "wiki": "test_entity",
        "byte_delta": 100,
        "is_bot": False,
    }
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.event_id == payload["meta"]["id"]
    assert wiki_event.entity == payload["wiki"]
    assert wiki_event.event_ts == datetime.fromisoformat(
        payload["meta"]["dt"].replace("Z", "+00:00")
    )
    assert wiki_event.byte_delta == payload["byte_delta"]
    assert wiki_event.is_bot == payload["is_bot"]
    assert wiki_event.received_at == raw_event.received_at
