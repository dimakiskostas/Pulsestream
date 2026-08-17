from datetime import UTC, datetime, timedelta

from pulsestream.models import WikiEvent
from tests.factories import _raw, create_payload


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
    payload = create_payload(timestamp=1786622989)
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.event_ts.utcoffset() == timedelta(0)


def test_shrinking_byte_delta() -> None:
    payload = create_payload(length={"old": 3409, "new": 3359})
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is not None
    assert wiki_event.byte_delta == payload["length"]["new"] - payload["length"]["old"]


def test_dropped_type() -> None:
    payload = create_payload(type="categorize")
    raw_event = _raw(payload)
    wiki_event = WikiEvent.from_raw(raw_event)
    assert wiki_event is None
