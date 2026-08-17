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
        "wiki": "enwiki",
        "length": {"old": 3359, "new": 3409},
        "bot": False,
        "title": "Test Title",
        "namespace": 0,
    }
    return {**base, **kwargs}


def create_WikiEvent(**kwargs) -> WikiEvent:
    base = {
        "event_id": "e49b3bf0-04d9-4104-877f-64cbc2e5e9d9",
        "entity": "enwiki",
        "event_ts": datetime.fromtimestamp(1786622924, tz=UTC),
        "byte_delta": 3409 - 3359,
        "is_bot": False,
        "received_at": datetime.fromtimestamp(1786622924, tz=UTC),
        "event_type": "edit",
        "title": "Test Title",
        "namespace": 0,
    }
    return WikiEvent(**{**base, **kwargs})
