from datetime import UTC, datetime
from uuid import UUID

import pytest

from pulsestream.bus.base import FIELDS, decode, encode
from pulsestream.models import RawEvent


def test_wire_format_round_trips_the_envelope() -> None:
    event = RawEvent(
        ingest_id=UUID("2c6a4d3e-1f0b-4a9c-8d7e-5b4c3a2f1e0d"),
        source="wikimedia",
        received_at=datetime(2026, 11, 1, 13, 4, 12, 418091, tzinfo=UTC),
        schema_version=1,
        raw_payload='{"type": "edit", "wiki": "enwiki"}',
    )
    fields = encode(event)
    assert set(fields) == set(FIELDS) and all(isinstance(v, str) for v in fields.values())
    assert fields["received_at"] == "2026-11-01T13:04:12.418091+00:00"
    assert decode(fields) == event


def test_received_at_keeps_its_offset() -> None:
    event = RawEvent(
        source="test",
        received_at=datetime(2026, 11, 1, 13, 0, tzinfo=UTC),
        schema_version=1,
        raw_payload="{}",
    )
    back = decode(encode(event))
    assert back.received_at.tzinfo is not None and back.received_at == event.received_at


def test_decoding_a_foreign_message_fails_loudly() -> None:
    with pytest.raises(KeyError):
        decode({"ingest_id": "2c6a4d3e-1f0b-4a9c-8d7e-5b4c3a2f1e0d"})  # a field is missing
    with pytest.raises(ValueError):
        decode(
            {
                **encode(
                    RawEvent(
                        source="t",
                        received_at=datetime.now(UTC),
                        schema_version=1,
                        raw_payload="{}",
                    )
                ),
                "ingest_id": "nope",
            }
        )
