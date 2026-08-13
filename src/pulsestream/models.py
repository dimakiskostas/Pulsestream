import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class RawEvent(BaseModel):
    ingest_id: UUID = Field(default_factory=uuid4)
    source: str
    received_at: datetime
    schema_version: int
    raw_payload: str


class WikiEvent(BaseModel):
    event_id: str
    entity: str
    event_ts: datetime
    byte_delta: int
    is_bot: bool
    received_at: datetime

    @classmethod
    def from_raw(cls, raw: RawEvent) -> "WikiEvent | None":
        types = ["edit", "new"]
        try:
            data = json.loads(raw.raw_payload)
        except (TypeError, ValueError):
            return None

        if data.get("type") not in types:
            return None

        event_id = data.get("meta", {}).get("id")
        entity = data.get("wiki") or "Wiki"
        event_ts_value = data.get("meta", {}).get("dt")

        if event_ts_value is None:
            return None

        if isinstance(event_ts_value, (int, float)):
            event_ts = datetime.fromtimestamp(event_ts_value, tz=UTC)
        else:
            event_ts = datetime.fromisoformat(str(event_ts_value).replace("Z", "+00:00"))

        byte_delta = data.get("byte_delta")
        if byte_delta is None:
            length = data.get("length", {})
            byte_delta = length.get("new", 0) - length.get("old", 0)

        is_bot = data.get("is_bot")
        if is_bot is None:
            is_bot = data.get("bot", False)
        if isinstance(is_bot, str):
            is_bot = is_bot.lower() == "true"

        return cls(
            event_id=str(event_id),
            entity=str(entity),
            event_ts=event_ts,
            byte_delta=int(byte_delta),
            is_bot=bool(is_bot),
            received_at=raw.received_at,
        )
