import json
from datetime import datetime
from typing import Literal
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
    event_type: Literal["edit", "new"]
    title: str
    namespace: int

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
        entity = data.get("wiki")
        event_ts = data.get("timestamp")

        length = data.get("length", {})
        byte_delta = length.get("new", 0) - length.get("old", 0)

        is_bot = data.get("bot")
        if is_bot is None:
            is_bot = data.get("bot", False)

        title = data.get("title", "")
        namespace = data.get("namespace", 0)
        return cls(
            event_id=event_id,
            entity=str(entity),
            event_ts=event_ts,
            byte_delta=int(byte_delta),
            is_bot=bool(is_bot),
            received_at=raw.received_at,
            event_type=data.get("type"),
            title=title,
            namespace=namespace,
        )
