import json
from datetime import datetime
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
        try:
            data = json.loads(raw.raw_payload)
            return cls(
                event_id=data["event_id"],
                entity=data["entity"],
                event_ts=datetime.fromisoformat(data["event_ts"]),
                byte_delta=data["byte_delta"],
                is_bot=data["is_bot"],
                received_at=raw.received_at,
            )
        except (KeyError, ValueError, TypeError):
            return None
