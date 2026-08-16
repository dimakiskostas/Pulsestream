import asyncio
import random
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
from httpx_sse import aconnect_sse

from pulsestream.models import RawEvent

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


class WikimediaSource:
    def __init__(self):
        self.cap = 30
        self.base = 1

    def delay_for(attemp: int, base: float = 1.0, cap: float = 30.0) -> float:
        d = min(cap, base * 2**attemp)
        return d / 2 + random.uniform(0, d / 2)

    async def stream(self) -> AsyncIterator[RawEvent]:
        attemp = 0
        RESET_AFTER = 500
        while True:
            try:
                events_seen = 0
                async with httpx.AsyncClient(timeout=None) as client:
                    async with aconnect_sse(client, "GET", STREAM_URL) as events:
                        async for sse in events.aiter_sse():
                            if sse.event == "message" and sse.data:
                                yield RawEvent(
                                    source="wikimedia",
                                    received_at=datetime.now(UTC),
                                    schema_version=1,
                                    raw_payload=sse.data,
                                )
                                events_seen += 1
                                if events_seen == RESET_AFTER:
                                    attemp = 0
            except httpx.RequestError:
                attemp += 1
                d = self.delay_for(attemp)
                await asyncio.sleep(d)
                continue
            except httpx.HTTPStatusError as e:
                attemp += 1
                d = self.delay_for(attemp)
                if e.response.status_code == 429:
                    await asyncio.sleep(60)
                    continue
                elif e.response.status_code[0] == "5":
                    await asyncio.sleep(d)
                    continue
                else:
                    raise httpx.HTTPStatusError(
                        f"HTTP error occurred: {e.response.status_code} - {e.response.text}"
                    ) from e
            except httpx.HTTPError:
                attemp += 1
                d = self.delay_for(attemp)
                await asyncio.sleep(d)
