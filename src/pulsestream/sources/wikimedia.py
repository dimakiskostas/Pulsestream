import asyncio
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

    async def stream(self) -> AsyncIterator[RawEvent]:
        attemp = 0
        while True:
            try:
                attemp += 1
                d = min(self.cap, self.base * 2**attemp)
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
            except httpx.RequestError:
                await asyncio.sleep(d)
                continue
            except httpx.HTTPStatusError as e:
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
                await asyncio.sleep(d)
