import asyncio
import random
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
from httpx_sse import SSEError, aconnect_sse

from pulsestream.models import RawEvent

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def delay_for(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    d = min(cap, base * 2**attempt)
    return d / 2 + random.uniform(0, d / 2)


class WikimediaSource:
    async def stream(self) -> AsyncIterator[RawEvent]:
        attempt = 0
        RESET_AFTER = 1000
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
                                    attempt = 0
            except httpx.RequestError:
                attempt += 1
                d = delay_for(attempt)
                await asyncio.sleep(d)
                continue
            except httpx.HTTPStatusError as e:
                attempt += 1
                d = delay_for(attempt)
                if e.response.status_code == 429:
                    await asyncio.sleep(60)
                    continue
                elif 500 <= e.response.status_code < 600:
                    await asyncio.sleep(d)
                    continue
            except httpx.HTTPError:
                attempt += 1
                d = delay_for(attempt)
                await asyncio.sleep(d)
            except SSEError:
                attempt += 1
                d = delay_for(attempt)
                await asyncio.sleep(d)
