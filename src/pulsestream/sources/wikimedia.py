"""Wikimedia EventStreams source: SSE over httpx with reconnect, resume and backoff.

The read loop is the part people fumble (tutorial Part 1.3). The engineering lives
around it:

* **Identify yourself.** Wikimedia's User-Agent policy applies to EventStreams; a
  generic client UA (``python-httpx/...``) is answered with HTTP 403 by the edge.
* **Fail fast on permanent errors.** A 4xx other than 429 will not get better by
  retrying, so it raises ``SourceFatalError`` instead of looping silently.
* **Reconnect on everything else** (timeouts, dropped connections, 5xx, 429 with
  ``Retry-After``) with exponential backoff and jitter, capped, and resume from the
  last seen event id so a blip does not lose events.
* **Bounded read timeout.** ``timeout=None`` means a silently dead connection is
  never noticed; Wikimedia emits events continuously, so 60 s without bytes is a
  dead stream.
"""

import asyncio
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime

import httpx
import structlog
from httpx_sse import aconnect_sse

from pulsestream import __version__
from pulsestream.models import RawEvent

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DEFAULT_USER_AGENT = f"PulseStream/{__version__} (https://github.com/dimakiskostas/Pulsestream)"

log = structlog.get_logger(__name__)

ClientFactory = Callable[[], httpx.AsyncClient]
SleepFn = Callable[[float], Awaitable[None]]


class SourceFatalError(RuntimeError):
    """The stream refused us in a way that retrying will not fix (e.g. HTTP 403)."""


class _Transient(Exception):
    """A failure worth reconnecting after, optionally with a server-dictated delay."""

    def __init__(self, reason: str, retry_after: float | None = None) -> None:
        super().__init__(reason)
        self.retry_after = retry_after


def base_delay_for(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    return float(min(cap, base * (2**attempt)))


def delay_for(
    attempt: int, base: float = 1.0, cap: float = 30.0, rng: random.Random | None = None
) -> float:
    """Exponential backoff with jitter: uniform in [d/2, d), d = min(cap, base * 2**attempt)."""
    d = base_delay_for(attempt, base, cap)
    jitter = rng.uniform(0, d / 2) if rng is not None else random.uniform(0, d / 2)
    return d / 2 + jitter


def _retry_after(response: httpx.Response, default: float | None) -> float | None:
    """Seconds the server asked us to wait, or ``default`` when the header is absent/garbled."""
    value = response.headers.get("retry-after")
    try:
        return float(value) if value is not None else default
    except ValueError:
        return default


class WikimediaSource:
    def __init__(
        self,
        url: str = STREAM_URL,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        read_timeout: float = 60.0,
        max_attempts: int | None = None,
        reset_after: int = 100,
        client_factory: ClientFactory | None = None,
        sleep: SleepFn = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        """
        ``max_attempts=None`` reconnects forever (a service); a number gives up with
        ``SourceFatalError`` after that many consecutive failures (a one-off capture).
        ``reset_after`` consecutive good events forgive earlier failures.
        ``client_factory`` / ``sleep`` / ``rng`` exist so tests can run offline and instantly.
        """
        self._url = url
        self._user_agent = user_agent
        self._timeout = httpx.Timeout(connect=10.0, read=read_timeout, write=10.0, pool=10.0)
        self._max_attempts = max_attempts
        self._reset_after = reset_after
        self._client_factory = client_factory or self._default_client
        self._sleep = sleep
        self._rng = rng
        self.last_event_id: str | None = None

    def _default_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=self._timeout)

    async def stream(self) -> AsyncIterator[RawEvent]:
        attempt = 0
        while True:
            headers = {"User-Agent": self._user_agent}
            if self.last_event_id:
                headers["Last-Event-ID"] = self.last_event_id
            events_seen = 0
            retry_after: float | None = None
            try:
                async with self._client_factory() as client:
                    async with aconnect_sse(client, "GET", self._url, headers=headers) as source:
                        status = source.response.status_code
                        if status == 429 or status >= 500:
                            # 429 without Retry-After: wait a minute; 5xx without it: back off.
                            default = 60.0 if status == 429 else None
                            raise _Transient(
                                f"HTTP {status}", _retry_after(source.response, default)
                            )
                        if status >= 400:
                            body = (await source.response.aread())[:200]
                            raise SourceFatalError(f"HTTP {status} from {self._url}: {body!r}")
                        log.info("source.connected", url=self._url, resume_from=self.last_event_id)
                        async for sse in source.aiter_sse():
                            if sse.id:
                                self.last_event_id = sse.id
                            if sse.event != "message" or not sse.data:
                                continue
                            yield RawEvent(
                                source="wikimedia",
                                received_at=datetime.now(UTC),
                                schema_version=1,
                                raw_payload=sse.data,
                            )
                            events_seen += 1
                            if events_seen >= self._reset_after:
                                attempt = 0  # a healthy run forgives earlier failures
                reason = "stream ended"
            except _Transient as exc:
                reason, retry_after = str(exc), exc.retry_after
            except httpx.RequestError as exc:  # timeouts, drops, protocol errors, SSEError
                reason = f"{type(exc).__name__}: {exc}"

            attempt += 1
            if self._max_attempts is not None and attempt > self._max_attempts:
                raise SourceFatalError(f"giving up after {attempt - 1} reconnects: {reason}")
            # First retry waits ~base (1 s), then 2 s, 4 s, ... capped: attempt is 1-based here.
            delay = (
                retry_after if retry_after is not None else delay_for(attempt - 1, rng=self._rng)
            )
            log.warning("source.reconnect", attempt=attempt, delay=round(delay, 2), reason=reason)
            await self._sleep(delay)
