"""WikimediaSource tests run offline: an httpx MockTransport plays the Wikimedia edge."""

import json
import random
from collections.abc import AsyncIterator, Callable

import httpx
import pytest

import pulsestream.sources.wikimedia as wikimedia
from pulsestream.models import RawEvent
from pulsestream.sources.wikimedia import SourceFatalError, WikimediaSource


def test_delay_for_varies():
    values = [wikimedia.delay_for(3) for _ in range(20)]
    assert len(set(values)) > 1


def test_delay_within_bounds():
    for attempt in range(20):
        assert (
            wikimedia.base_delay_for(attempt) / 2
            <= wikimedia.delay_for(attempt)
            <= wikimedia.base_delay_for(attempt)
        )


def test_base_delay_for_doubles_and_caps():
    assert wikimedia.base_delay_for(0) == 1.0
    assert wikimedia.base_delay_for(3) == 8.0
    assert wikimedia.base_delay_for(4) == 16.0
    assert wikimedia.base_delay_for(5) == 30.0
    assert wikimedia.base_delay_for(10) == 30.0
    assert wikimedia.base_delay_for(20) == 30.0
    assert wikimedia.base_delay_for(100) == 30.0


def test_delay_for_is_reproducible_with_a_seeded_rng():
    assert wikimedia.delay_for(2, rng=random.Random(7)) == wikimedia.delay_for(
        2, rng=random.Random(7)
    )


# --- SSE mock machinery -----------------------------------------------------------


def sse_bytes(ids: list[int]) -> bytes:
    """Serialise message events the way the Wikimedia stream does (comment, id, data)."""
    out = b":ok\n\n"
    for i in ids:
        payload = json.dumps({"n": i})
        out += f"event: message\nid: id-{i}\ndata: {payload}\n\n".encode()
    return out


class DroppingStream(httpx.AsyncByteStream):
    """Deliver the bytes, then (optionally) die like a half-closed TCP connection."""

    def __init__(self, body: bytes, drop: bool) -> None:
        self._body = body
        self._drop = drop

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._body
        if self._drop:
            raise httpx.ReadError("connection dropped")


class Edge:
    """Scripted responses, one per connection attempt; records every request."""

    def __init__(self, responses: list[Callable[[], httpx.Response]]) -> None:
        self._responses = responses
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        index = min(len(self.requests) - 1, len(self._responses) - 1)
        return self._responses[index]()

    def client_factory(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


def ok(ids: list[int], *, drop: bool = False) -> Callable[[], httpx.Response]:
    return lambda: httpx.Response(
        200,
        headers={"content-type": "text/event-stream; charset=utf-8"},
        stream=DroppingStream(sse_bytes(ids), drop),
    )


def status(code: int, **headers: str) -> Callable[[], httpx.Response]:
    return lambda: httpx.Response(
        code, headers={"content-type": "text/plain", **headers}, content=b"nope"
    )


def make_source(edge: Edge, **kwargs: object) -> tuple[WikimediaSource, list[float]]:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    source = WikimediaSource(
        "https://edge.invalid/stream",
        client_factory=edge.client_factory,
        sleep=fake_sleep,
        rng=random.Random(1),
        **kwargs,  # type: ignore[arg-type]
    )
    return source, slept


async def take(source: WikimediaSource, n: int) -> list[RawEvent]:
    out: list[RawEvent] = []
    gen = source.stream()
    try:
        async for raw in gen:
            out.append(raw)
            if len(out) >= n:
                break
    finally:
        await gen.aclose()
    return out


# --- behaviour --------------------------------------------------------------------


async def test_identifies_itself_and_yields_message_events() -> None:
    edge = Edge([ok([1, 2, 3])])
    source, _ = make_source(edge)

    raws = await take(source, 3)

    assert [json.loads(r.raw_payload)["n"] for r in raws] == [1, 2, 3]
    assert all(r.source == "wikimedia" and r.received_at.tzinfo is not None for r in raws)
    assert edge.requests[0].headers["user-agent"].startswith("PulseStream/")
    assert "last-event-id" not in edge.requests[0].headers


async def test_resumes_from_last_event_id_after_a_dropped_connection() -> None:
    edge = Edge([ok([1, 2], drop=True), ok([3, 4])])
    source, slept = make_source(edge)

    raws = await take(source, 4)

    assert [json.loads(r.raw_payload)["n"] for r in raws] == [1, 2, 3, 4]
    assert len(edge.requests) == 2
    assert edge.requests[1].headers["last-event-id"] == "id-2"
    assert len(slept) == 1 and 0.5 <= slept[0] <= 1.0  # attempt 1: jittered 1 s backoff


async def test_403_is_fatal_and_is_not_retried() -> None:
    edge = Edge([status(403)])
    source, slept = make_source(edge)

    with pytest.raises(SourceFatalError, match="HTTP 403"):
        await take(source, 1)

    assert len(edge.requests) == 1
    assert slept == []


async def test_429_waits_for_retry_after_then_reconnects() -> None:
    edge = Edge([status(429, **{"retry-after": "2"}), ok([1])])
    source, slept = make_source(edge)

    raws = await take(source, 1)

    assert json.loads(raws[0].raw_payload)["n"] == 1
    assert slept == [2.0]


async def test_5xx_backs_off_exponentially_then_recovers() -> None:
    edge = Edge([status(503), status(503), ok([1])])
    source, slept = make_source(edge)

    await take(source, 1)

    assert len(edge.requests) == 3
    assert 0.5 <= slept[0] <= 1.0 and 1.0 <= slept[1] <= 2.0


async def test_gives_up_after_max_attempts() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    edge = Edge([status(500)])
    edge.handler = refuse  # type: ignore[method-assign]
    source, slept = make_source(edge, max_attempts=2)

    with pytest.raises(SourceFatalError, match="giving up after 2"):
        await take(source, 1)
    assert len(slept) == 2


async def test_a_healthy_run_resets_the_attempt_counter() -> None:
    # fail, then 3 good events (reset_after=3), then drop: the next backoff is attempt 1 again
    edge = Edge([status(503), ok([1, 2, 3], drop=True), ok([4])])
    source, slept = make_source(edge, reset_after=3)

    await take(source, 4)

    assert len(slept) == 2
    assert 0.5 <= slept[1] <= 1.0, "counter was reset by the healthy run"
