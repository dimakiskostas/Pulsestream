from pathlib import Path

from pulsestream.models import RawEvent
from pulsestream.sources.base import EventSource
from pulsestream.sources.fake import FakeSource, load_fixture
from tests.factories import _raw, create_payload


def make_raws(n: int) -> list[RawEvent]:
    return [_raw(create_payload(meta={"id": f"evt-{i}"})) for i in range(n)]


async def collect(source: EventSource) -> list[RawEvent]:
    return [raw async for raw in source.stream()]


async def test_fake_source_replays_in_order() -> None:
    raws = make_raws(5)
    source = FakeSource(raws)
    assert len(source) == 5
    assert await collect(source) == raws


async def test_fake_source_is_finite_and_empty_is_fine() -> None:
    assert await collect(FakeSource([])) == []


async def test_fake_source_paces_events_when_delay_is_set() -> None:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    assert len(await collect(FakeSource(make_raws(3), delay=0.25, sleep=fake_sleep))) == 3
    assert slept == [0.25, 0.25, 0.25]


async def test_fake_source_does_not_sleep_without_delay() -> None:
    async def explode(seconds: float) -> None:
        raise AssertionError("sleep must not be called when delay=0")

    assert len(await collect(FakeSource(make_raws(2), sleep=explode))) == 2


def test_load_fixture_round_trips_raw_events(tmp_path: Path) -> None:
    raws = make_raws(3)
    path = tmp_path / "sample.jsonl"
    path.write_text("\n".join(r.model_dump_json() for r in raws) + "\n", encoding="utf-8")

    loaded = load_fixture(path)

    assert loaded == raws
    assert all(r.received_at.tzinfo is not None for r in loaded), "timestamps stay tz-aware"


def test_load_fixture_skips_blank_lines(tmp_path: Path) -> None:
    raws = make_raws(2)
    path = tmp_path / "sample.jsonl"
    text = raws[0].model_dump_json() + "\n\n" + raws[1].model_dump_json()
    path.write_text(text, encoding="utf-8")
    assert load_fixture(str(path)) == raws
