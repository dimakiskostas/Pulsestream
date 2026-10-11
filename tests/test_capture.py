from pathlib import Path

from pulsestream.capture import capture
from pulsestream.sources.fake import FakeSource, load_fixture
from tests.factories import _raw, create_payload


def make_raws(n: int):
    return [_raw(create_payload(meta={"id": f"evt-{i}"})) for i in range(n)]


async def test_capture_writes_exactly_n_events_and_round_trips(tmp_path: Path) -> None:
    raws = make_raws(5)
    path = tmp_path / "fixtures" / "sample.jsonl"  # parent dir does not exist yet

    written = await capture(FakeSource(raws), 3, path)

    assert written == 3
    assert load_fixture(path) == raws[:3]


async def test_capture_stops_when_the_source_ends_first(tmp_path: Path) -> None:
    raws = make_raws(2)
    path = tmp_path / "sample.jsonl"
    assert await capture(FakeSource(raws), 10, path) == 2
    assert load_fixture(path) == raws


async def test_capture_zero_creates_an_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "sample.jsonl"
    assert await capture(FakeSource(make_raws(2)), 0, path) == 0
    assert path.read_text(encoding="utf-8") == ""
