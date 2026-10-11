from pathlib import Path

import pytest

import pulsestream.sources.wikimedia as wikimedia
from pulsestream import __version__
from pulsestream.cli import main
from pulsestream.sources.fake import FakeSource, load_fixture
from tests.factories import _raw, create_payload


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"pulsestream {__version__}"


def test_a_sub_command_is_required() -> None:
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2


def test_capture_sub_command_uses_the_live_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    raws = [_raw(create_payload(meta={"id": f"evt-{i}"})) for i in range(4)]
    # Stand in for the live stream: the CLI looks the class up at call time.
    monkeypatch.setattr(wikimedia, "WikimediaSource", lambda: FakeSource(raws))
    out = tmp_path / "sample.jsonl"

    assert main(["capture", "-n", "2", "-o", str(out)]) == 0

    assert load_fixture(out) == raws[:2]
    assert capsys.readouterr().out.strip() == f"wrote 2 events to {out}"
