"""ParquetSource and replay(): the lake goes back through the real pipeline."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pulsestream.cli import main
from pulsestream.models import RawEvent
from pulsestream.replay import replay
from pulsestream.sources.fake import load_fixture
from pulsestream.sources.lake import ParquetSource
from pulsestream.storage.duck import connect
from pulsestream.storage.parquet import ParquetRawStore
from tests.test_integration_pipeline import EXPECTED_KEPT, FIXTURE

T0 = datetime(2026, 10, 11, 10, 59, 50, tzinfo=UTC)


def raws_across_two_hours() -> list[RawEvent]:
    """Six events, 5 s apart, crossing the 11:00 boundary; written as two batches."""
    return [
        RawEvent(
            source="t",
            received_at=T0 + timedelta(seconds=5 * i),
            schema_version=1,
            raw_payload=json.dumps({"n": i}),
        )
        for i in range(6)
    ]


async def collect(source: ParquetSource) -> list[RawEvent]:
    return [raw async for raw in source.stream()]


async def test_parquet_source_streams_the_lake_in_received_order(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    raws = raws_across_two_hours()
    store.write_batch(raws[3:])  # later batch written first: order must come from the data
    store.write_batch(raws[:3])
    assert len(store.partitions()) == 2

    out = await collect(ParquetSource(tmp_path / "raw", chunk_size=4))

    assert out == raws, "ingest_id, timestamps and payloads round-trip exactly"


async def test_parquet_source_window_is_inclusive_exclusive(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    raws = raws_across_two_hours()
    store.write_batch(raws)

    since, until = raws[1].received_at, raws[4].received_at
    out = await collect(ParquetSource(tmp_path / "raw", since=since, until=until))

    assert out == raws[1:4]


async def test_parquet_source_on_an_empty_lake_yields_nothing(tmp_path: Path) -> None:
    assert await collect(ParquetSource(tmp_path / "nope")) == []


async def test_replay_rebuilds_a_database_without_touching_the_lake(tmp_path: Path) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    fixture = load_fixture(FIXTURE)
    for i in range(0, len(fixture), 50):
        store.write_batch(fixture[i : i + 50])
    files_before = store.files()

    report = await replay(tmp_path / "raw", tmp_path / "replay.duckdb", batch_size=100)

    assert (report.raw, report.kept, report.rows) == (500, EXPECTED_KEPT, EXPECTED_KEPT)
    assert report.buckets >= 1
    assert store.files() == files_before, "a replay reads the lake, it never writes it"
    con = connect(tmp_path / "replay.duckdb", read_only=True)
    assert con.execute("SELECT count(*) FROM wiki_events").fetchone() == (EXPECTED_KEPT,)
    assert con.execute("SELECT sum(count) FROM agg_minutely").fetchone() == (EXPECTED_KEPT,)
    con.close()

    again = await replay(tmp_path / "raw", tmp_path / "replay.duckdb", batch_size=100)
    assert again.rows == EXPECTED_KEPT, "replaying twice is idempotent (ADR 0005)"


def test_replay_cli_prints_a_report(tmp_path: Path, capsys) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    store.write_batch(load_fixture(FIXTURE)[:100])

    assert (
        main(["replay", "--lake", str(tmp_path / "raw"), "--db", str(tmp_path / "r.duckdb")]) == 0
    )

    out = capsys.readouterr().out
    assert "replayed 100 raw events from" in out  # structlog debug lines may precede it
    assert "minute buckets aggregated" in out


def test_replay_cli_window_flags_parse_naive_timestamps_as_utc(tmp_path: Path, capsys) -> None:
    store = ParquetRawStore(tmp_path / "raw")
    raws = raws_across_two_hours()
    store.write_batch(raws)

    main(
        [
            "replay",
            "--lake",
            str(tmp_path / "raw"),
            "--db",
            str(tmp_path / "w.duckdb"),
            "--since",
            "2026-10-11T11:00:00",
        ]
    )

    assert "replayed 4 raw events" in capsys.readouterr().out  # 11:00:00 .. 11:00:15
