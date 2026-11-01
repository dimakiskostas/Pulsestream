import io
import json
import logging

import structlog

from pulsestream.common.logging import bind_context, clear_context, configure_logging


def test_json_format_renders_structlog_and_stdlib_lines_alike() -> None:
    stream = io.StringIO()
    configure_logging("INFO", "json", stream=stream)
    try:
        bind_context(ingest_id="abc-123")
        structlog.get_logger("pulsestream.test").info("batch.processed", raw=50, kept=31)
        logging.getLogger("uvicorn.error").warning("stdlib says %s", "hello")
    finally:
        clear_context()
        configure_logging("INFO", "console")

    lines = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert len(lines) == 2
    first, second = lines
    assert first["event"] == "batch.processed" and first["raw"] == 50 and first["kept"] == 31
    assert first["level"] == "info" and first["logger"] == "pulsestream.test"
    assert first["ingest_id"] == "abc-123", "bound context travels with the line"
    assert first["timestamp"].endswith("Z")
    assert second["event"] == "stdlib says hello" and second["level"] == "warning"
    assert second["logger"] == "uvicorn.error"
    for line in lines:
        internals = {key for key in line if key.startswith("_")}
        assert not internals, f"formatter internals leaked into the output: {internals}"


def test_level_filters_and_console_format_is_human_readable() -> None:
    stream = io.StringIO()
    configure_logging("WARNING", "console", stream=stream)
    try:
        log = structlog.get_logger("pulsestream.test")
        log.info("hidden", because="level")
        log.warning("shown", code=7)
        logging.getLogger("httpx").warning("quiet logger still shows warnings")
    finally:
        configure_logging("INFO", "console")

    text = stream.getvalue()
    assert "hidden" not in text
    assert "shown" in text and "code=7" in text
    assert "quiet logger still shows warnings" in text


def test_context_is_cleared() -> None:
    stream = io.StringIO()
    configure_logging("INFO", "json", stream=stream)
    try:
        bind_context(entity="enwiki")
        clear_context()
        structlog.get_logger("t").info("after clear")
    finally:
        configure_logging("INFO", "console")
    assert "entity" not in json.loads(stream.getvalue().strip())
