"""One logging configuration for the whole process.

structlog renders every line, and the standard-library loggers that uvicorn,
APScheduler and httpx use are routed through the same renderer, so a log file is
one format throughout: human-readable console output while developing, JSON in
containers. ``PULSE_LOG_LEVEL`` and ``PULSE_LOG_FORMAT`` select both.

Context bound with ``bind_context`` (a batch's first ingest id, an entity) travels
with every line logged while it is bound, which is how one event can be traced
across the pipeline's stages.
"""

from __future__ import annotations

import logging
import sys
from typing import Literal, TextIO

import structlog

LogFormat = Literal["console", "json"]

# Loggers that libraries like to configure for themselves (uvicorn installs handlers unless
# told not to; Streamlit configures the uvicorn loggers too). They are re-routed through
# the root handler so one format and one level apply everywhere.
ROUTED_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access", "apscheduler", "streamlit")
# Third-party loggers that are chatty at INFO and say nothing useful at that level.
QUIET_LOGGERS = ("httpx", "httpcore", "apscheduler.executors.default", "watchfiles")


def configure_logging(
    level: str = "INFO", fmt: LogFormat = "console", *, stream: TextIO | None = None
) -> None:
    """Configure structlog and the standard library together; safe to call more than once."""
    numeric = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.make_filtering_bound_logger(numeric),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(foreign_pre_chain=shared, processors=[renderer])
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(numeric)
    for name in ROUTED_LOGGERS:
        routed = logging.getLogger(name)
        routed.handlers.clear()
        routed.propagate = True
        routed.setLevel(logging.NOTSET)
    for name in QUIET_LOGGERS:
        logging.getLogger(name).setLevel(max(numeric, logging.WARNING))


def bind_context(**values: object) -> None:
    """Attach key/value pairs to every line logged on this task/thread until cleared."""
    structlog.contextvars.bind_contextvars(**values)


def clear_context() -> None:
    structlog.contextvars.clear_contextvars()
