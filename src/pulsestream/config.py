"""Typed settings (Pydantic v2 ``BaseSettings``).

Every field can be overridden with a ``PULSE_*`` environment variable or a ``.env``
file in the working directory; see ``.env.example`` for the full list.
"""

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

from pulsestream.sources.wikimedia import DEFAULT_USER_AGENT, STREAM_URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PULSE_", env_file=".env", extra="ignore")

    # --- storage -------------------------------------------------------------
    db_path: str = "pulsestream.duckdb"

    # --- pipeline ------------------------------------------------------------
    queue_maxsize: int = 1000  # bounded queue == backpressure knob (HLD §7)
    batch_size: int = 50  # micro-batch size (HLD decision D4)
    flush_interval: float = 2.0  # seconds a partial batch may wait

    # --- source --------------------------------------------------------------
    source: Literal["wikimedia", "fake"] = "wikimedia"
    fixture_path: str = "tests/fixtures/sample_events.jsonl"  # used when source=fake
    fixture_delay: float = 0.0  # seconds between replayed events (0 = as fast as possible)
    stream_url: str = STREAM_URL
    user_agent: str = DEFAULT_USER_AGENT
    read_timeout: float = 60.0  # seconds without bytes before the stream counts as dead


settings = Settings()
