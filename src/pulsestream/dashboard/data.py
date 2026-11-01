"""Data access for the dashboard: HTTP in, Polars frames out (ADR 0007).

No Streamlit import here, so this module is unit-tested against an httpx mock
transport, and the page module only renders what it gets.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx
import polars as pl

MINUTELY_SCHEMA = pl.Schema(
    {
        "bucket_ts": pl.Datetime("us", "UTC"),
        "entity": pl.Utf8(),
        "count": pl.Int64(),
        "bot_count": pl.Int64(),
        "avg_value": pl.Float64(),
        "max_value": pl.Int64(),
        "anomaly_count": pl.Int64(),
    }
)
TOP_SCHEMA = pl.Schema({"entity": pl.Utf8(), "count": pl.Int64(), "bot_count": pl.Int64()})


def _timestamp(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


class ApiClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 5.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)

    def close(self) -> None:
        self._client.close()

    def _get(self, path: str, **params: str | int | None) -> Any:
        query: dict[str, str | int] = {k: v for k, v in params.items() if v is not None}
        response = self._client.get(path, params=query)
        response.raise_for_status()
        return response.json()

    def health(self) -> dict[str, Any]:
        result: dict[str, Any] = self._get("/health")
        return result

    def stats(self) -> dict[str, Any]:
        result: dict[str, Any] = self._get("/stats/latest")
        return result

    def lake(self) -> dict[str, Any]:
        result: dict[str, Any] = self._get("/lake")
        return result

    def jobs(self) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = self._get("/jobs")["jobs"]
        return jobs

    def minutely(self, minutes: int = 60, entity: str | None = None) -> pl.DataFrame:
        points = self._get("/aggregates/minutely", minutes=minutes, entity=entity)["points"]
        rows = [{**point, "bucket_ts": _timestamp(point["bucket_ts"])} for point in points]
        return pl.DataFrame(rows, schema=MINUTELY_SCHEMA)

    def top_entities(self, minutes: int = 60, limit: int = 10) -> pl.DataFrame:
        entities = self._get("/entities/top", minutes=minutes, limit=limit)["entities"]
        return pl.DataFrame(entities, schema=TOP_SCHEMA)
