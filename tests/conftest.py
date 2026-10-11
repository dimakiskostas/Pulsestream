from collections.abc import Iterator

import duckdb
import pytest

from pulsestream.storage.duck import connect


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    """An in-memory DuckDB with the schema created and the session pinned to UTC."""
    con = connect(":memory:")
    yield con
    con.close()
