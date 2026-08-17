import duckdb
import pytest

from pulsestream.storage.duck import init_db


@pytest.fixture
def con():
    con = duckdb.connect(":memory:")
    init_db(con)
    yield con
    con.close()
