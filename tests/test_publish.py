"""Tests for include/publish.py (uses a temporary folder, no real warehouse)."""
import duckdb

from include import publish


def make_source(tmp_path, value):
    con = duckdb.connect(str(tmp_path / publish.SOURCE_FILENAME))
    con.execute("CREATE OR REPLACE TABLE t AS SELECT ? AS x", [value])
    con.close()


def read_serving(tmp_path):
    con = duckdb.connect(str(tmp_path / publish.SERVING_FILENAME), read_only=True)
    try:
        return con.execute("SELECT x FROM t").fetchone()[0]
    finally:
        con.close()


def test_publish_creates_a_readable_copy(tmp_path):
    make_source(tmp_path, 1)

    path = publish.publish_serving_copy(tmp_path)

    assert path.endswith(publish.SERVING_FILENAME)
    assert read_serving(tmp_path) == 1
    assert list(tmp_path.glob("*.tmp")) == []  # no leftovers


def test_publish_replaces_the_previous_copy(tmp_path):
    make_source(tmp_path, 1)
    publish.publish_serving_copy(tmp_path)

    make_source(tmp_path, 2)
    publish.publish_serving_copy(tmp_path)

    assert read_serving(tmp_path) == 2