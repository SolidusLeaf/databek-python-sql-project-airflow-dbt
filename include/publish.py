"""Publish a read-only snapshot of the warehouse for the dashboard.

Why a copy? DuckDB lets only one process write to a file, and a reader cannot
open it while a writer holds it. The pipeline writes to earthquakes.duckdb;
the dashboard reads earthquakes_serving.duckdb, which this step replaces
atomically AFTER dbt has built and tested everything. If dbt fails, this step
does not run, so the dashboard keeps showing the last good copy.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb

SOURCE_FILENAME = "earthquakes.duckdb"
SERVING_FILENAME = "earthquakes_serving.duckdb"


def publish_serving_copy(warehouse_dir: str | Path | None = None) -> str:
    wdir = Path(warehouse_dir or os.environ.get("WAREHOUSE_DIR", "/opt/airflow/warehouse"))
    src = wdir / SOURCE_FILENAME
    dst = wdir / SERVING_FILENAME
    tmp = wdir / (SERVING_FILENAME + ".tmp")

    # Flush everything into the main file, then release the file again.
    con = duckdb.connect(str(src))
    try:
        con.execute("CHECKPOINT")
    finally:
        con.close()

    wal = Path(str(src) + ".wal")
    if wal.exists() and wal.stat().st_size > 0:
        raise RuntimeError(f"{wal} still has data; refusing to copy a half-flushed database")

    # Copy to a temp name, then rename: the dashboard never sees a partial file.
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)
    return str(dst)