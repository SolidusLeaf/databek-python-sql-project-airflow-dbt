"""Rebuild raw.raw_events from the JSON backup files (disaster recovery).

The ingest task saves every downloaded feed under RAW_BACKUP_DIR. If the
DuckDB file is lost or recreated, this script loads all of those files back.

    python /opt/airflow/include/replay_backups.py

It is safe to run at any time: load_raw() is idempotent, so versions that are
already stored are skipped. Pause the DAG first, so two processes do not try
to write to the DuckDB file at once.

Note: ingested_at will show the time of the replay, not the original download.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

import duckdb

from include.ingest import DB_FILENAME, load_raw

log = logging.getLogger(__name__)


def replay(backup_dir: Path, warehouse_dir: Path) -> dict:
    loaded = []
    for path in sorted(Path(backup_dir).rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            generated = int(payload["metadata"]["generated"])
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            log.warning("Skipping unreadable backup %s: %s", path, exc)
            continue
        loaded.append((generated, path, payload))

    # Oldest download first, so the history is replayed in the original order.
    loaded.sort(key=lambda item: item[0])

    warehouse_dir = Path(warehouse_dir)
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    inserted = 0
    con = duckdb.connect(str(warehouse_dir / DB_FILENAME))
    try:
        for _, path, payload in loaded:
            inserted += load_raw(con, payload)["inserted"]
    finally:
        con.close()

    return {"files": len(loaded), "inserted": inserted}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    backup_dir = Path(
        sys.argv[1] if len(sys.argv) > 1
        else os.environ.get("RAW_BACKUP_DIR", "/opt/airflow/data/raw_json")
    )
    warehouse_dir = Path(os.environ.get("WAREHOUSE_DIR", "/opt/airflow/warehouse"))

    stats = replay(backup_dir, warehouse_dir)
    log.info("replayed %d backup files, inserted %d new rows", stats["files"], stats["inserted"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())