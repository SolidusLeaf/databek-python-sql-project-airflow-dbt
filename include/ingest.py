"""Ingest the USGS earthquake GeoJSON feed into the DuckDB raw layer.

Steps
-----
1. fetch_feed   - download the feed (with a few retries)
2. save_backup  - keep the exact JSON we received on disk (replayable backup)
3. load_raw     - insert every event *version* into raw.raw_events

Idempotency
-----------
raw.raw_events has the primary key (event_id, updated_ms). USGS bumps
`updated` whenever it revises an event, so:
  * the same version of an event is never stored twice (re-running is safe)
  * a revised event is stored as a NEW row, so the revision history is kept

Run it on its own:
    python /opt/airflow/include/ingest.py
or import it from a DAG:
    from include.ingest import run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

log = logging.getLogger(__name__)

DEFAULT_FEED_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
)
DB_FILENAME = "earthquakes.duckdb"

CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS raw"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.raw_events (
    event_id            VARCHAR     NOT NULL,   -- feature "id" from USGS
    updated_ms          BIGINT      NOT NULL,   -- properties.updated (epoch ms)
    source_generated_ms BIGINT,                 -- metadata.generated of the file
    payload             JSON        NOT NULL,   -- the whole feature as received
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (event_id, updated_ms)
)
"""

INSERT_SQL = """
INSERT INTO raw.raw_events (event_id, updated_ms, source_generated_ms, payload)
VALUES (?, ?, ?, CAST(? AS JSON))
ON CONFLICT DO NOTHING
"""


def fetch_feed(
    url: str,
    attempts: int = 3,
    backoff_seconds: float = 2.0,
    timeout: float = 30.0,
) -> dict:
    """Download the GeoJSON feed. Retries a few times before giving up."""
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if "features" not in payload or "metadata" not in payload:
                raise ValueError("Response does not look like a USGS GeoJSON feed")
            return payload
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            log.warning("Fetch attempt %d/%d failed: %s", attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(backoff_seconds * attempt)
    raise RuntimeError(f"Could not fetch {url} after {attempts} attempts") from last_error


def save_backup(payload: dict, backup_dir: Path, feed_name: str) -> Path:
    """Write the raw JSON to <backup_dir>/<YYYY-MM-DD>/<feed>_<generated_ms>.json.

    The file name comes from the feed's own `generated` timestamp, so the same
    version of the feed always maps to the same file name (no duplicates).
    """
    generated_ms = int(payload["metadata"]["generated"])
    generated_at = datetime.fromtimestamp(generated_ms / 1000, tz=timezone.utc)

    day_dir = Path(backup_dir) / generated_at.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)

    target = day_dir / f"{feed_name}_{generated_ms}.json"
    if target.exists():
        return target

    # Write to a temp file first, then rename: a crash never leaves a
    # half-written backup behind.
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, target)
    return target


def load_raw(con: duckdb.DuckDBPyConnection, payload: dict) -> dict:
    """Insert every event version into raw.raw_events (idempotent)."""
    generated_ms = int(payload["metadata"]["generated"])

    rows = []
    skipped = 0
    for feature in payload["features"]:
        event_id = feature.get("id")
        updated_ms = (feature.get("properties") or {}).get("updated")
        if event_id is None or updated_ms is None:
            skipped += 1
            log.warning("Skipping feature without id/updated: %.200s", json.dumps(feature))
            continue
        rows.append((str(event_id), int(updated_ms), generated_ms, json.dumps(feature)))

    con.execute(CREATE_SCHEMA_SQL)
    con.execute(CREATE_TABLE_SQL)

    before = con.execute("SELECT count(*) FROM raw.raw_events").fetchone()[0]

    con.execute("BEGIN TRANSACTION")
    try:
        if rows:
            con.executemany(INSERT_SQL, rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    after = con.execute("SELECT count(*) FROM raw.raw_events").fetchone()[0]

    return {
        "fetched": len(payload["features"]),
        "skipped": skipped,
        "inserted": after - before,
    }


def run(
    feed_url: str | None = None,
    warehouse_dir: str | None = None,
    backup_dir: str | None = None,
) -> dict:
    """Fetch -> back up -> load. Returns a small stats dict."""
    feed_url = feed_url or os.environ.get("USGS_FEED_URL", DEFAULT_FEED_URL)
    warehouse_dir = Path(
        warehouse_dir or os.environ.get("WAREHOUSE_DIR", "/opt/airflow/warehouse")
    )
    backup_dir = Path(
        backup_dir or os.environ.get("RAW_BACKUP_DIR", "/opt/airflow/data/raw_json")
    )
    # "https://.../all_hour.geojson" -> "all_hour"
    feed_name = feed_url.rstrip("/").split("/")[-1].split(".")[0]

    payload = fetch_feed(feed_url)
    backup_file = save_backup(payload, backup_dir, feed_name)

    warehouse_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse_dir / DB_FILENAME))
    try:
        stats = load_raw(con, payload)
    finally:
        # Always close: an open connection keeps the write lock on the file
        # and would block the next task (dbt) from opening it.
        con.close()

    stats["backup_file"] = str(backup_file)
    return stats


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--feed-url",
        default=None,
        help="Override the feed URL (default: USGS_FEED_URL env var, else all_hour).",
    )
    args = parser.parse_args()

    stats = run(feed_url=args.feed_url)
    log.info(
        "fetched %d events, inserted %d new rows, skipped %d (backup: %s)",
        stats["fetched"],
        stats["inserted"],
        stats["skipped"],
        stats["backup_file"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())