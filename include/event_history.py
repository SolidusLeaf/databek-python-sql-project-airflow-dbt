"""Print every stored version of one earthquake, oldest first.

    python /opt/airflow/include/event_history.py               # most-revised event
    python /opt/airflow/include/event_history.py uw714139181   # a specific event

Handy for the demo: shows how an event changed between USGS revisions.
Pause the DAG first (or wait a minute) if you hit a file-lock error.
"""
import os
import sys
from pathlib import Path

import duckdb

DB_FILENAME = "earthquakes.duckdb"

HISTORY_SQL = """
SELECT
    epoch_ms(updated_ms)                    AS updated_at,
    payload->>'$.properties.mag'            AS mag,
    payload->>'$.properties.magType'        AS mag_type,
    payload->>'$.properties.status'         AS status,
    payload->>'$.properties.sig'            AS sig,
    payload->>'$.properties.nst'            AS stations,
    payload->>'$.properties.alert'          AS alert
FROM raw.raw_events
WHERE event_id = ?
ORDER BY updated_ms
"""

MOST_REVISED_SQL = """
SELECT event_id
FROM raw.raw_events
GROUP BY event_id
ORDER BY count(*) DESC, event_id
LIMIT 1
"""


def print_table(columns, rows):
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [
        max(len(col), *(len(r[i]) for r in cells)) if cells else len(col)
        for i, col in enumerate(columns)
    ]
    line = "  ".join(col.ljust(widths[i]) for i, col in enumerate(columns))
    print(line)
    print("  ".join("-" * w for w in widths))
    for r in cells:
        print("  ".join(r[i].ljust(widths[i]) for i in range(len(columns))))


def main() -> int:
    warehouse_dir = Path(os.environ.get("WAREHOUSE_DIR", "/opt/airflow/warehouse"))
    con = duckdb.connect(str(warehouse_dir / DB_FILENAME), read_only=True)
    try:
        event_id = sys.argv[1] if len(sys.argv) > 1 else None
        if event_id is None:
            row = con.execute(MOST_REVISED_SQL).fetchone()
            if row is None:
                print("raw.raw_events is empty")
                return 1
            event_id = row[0]

        cursor = con.execute(HISTORY_SQL, [event_id])
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
    finally:
        con.close()

    if not rows:
        print(f"No versions found for event {event_id}")
        return 1

    print(f"Event {event_id}: {len(rows)} stored versions\n")
    print_table(columns, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())