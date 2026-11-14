"""Tests for include/ingest.py. No network and no real warehouse needed.

Run inside the container:
    docker compose exec airflow python -m pytest /opt/airflow/tests -v
"""
import json
from pathlib import Path

import duckdb
import pytest
import requests

from include import ingest

FIXTURES = Path(__file__).parent / "fixtures"


# ---------- helpers -------------------------------------------------------

def make_feature(event_id, updated_ms, mag=1.0, status="automatic"):
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "mag": mag,
            "place": "somewhere",
            "time": updated_ms - 1000,
            "updated": updated_ms,
            "status": status,
        },
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0, 5.0]},
    }


def make_payload(features, generated_ms=1_000_000):
    return {
        "type": "FeatureCollection",
        "metadata": {"generated": generated_ms, "count": len(features)},
        "features": features,
    }


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def row_count(con):
    return con.execute("SELECT count(*) FROM raw.raw_events").fetchone()[0]


# ---------- load_raw: idempotency ----------------------------------------

def test_load_is_idempotent(con):
    payload = make_payload(
        [make_feature("a", 100), make_feature("b", 100), make_feature("c", 100)]
    )
    first = ingest.load_raw(con, payload)
    second = ingest.load_raw(con, payload)

    assert first["inserted"] == 3
    assert second["inserted"] == 0
    assert row_count(con) == 3


def test_revised_event_is_stored_as_new_version(con):
    ingest.load_raw(con, make_payload([make_feature("a", 100, status="automatic")]))
    stats = ingest.load_raw(
        con, make_payload([make_feature("a", 200, status="reviewed")], generated_ms=2_000_000)
    )

    assert stats["inserted"] == 1
    assert row_count(con) == 2
    versions = con.execute(
        "SELECT updated_ms FROM raw.raw_events WHERE event_id = 'a' ORDER BY updated_ms"
    ).fetchall()
    assert versions == [(100,), (200,)]


def test_overlapping_feeds_only_add_new_events(con):
    # Hourly windows overlap: the second download repeats "b" and adds "c".
    ingest.load_raw(con, make_payload([make_feature("a", 100), make_feature("b", 100)]))
    stats = ingest.load_raw(con, make_payload([make_feature("b", 100), make_feature("c", 100)]))

    assert stats["fetched"] == 2
    assert stats["inserted"] == 1
    assert row_count(con) == 3


# ---------- load_raw: edge cases -----------------------------------------

def test_features_without_id_or_updated_are_skipped(con):
    no_id = make_feature("x", 100)
    del no_id["id"]
    no_updated = make_feature("y", 100)
    no_updated["properties"]["updated"] = None

    stats = ingest.load_raw(con, make_payload([no_id, no_updated, make_feature("ok", 100)]))

    assert stats == {"fetched": 3, "skipped": 2, "inserted": 1}
    assert row_count(con) == 1


def test_empty_feed_creates_table_and_inserts_nothing(con):
    stats = ingest.load_raw(con, make_payload([]))

    assert stats == {"fetched": 0, "skipped": 0, "inserted": 0}
    assert row_count(con) == 0


def test_payload_is_valid_json_and_extractable(con):
    ingest.load_raw(con, make_payload([make_feature("a", 100, mag=2.5)]))

    mag = con.execute(
        "SELECT payload->>'$.properties.mag' FROM raw.raw_events"
    ).fetchone()[0]
    assert float(mag) == 2.5


# ---------- save_backup ---------------------------------------------------

def test_save_backup_writes_file_once(tmp_path):
    payload = make_payload([make_feature("a", 100)], generated_ms=1_794_567_759_000)

    first = ingest.save_backup(payload, tmp_path, "all_hour")
    second = ingest.save_backup(payload, tmp_path, "all_hour")

    assert first == second
    assert first.name == "all_hour_1794567759000.json"
    assert json.loads(first.read_text(encoding="utf-8")) == payload
    assert list(tmp_path.rglob("*.tmp")) == []  # no half-written leftovers
    assert len(list(tmp_path.rglob("*.json"))) == 1


# ---------- fetch_feed (network mocked) -----------------------------------

class FakeResponse:
    def __init__(self, payload=None, error=None):
        self._payload = payload
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise self._error

    def json(self):
        return self._payload


def test_fetch_feed_retries_then_succeeds(monkeypatch):
    good = make_payload([make_feature("a", 100)])
    responses = [
        FakeResponse(error=requests.ConnectionError("boom")),
        FakeResponse(error=requests.Timeout("slow")),
        FakeResponse(payload=good),
    ]
    calls = []

    def fake_get(url, timeout):
        calls.append(url)
        return responses.pop(0)

    monkeypatch.setattr(ingest.requests, "get", fake_get)
    monkeypatch.setattr(ingest.time, "sleep", lambda s: None)

    assert ingest.fetch_feed("http://example.test/feed") == good
    assert len(calls) == 3


def test_fetch_feed_gives_up_after_all_attempts(monkeypatch):
    def fake_get(url, timeout):
        return FakeResponse(error=requests.ConnectionError("down"))

    monkeypatch.setattr(ingest.requests, "get", fake_get)
    monkeypatch.setattr(ingest.time, "sleep", lambda s: None)

    with pytest.raises(RuntimeError):
        ingest.fetch_feed("http://example.test/feed", attempts=3)


def test_fetch_feed_rejects_non_usgs_json(monkeypatch):
    monkeypatch.setattr(
        ingest.requests, "get", lambda url, timeout: FakeResponse(payload={"hello": "world"})
    )
    monkeypatch.setattr(ingest.time, "sleep", lambda s: None)

    with pytest.raises(RuntimeError):
        ingest.fetch_feed("http://example.test/feed", attempts=2)


# ---------- real samples (your own backup files) --------------------------

@pytest.mark.skipif(
    len(list(FIXTURES.glob("all_hour_*.json"))) < 2,
    reason="copy two backup files from data/raw_json into tests/fixtures/",
)
def test_real_backup_files_load_and_reload(con):
    files = sorted(FIXTURES.glob("all_hour_*.json"))
    payloads = [json.loads(f.read_text(encoding="utf-8")) for f in files]
    total_events = sum(len(p["features"]) for p in payloads)

    for payload in payloads:
        ingest.load_raw(con, payload)
    first_count = row_count(con)

    for payload in payloads:  # replaying the same files must change nothing
        stats = ingest.load_raw(con, payload)
        assert stats["inserted"] == 0

    assert row_count(con) == first_count
    assert first_count <= total_events


# ---------- a real revision, captured from the live feed ------------------

REVISION_FILES = [
    "all_hour_1794597579000.json",
    "all_hour_1794597879000.json",
    "all_hour_1794598599000.json",
]


@pytest.mark.skipif(
    not all((FIXTURES / name).exists() for name in REVISION_FILES),
    reason="copy the three all_hour_17945975*/17945978*/17945985* backups into tests/fixtures/",
)
def test_real_revision_is_captured(con):
    """Three real downloads: 22 events in total, 9 distinct versions.

    - file 2 adds one new event (us7000ty3q)
    - file 3 contains a REVISED event: nc75466172 got a new `updated` value
      and its `types` list gained "scitech-link"
    """
    inserted = []
    for name in REVISION_FILES:
        payload = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
        inserted.append(ingest.load_raw(con, payload)["inserted"])

    assert inserted == [7, 1, 1]
    assert row_count(con) == 9

    versions = con.execute(
        """
        SELECT updated_ms, payload->>'$.properties.types'
        FROM raw.raw_events
        WHERE event_id = 'nc75466172'
        ORDER BY updated_ms
        """
    ).fetchall()

    assert [v[0] for v in versions] == [1794597001356, 1794598343569]
    assert "scitech-link" not in versions[0][1]
    assert "scitech-link" in versions[1][1]