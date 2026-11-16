"""Streamlit dashboard for the earthquake pipeline.

Reads ONLY the read-only serving copy that the DAG's last task publishes
(earthquakes_serving.duckdb). It never touches the live warehouse file.
"""
import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

SERVING_DB = os.environ.get("SERVING_DB", "/warehouse/earthquakes_serving.duckdb")

WINDOWS = {"Last 24 hours": 1, "Last 7 days": 7, "Last 30 days": 30, "Everything": None}

# Prefix digits so the bar chart sorts the buckets in the natural order.
BUCKET_LABELS = {
    "below 2": "1: below 2",
    "2 to 3.9": "2: 2 to 3.9",
    "4 to 5.9": "3: 4 to 5.9",
    "6 and above": "4: 6 and above",
    "unknown": "5: unknown",
}

st.set_page_config(page_title="Earthquakes pipeline", layout="wide")


def run_query(sql, params=None):
    # A fresh connection every time: the serving file is replaced (renamed)
    # every 5 minutes, and an old connection would keep reading the old file.
    # ATTACH under the name "earthquakes" so the dbt views keep working.
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{SERVING_DB}' AS earthquakes (READ_ONLY)")
        con.execute("USE earthquakes")
        return con.execute(sql, params or []).df()
    finally:
        con.close()


def build_where(days, min_mag, only_earthquakes):
    """WHERE clause on the location_date table (alias f) plus its parameters."""
    clauses, params = [], []
    if days is not None:
        clauses.append(
            "f.event_time >= (SELECT max(event_time) FROM main.earthquake_location_date)"
            " - to_days(CAST(? AS INTEGER))"
        )
        params.append(days)
    clauses.append("coalesce(f.magnitude, -99) >= ?")  # events without magnitude are hidden
    params.append(min_mag)
    if only_earthquakes:
        clauses.append("f.event_type = 'earthquake'")
    return " AND ".join(clauses), params


@st.cache_data(ttl=60)
def get_freshness():
    return run_query(
        """
        SELECT cast(max(ingested_at) AS VARCHAR) AS latest,
               date_diff('minute', max(ingested_at), now()) AS minutes_ago
        FROM raw.raw_events
        """
    )


@st.cache_data(ttl=60)
def get_summary(days, min_mag, only_earthquakes):
    where, params = build_where(days, min_mag, only_earthquakes)
    return run_query(
        f"""
        SELECT count(*)                                         AS events,
               max(f.magnitude)                                 AS strongest,
               count(*) FILTER (WHERE e.alert IS NOT NULL)      AS with_alert,
               count(*) FILTER (WHERE e.versions_seen > 1)      AS revised
        FROM main.earthquake_location_date f
        JOIN main.earthquakes e USING (event_id)
        WHERE {where}
        """,
        params,
    )


@st.cache_data(ttl=60)
def get_per_day(days, min_mag, only_earthquakes):
    where, params = build_where(days, min_mag, only_earthquakes)
    return run_query(
        f"""
        SELECT f.event_date, count(*) AS events
        FROM main.earthquake_location_date f
        WHERE {where}
        GROUP BY 1 ORDER BY 1
        """,
        params,
    )


@st.cache_data(ttl=60)
def get_buckets(days, min_mag, only_earthquakes):
    where, params = build_where(days, min_mag, only_earthquakes)
    return run_query(
        f"""
        SELECT f.magnitude_bucket, count(*) AS events
        FROM main.earthquake_location_date f
        WHERE {where}
        GROUP BY 1
        """,
        params,
    )


@st.cache_data(ttl=60)
def get_top_areas(days, min_mag, only_earthquakes):
    where, params = build_where(days, min_mag, only_earthquakes)
    return run_query(
        f"""
        SELECT f.area, count(*) AS events, max(f.magnitude) AS strongest
        FROM main.earthquake_location_date f
        WHERE {where}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """,
        params,
    )


@st.cache_data(ttl=60)
def get_map_points(days, min_mag, only_earthquakes):
    where, params = build_where(days, min_mag, only_earthquakes)
    return run_query(
        f"""
        SELECT f.latitude, f.longitude, f.magnitude
        FROM main.earthquake_location_date f
        WHERE {where} AND f.latitude IS NOT NULL AND f.longitude IS NOT NULL
        ORDER BY f.event_time DESC LIMIT 2000
        """,
        params,
    )


@st.cache_data(ttl=60)
def get_most_revised():
    return run_query(
        """
        SELECT event_id, place, magnitude, status, versions_seen
        FROM main.earthquakes
        ORDER BY versions_seen DESC, event_time DESC
        LIMIT 10
        """
    )


@st.cache_data(ttl=60)
def get_alerts():
    return run_query(
        """
        SELECT event_time, place, magnitude, alert, tsunami, significance, event_url
        FROM main.earthquake_alerts
        ORDER BY event_time DESC
        LIMIT 50
        """
    )


# ---------------------------------------------------------------- page ----

st.title("Earthquakes pipeline")
st.caption(
    "USGS feed → Airflow → DuckDB with dbt → this dashboard. "
    "It reads a read-only copy that is refreshed every 5 minutes."
)

if not Path(SERVING_DB).exists():
    st.warning(
        "The serving copy does not exist yet. Run the earthquake_pipeline DAG once "
        "in Airflow (the last task, publish_serving_copy, creates it), then refresh."
    )
    st.stop()

with st.sidebar:
    st.header("Filters")
    window_label = st.selectbox("Time window", list(WINDOWS), index=1)
    min_mag = st.slider("Minimum magnitude", -2.0, 8.0, -2.0, 0.1)
    only_eq = st.checkbox("Only event type 'earthquake'", value=True)
    if st.button("Refresh now"):
        st.cache_data.clear()
        st.rerun()

days = WINDOWS[window_label]

try:
    fresh = get_freshness()
    summary = get_summary(days, min_mag, only_eq)
    per_day = get_per_day(days, min_mag, only_eq)
    buckets = get_buckets(days, min_mag, only_eq)
    top_areas = get_top_areas(days, min_mag, only_eq)
    points = get_map_points(days, min_mag, only_eq)
    most_revised = get_most_revised()
    alerts = get_alerts()
except duckdb.Error as exc:
    st.error(f"Could not read the serving copy: {exc}")
    st.stop()

latest = fresh["latest"].iloc[0]
if pd.isna(latest):
    st.info("The warehouse has no data yet.")
    st.stop()
st.caption(
    f"Newest data arrived {int(fresh['minutes_ago'].iloc[0])} minutes ago "
    f"({str(latest)[:19]} UTC). Freshness shows when NEW data last arrived, "
    "not when the pipeline last ran."
)

row = summary.iloc[0]
if int(row["events"]) == 0:
    st.info("No events match these filters.")
    st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Events", f"{int(row['events']):,}")
c2.metric("Strongest magnitude", f"{row['strongest']:.1f}")
c3.metric("With PAGER alert", f"{int(row['with_alert']):,}")
c4.metric("Revised at least once", f"{int(row['revised']):,}")

left, right = st.columns(2)
with left:
    st.subheader("Events per day")
    per_day["event_date"] = pd.to_datetime(per_day["event_date"])
    st.bar_chart(per_day.set_index("event_date")["events"])
with right:
    st.subheader("Magnitude buckets")
    buckets["label"] = buckets["magnitude_bucket"].map(BUCKET_LABELS)
    st.bar_chart(buckets.set_index("label")["events"])

st.subheader("Where (latest 2,000 matching events)")
points["size_m"] = (points["magnitude"].fillna(0).clip(lower=0) + 1) ** 2 * 3000
st.map(points, latitude="latitude", longitude="longitude", size="size_m")

left, right = st.columns(2)
with left:
    st.subheader("Top areas")
    st.dataframe(top_areas, hide_index=True)
    st.caption(
        "Area is the last part of the USGS place name, so names are not uniform "
        "(for example CA and California are counted separately)."
    )
with right:
    st.subheader("Most revised events")
    st.dataframe(most_revised, hide_index=True)
    st.caption(
        "USGS revises events after publishing them. The raw layer keeps every "
        "version; the clean table keeps the latest."
    )

st.subheader("Events with a PAGER alert (view: earthquake_alerts)")
st.dataframe(
    alerts,
    hide_index=True,
    column_config={"event_url": st.column_config.LinkColumn("USGS page")},
)