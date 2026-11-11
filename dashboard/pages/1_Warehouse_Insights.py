"""Warehouse-derived charts for the Phase 8 business-insights page."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
QUERY_DIR = ROOT / "queries" / "warehouse"
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="Cosmic Eye Warehouse Insights", page_icon="🌌", layout="wide")
st.title("Cosmic Eye — warehouse insights")
st.caption(
    "These charts query the curated BigQuery tables. Observation quality values are synthetic; "
    "the SPHEREx archive metadata retains its own provenance."
)


@st.cache_resource
def get_bigquery_client():
    from google.cloud import bigquery

    project = os.getenv("GCP_PROJECT_ID", "").strip()
    if not project:
        raise RuntimeError("GCP_PROJECT_ID is missing from the repository-root .env file")
    return bigquery.Client(project=project, location=os.getenv("BIGQUERY_LOCATION", "US").strip())


@st.cache_data(ttl=300, show_spinner=False)
def query_rows(sql: str) -> pd.DataFrame:
    from google.cloud import bigquery

    client = get_bigquery_client()
    config = bigquery.QueryJobConfig(
        use_query_cache=True,
        maximum_bytes_billed=4 * 1024**3,
    )
    rows = client.query(sql, job_config=config).result()
    return pd.DataFrame([dict(row.items()) for row in rows])


def load_saved_query(query_id: str) -> str:
    matches = sorted(QUERY_DIR.glob(f"{query_id}_*.sql"))
    if not matches:
        raise FileNotFoundError(f"No saved query found for {query_id}")
    project = os.getenv("GCP_PROJECT_ID", "cosmic-eye-telemetry-demo").strip()
    return matches[0].read_text(encoding="utf-8").replace("cosmic-eye-telemetry-demo", project)


def draw_chart(title: str, frame: pd.DataFrame, x: str, y: list[str], takeaway: str) -> None:
    st.subheader(title)
    st.caption(takeaway)
    st.bar_chart(frame.set_index(x)[y], width="stretch")


try:
    if st.button("Refresh warehouse results", help="Clear the five-minute cache and rerun the read-only queries"):
        query_rows.clear()
        st.rerun()

    with st.spinner("Reading curated BigQuery summaries…"):
        by_band = query_rows(load_saved_query("q01"))
        noisy_regions = query_rows(load_saved_query("q02"))
        usage = query_rows(load_saved_query("q06"))
        project = os.getenv("GCP_PROJECT_ID", "cosmic-eye-telemetry-demo").strip()
        busy_regions_sql = f"""
        WITH exposure_daily AS (
          SELECT DATE(ingested_at) AS ingestion_day, region_key, COUNT(*) AS exposure_count
          FROM `{project}.curated.fact_exposures`
          WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
          GROUP BY ingestion_day, region_key
        ), usage_daily AS (
          SELECT DATE(ingested_at) AS ingestion_day, region_key, COUNT(*) AS usage_event_count
          FROM `{project}.curated.fact_usage_events`
          WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
          GROUP BY ingestion_day, region_key
        )
        SELECT COALESCE(e.region_key, u.region_key) AS region_key,
               SUM(COALESCE(e.exposure_count, 0)) AS exposure_count,
               SUM(COALESCE(u.usage_event_count, 0)) AS usage_event_count
        FROM exposure_daily e
        FULL OUTER JOIN usage_daily u USING (ingestion_day, region_key)
        GROUP BY region_key
        ORDER BY exposure_count + usage_event_count DESC, region_key
        LIMIT 10
        """
        busy_regions = query_rows(busy_regions_sql)

    exposure_total = int(by_band["exposure_count"].sum()) if not by_band.empty else 0
    usage_total = int(usage["request_count"].sum()) if not usage.empty else 0
    a, b, c = st.columns(3)
    a.metric("Curated exposures in query window", f"{exposure_total:,}")
    b.metric("Curated synthetic app events", f"{usage_total:,}")
    c.metric("Regions with exposure and/or app activity", f"{len(busy_regions):,}")

    left, right = st.columns(2)
    with left:
        band_takeaway = "No exposure facts matched the selected period."
        if not by_band.empty:
            row = by_band.sort_values("exposure_count", ascending=False).iloc[0]
            band_takeaway = (
                f"{row['band_id']} has the most recorded exposures ({int(row['exposure_count']):,}); "
                f"its mean synthetic noise sigma is {row['average_noise_sigma']:.3f}."
            )
        draw_chart("1. Exposure volume by wavelength band", by_band, "band_id", ["exposure_count"], band_takeaway)

        region_takeaway = "No region met the query’s minimum exposure count."
        if not noisy_regions.empty:
            row = noisy_regions.sort_values("average_noise_sigma", ascending=False).iloc[0]
            region_takeaway = (
                f"{row['region_key']} has the highest mean synthetic noise sigma "
                f"({row['average_noise_sigma']:.3f}) among regions with at least two exposures."
            )
        draw_chart(
            "2. Regions with the highest mean noise",
            noisy_regions.head(10).sort_values("average_noise_sigma"),
            "region_key",
            ["average_noise_sigma"],
            region_takeaway,
        )

        latency_takeaway = "No app usage facts matched the selected period."
        if not usage.empty:
            row = usage.sort_values("p95_latency_ms", ascending=False).iloc[0]
            latency_takeaway = (
                f"{row['event_name']} has the highest p95 latency ({int(row['p95_latency_ms']):,} ms) "
                "among the recorded synthetic app actions."
            )
        draw_chart(
            "3. 95th-percentile latency by app action",
            usage.sort_values("p95_latency_ms"),
            "event_name",
            ["p95_latency_ms"],
            latency_takeaway,
        )

    with right:
        error_takeaway = "No app usage facts matched the selected period."
        if not usage.empty:
            row = usage.sort_values("error_count", ascending=False).iloc[0]
            error_takeaway = (
                f"{row['event_name']} has the most 4xx/5xx responses ({int(row['error_count']):,}) "
                "in the synthetic usage records."
            )
        draw_chart(
            "4. HTTP errors by app action",
            usage.sort_values("error_count"),
            "event_name",
            ["error_count"],
            error_takeaway,
        )

        busy_takeaway = "No region activity was found in either fact table."
        if not busy_regions.empty:
            row = busy_regions.iloc[0]
            busy_takeaway = (
                f"{row['region_key']} has the most combined activity: "
                f"{int(row['exposure_count']):,} exposures and {int(row['usage_event_count']):,} app events."
            )
        draw_chart(
            "5. Busiest sky regions across both fact tables",
            busy_regions,
            "region_key",
            ["exposure_count", "usage_event_count"],
            busy_takeaway,
        )

    with st.expander("Show the warehouse SQL behind these charts"):
        for query_id in ("q01", "q02", "q06"):
            st.markdown(f"**{query_id}**")
            st.code(load_saved_query(query_id), language="sql")
        st.markdown("**Combined region activity query**")
        st.code(busy_regions_sql, language="sql")

except Exception as exc:
    st.error(f"Could not load warehouse insights ({type(exc).__name__}). Check .env and BigQuery access.")
