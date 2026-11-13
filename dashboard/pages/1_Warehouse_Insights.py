"""Plain-language BigQuery analytics for the Cosmic Eye pipeline."""

from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
QUERY_DIR = ROOT / "queries" / "warehouse"
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="Warehouse Insights · Cosmic Eye", page_icon="📊", layout="wide")
st.markdown(
    """
    <style>
    .block-container {max-width: 1440px; padding-top: 1.7rem; padding-bottom: 3rem;}
    div[data-testid="stVerticalBlockBorderWrapper"] {background: rgba(13, 24, 42, .42); border-color: rgba(130, 160, 205, .2);}
    [data-testid="stMetric"] {padding: .8rem 1rem; border: 1px solid rgba(130, 160, 205, .18); border-radius: 10px; background: rgba(74, 105, 155, .1);}
    </style>
    """,
    unsafe_allow_html=True,
)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "").strip()
LOCATION = os.getenv("BIGQUERY_LOCATION", "US").strip() or "US"
DATASET = os.getenv("BIGQUERY_CURATED_DATASET", "curated").strip() or "curated"
PROJECT_PATTERN = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]")
DATASET_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,1023}")

st.title("Warehouse Insights")
st.caption("Understand SPHEREx archive coverage and the synthetic app activity stored in the curated warehouse.")

with st.container(border=True):
    real_col, synthetic_col = st.columns(2)
    with real_col:
        st.markdown("**Archive-backed data**")
        st.write("Exposure counts, observation IDs, wavelength bands, and sky positions come from IRSA SPHEREx archive metadata.")
    with synthetic_col:
        st.markdown("**Synthetic demo data**")
        st.write("Noise, saturation, request latency, HTTP errors, and app actions are generated examples—not telescope telemetry or personal user tracking.")

with st.expander("How to read these charts", expanded=False):
    st.markdown(
        "- **Exposure volume** counts curated archive records in the last 60 days; it is not the number of unique objects in the universe.\n"
        "- **Noise and saturation** are synthetic fields attached to demo exposures. They do not measure SPHEREx instrument performance.\n"
        "- **Latency and errors** summarize synthetic app actions. They do not describe this Streamlit page’s response time.\n"
        "- **Sky regions** are approximate RA/Dec grid cells, not named astronomical regions.\n"
        "- Query results are cached for five minutes. Use Refresh to rerun the read-only queries."
    )


@st.cache_resource(show_spinner=False)
def get_bigquery_client(project_id: str, location: str):
    from google.cloud import bigquery

    return bigquery.Client(project=project_id, location=location)


@st.cache_data(ttl=300, show_spinner=False)
def query_rows(sql: str, project_id: str, location: str) -> pd.DataFrame:
    from google.cloud import bigquery

    client = get_bigquery_client(project_id, location)
    config = bigquery.QueryJobConfig(use_query_cache=True, maximum_bytes_billed=4 * 1024**3)
    rows = client.query(sql, job_config=config).result()
    return pd.DataFrame([dict(row.items()) for row in rows])


def load_saved_query(query_id: str) -> str:
    matches = sorted(QUERY_DIR.glob(f"{query_id}_*.sql"))
    if not matches:
        raise FileNotFoundError(f"No saved query found for {query_id}")
    sql = matches[0].read_text(encoding="utf-8")
    sql = sql.replace("cosmic-eye-telemetry-demo", PROJECT_ID)
    return sql.replace(".curated.", f".{DATASET}.")


def draw_chart(title: str, frame: pd.DataFrame, x: str, y: list[str], note: str, *, horizontal: bool = False) -> None:
    with st.container(border=True):
        st.markdown(f"#### {title}")
        st.caption(note)
        required = {x, *y}
        if frame.empty or not required.issubset(frame.columns):
            st.info("No rows matched this chart for the current warehouse data.")
        elif horizontal:
            st.bar_chart(frame, x=x, y=y, horizontal=True, width="stretch")
        else:
            st.bar_chart(frame, x=x, y=y, width="stretch")


def show_connection_help(reason: str, detail: str) -> None:
    st.error(f"BigQuery is not ready: {reason}")
    st.write(detail)
    with st.expander("Connect this dashboard to Google Cloud", expanded=True):
        st.markdown(
            "1. Set `GCP_PROJECT_ID` to the project that contains the curated tables. "
            "The example in `.env.example` is not a substitute for a project you can access.\n"
            "2. Sign in with `gcloud auth application-default login` or configure "
            "`GOOGLE_APPLICATION_CREDENTIALS` with a permitted local service-account JSON path.\n"
            "3. Grant BigQuery Job User at project level and BigQuery Data Viewer on the curated dataset.\n"
            "4. Confirm `BIGQUERY_LOCATION` matches the dataset location, then restart Streamlit."
        )
        st.code("GCP_PROJECT_ID=your-real-project-id\nBIGQUERY_CURATED_DATASET=curated\nBIGQUERY_LOCATION=US", language="ini")
        st.caption("Keep service-account credentials local. Never commit or paste key contents.")


if not PROJECT_ID:
    show_connection_help("GCP_PROJECT_ID is empty in the repository-root .env file.", "Set the actual project ID before running warehouse queries.")
    st.stop()
if not PROJECT_PATTERN.fullmatch(PROJECT_ID):
    show_connection_help("GCP_PROJECT_ID is not a valid Google Cloud project ID.", "Use the lowercase project ID shown in Google Cloud Console, not the project display name.")
    st.stop()
if not DATASET_PATTERN.fullmatch(DATASET):
    show_connection_help("BIGQUERY_CURATED_DATASET is not a valid dataset name.", "Check the dataset identifier in the repository-root .env file.")
    st.stop()

try:
    client = get_bigquery_client(PROJECT_ID, LOCATION)
    dataset_info = client.get_dataset(f"{PROJECT_ID}.{DATASET}")
except Exception as exc:
    name = type(exc).__name__
    if name in {"DefaultCredentialsError", "RefreshError"}:
        reason = "Google Application Default Credentials are missing or expired."
        detail = "Sign in to the Google account that has access to this project, or point GOOGLE_APPLICATION_CREDENTIALS at the approved service account."
    elif name in {"Forbidden", "PermissionDenied"}:
        reason = "The active Google identity does not have access to this dataset or project."
        detail = f"Confirm access to `{PROJECT_ID}.{DATASET}` and BigQuery Job User permission for query jobs."
    elif name == "NotFound":
        reason = f"Dataset `{PROJECT_ID}.{DATASET}` was not found."
        detail = "Check the project ID and curated dataset name in the root .env file."
    else:
        reason = f"Google Cloud connection failed ({name})."
        detail = "Check the project, BigQuery API, credentials, dataset location, and network connection."
    show_connection_help(reason, detail)
    st.stop()

st.success(f"Connected to `{PROJECT_ID}.{DATASET}` · {dataset_info.location} · read-only dashboard")
if st.button("Refresh warehouse results", help="Clear the five-minute result cache and rerun the read-only queries"):
    query_rows.clear()
    st.rerun()

try:
    with st.spinner("Reading curated BigQuery summaries…"):
        by_band = query_rows(load_saved_query("q01"), PROJECT_ID, LOCATION)
        noisy_regions = query_rows(load_saved_query("q02"), PROJECT_ID, LOCATION)
        usage = query_rows(load_saved_query("q06"), PROJECT_ID, LOCATION)
        project_table = f"`{PROJECT_ID}.{DATASET}"
        busy_regions_sql = f"""
        WITH exposure_daily AS (
          SELECT DATE(ingested_at) AS ingestion_day, region_key, COUNT(*) AS exposure_count
          FROM {project_table}.fact_exposures`
          WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
          GROUP BY ingestion_day, region_key
        ), usage_daily AS (
          SELECT DATE(ingested_at) AS ingestion_day, region_key, COUNT(*) AS usage_event_count
          FROM {project_table}.fact_usage_events`
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
        busy_regions = query_rows(busy_regions_sql, PROJECT_ID, LOCATION)

    exposure_total = int(pd.to_numeric(by_band.get("exposure_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    usage_total = int(pd.to_numeric(usage.get("request_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    a, b, c, d = st.columns(4)
    a.metric("Archive exposures", f"{exposure_total:,}", help="Curated SPHEREx archive product records in the 60-day query window.")
    b.metric("Synthetic app actions", f"{usage_total:,}", help="Generated demo request events; not real user analytics.")
    c.metric("Sky grid cells", f"{len(busy_regions):,}", help="Approximate RA/Dec cells with archive or demo activity.")
    d.metric("Query window", "60 days", help="Window used by the saved warehouse queries.")

    st.subheader("Archive coverage")
    archive_left, archive_right = st.columns(2, gap="large")
    with archive_left:
        band_takeaway = "No exposure facts matched the 60-day period."
        if not by_band.empty and {"exposure_count", "band_id"}.issubset(by_band.columns):
            row = by_band.sort_values("exposure_count", ascending=False).iloc[0]
            band_takeaway = f"Band {row['band_id']} has the most archived exposure records ({int(row['exposure_count']):,})."
        draw_chart("Exposure records by wavelength band", by_band, "band_id", ["exposure_count"], band_takeaway)
    with archive_right:
        noise_takeaway = "No regions met the minimum sample size for this comparison. Noise values are synthetic."
        if not noisy_regions.empty and {"average_noise_sigma", "region_key"}.issubset(noisy_regions.columns):
            row = noisy_regions.sort_values("average_noise_sigma", ascending=False).iloc[0]
            noise_takeaway = f"{row['region_key']} has the highest mean synthetic noise sigma ({float(row['average_noise_sigma']):.3f}) among qualifying cells."
        draw_chart("Highest synthetic noise by sky cell", noisy_regions.head(10).sort_values("average_noise_sigma") if "average_noise_sigma" in noisy_regions else noisy_regions, "region_key", ["average_noise_sigma"], noise_takeaway, horizontal=True)

    st.subheader("Synthetic app activity")
    activity_left, activity_right = st.columns(2, gap="large")
    with activity_left:
        latency_note = "No synthetic app actions matched this query window."
        if not usage.empty and {"event_name", "p95_latency_ms"}.issubset(usage.columns):
            row = usage.sort_values("p95_latency_ms", ascending=False).iloc[0]
            latency_note = f"{row['event_name']} has the highest 95th-percentile latency ({int(row['p95_latency_ms']):,} ms) in the generated demo data."
        draw_chart("95th-percentile latency by demo action", usage, "event_name", ["p95_latency_ms"], latency_note)
    with activity_right:
        errors_note = "No synthetic app actions matched this query window."
        if not usage.empty and {"event_name", "error_count"}.issubset(usage.columns):
            row = usage.sort_values("error_count", ascending=False).iloc[0]
            errors_note = f"{row['event_name']} has the most generated 4xx/5xx responses ({int(row['error_count']):,})."
        draw_chart("Generated HTTP errors by demo action", usage, "event_name", ["error_count"], errors_note)

    st.subheader("Combined regional activity")
    region_note = "No matching activity was returned by the two curated fact tables."
    if not busy_regions.empty:
        row = busy_regions.iloc[0]
        region_note = f"Cell {row['region_key']} has the most combined activity: {int(row['exposure_count']):,} exposures and {int(row['usage_event_count']):,} synthetic actions."
    draw_chart("Top sky grid cells", busy_regions, "region_key", ["exposure_count", "usage_event_count"], region_note, horizontal=True)

    with st.expander("Show the warehouse SQL behind these charts"):
        for query_id in ("q01", "q02", "q06"):
            st.markdown(f"**{query_id}**")
            st.code(load_saved_query(query_id), language="sql")
        st.markdown("**Combined sky-cell activity**")
        st.code(busy_regions_sql, language="sql")
    st.caption("Read-only BigQuery queries · maximum 4 GiB billed per query · cached for five minutes.")
except Exception as exc:
    st.error(f"A warehouse query failed ({type(exc).__name__}).")
    st.caption(f"Check that `{PROJECT_ID}.{DATASET}` contains the curated fact and dimension tables and that your identity can run read-only query jobs.")
