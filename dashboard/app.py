"""Live charts for synthetic Spark streaming summaries and alerts."""

from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import altair as alt
import streamlit as st
from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient

ROOT = Path(__file__).resolve().parents[1]
LOCAL_TZ = "Asia/Kolkata"
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="Cosmic Eye Command Center", page_icon="🌌", layout="wide")
st.markdown(
    """
    <style>
    .block-container {max-width: 1540px; padding: 2rem 2.5rem 3.5rem;}
    [data-testid="stAppViewContainer"] {background: radial-gradient(ellipse at 80% 0%, #142544 0%, #0b1220 48%, #080d16 100%);}
    [data-testid="stSidebar"] {background: rgba(11, 18, 32, .96); border-right: 1px solid #22334d;}
    h1 {letter-spacing: -.035em; font-size: 2.5rem !important;}
    h2, h3 {letter-spacing: -.02em;}
    div[data-testid="stVerticalBlockBorderWrapper"] {background: rgba(17, 29, 48, .72); border: 1px solid #263a58; border-radius: 14px;}
    [data-testid="stMetric"] {padding: .45rem .25rem;}
    [data-testid="stMetricLabel"] {color: #a9bdd8; font-size: .88rem;}
    [data-testid="stMetricValue"] {font-size: 1.8rem;}
    [data-testid="stAlert"] {border-radius: 12px;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Cosmic Eye Command Center")
st.caption("Live operations view · Redpanda → Spark → MongoDB")
st.info(
        "This page monitors generated pipeline events. Exposure quality values and app actions are synthetic; "
        "this is not live telescope data or personal user analytics."
)

TIME_RANGES: dict[str, int | None] = {
    "5 minutes": 5,
    "15 minutes": 15,
    "30 minutes": 30,
    "60 minutes": 60,
    "All available": None,
}


@st.cache_data(ttl=5, show_spinner=False)
def check_redpanda() -> tuple[str, str]:
    """Check the configured broker socket without publishing or changing data."""
    endpoint = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip().split(",", 1)[0]
    try:
        if endpoint.startswith("[") and "]:" in endpoint:
            host, port_text = endpoint[1:].rsplit("]:", 1)
        else:
            host, port_text = endpoint.rsplit(":", 1)
        with socket.create_connection((host, int(port_text)), timeout=1.0):
            return "Reachable", endpoint
    except (OSError, ValueError):
        return "Unavailable", endpoint


@st.cache_resource
def get_mongo() -> MongoClient:
    uri = os.getenv("MONGODB_URI", "").strip()
    if not uri:
        raise RuntimeError("MONGODB_URI is missing from the local .env file")
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def to_local_frame(documents: list[dict[str, Any]]) -> pd.DataFrame:
    """Make Mongo's naive UTC datetimes timezone-aware and display them in India time."""
    frame = pd.DataFrame(documents)
    for column in ("window_start", "window_end", "updated_at", "event_at", "created_at"):
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce").dt.tz_convert(LOCAL_TZ)
    return frame


def without_empty_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Hide fields that do not apply to any row in this table."""
    return frame.dropna(axis="columns", how="all")


def get_snapshot(db, range_minutes: int | None) -> dict[str, Any]:
    """Read only recent summaries and alerts needed for this page."""
    now_utc = datetime.now(timezone.utc)
    metric_filter: dict[str, Any] = {
        "metric_name": {"$in": ["events_per_region_band_1m", "usage_latency_errors_5m"]}
    }
    alert_filter: dict[str, Any] = {}
    if range_minutes is not None:
        cutoff = now_utc - timedelta(minutes=range_minutes)
        metric_filter["window_start"] = {"$gte": cutoff}
        alert_filter["created_at"] = {"$gte": cutoff}

    metrics = list(
        db["live_metrics"].find(metric_filter, {"_id": 0})
        .sort([("window_start", DESCENDING), ("updated_at", DESCENDING)])
        .limit(10000)
    )
    alerts = list(
        db["live_alerts"].find(alert_filter, {"_id": 0})
        .sort("created_at", DESCENDING)
        .limit(5000)
    )
    return {
        "metrics": to_local_frame(metrics),
        "alerts": to_local_frame(alerts),
        "alert_total": db["live_alerts"].count_documents({}),
        "window_total": db["live_metrics"].count_documents({}),
        "read_at": datetime.now(timezone.utc),
    }


def latest_five_minute_metrics(metrics: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return event-count and latency rows whose windows overlap the last five minutes."""
    if metrics.empty or "metric_name" not in metrics:
        return pd.DataFrame(), pd.DataFrame()
    cutoff = pd.Timestamp.now(tz=LOCAL_TZ) - pd.Timedelta(minutes=5)
    recent = metrics[metrics["window_start"].notna() & (metrics["window_start"] >= cutoff)]
    event_rows = recent[recent["metric_name"] == "events_per_region_band_1m"].copy()
    usage_rows = recent[recent["metric_name"] == "usage_latency_errors_5m"].copy()
    return event_rows, usage_rows


@st.fragment(run_every="5s")
def live_panel(range_label: str, auto_refresh: bool) -> None:
    database_name = os.getenv("MONGODB_DATABASE", "cosmic_eye").strip()
    minutes = TIME_RANGES[range_label]
    cache_key = f"cosmic_eye_live_snapshot_{range_label}"
    try:
        client = get_mongo()
        # Ping on refresh, or once when the user has paused refresh.
        snapshot = st.session_state.get(cache_key) if not auto_refresh else None
        if snapshot is None:
            client.admin.command("ping")
            snapshot = get_snapshot(client[database_name], minutes)
            st.session_state[cache_key] = snapshot

        metrics = snapshot["metrics"]
        alerts = snapshot["alerts"]
        broker_status, broker_endpoint = check_redpanda()
        latest_update = metrics["updated_at"].dropna().max() if not metrics.empty and "updated_at" in metrics else None
        if latest_update is not None and pd.notna(latest_update):
            output_age_seconds = max(0, int((pd.Timestamp.now(tz=LOCAL_TZ) - latest_update).total_seconds()))
            spark_status = "Recent output" if output_age_seconds <= 90 else "Idle"
            output_detail = f"Last summary {output_age_seconds // 60}m {output_age_seconds % 60}s ago"
        else:
            spark_status = "Waiting for output"
            output_detail = "No saved metric windows yet"
        event_rows, usage_rows = latest_five_minute_metrics(metrics)
        events_last_five = int(pd.to_numeric(event_rows.get("event_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
        latency_values = pd.DataFrame()
        if not usage_rows.empty:
            latency_values = usage_rows.dropna(subset=["request_count", "average_latency_ms"]).copy()
        request_total = int(pd.to_numeric(latency_values.get("request_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
        if request_total:
            weighted_latency = (
                pd.to_numeric(latency_values["request_count"], errors="coerce")
                * pd.to_numeric(latency_values["average_latency_ms"], errors="coerce")
            ).sum() / request_total
            latency_label = f"{weighted_latency:,.0f} ms"
        else:
            latency_label = "No recent requests"

        st.caption(
            f"Showing {range_label.lower()} of saved stream results Â· "
            f"last database read {pd.Timestamp(snapshot['read_at']).tz_convert(LOCAL_TZ).strftime('%H:%M:%S')} IST"
        )
        st.markdown("### Pipeline status")
        status_cards = st.columns(3, gap="medium")
        with status_cards[0].container(border=True):
            st.metric("REDPANDA · BROKER", broker_status)
            st.caption(broker_endpoint)
        with status_cards[1].container(border=True):
            st.metric("SPARK · STREAM OUTPUT", spark_status)
            st.caption(output_detail)
        with status_cards[2].container(border=True):
            st.metric("MONGODB · STORAGE", "Connected")
            st.caption(f"{snapshot['window_total']:,} saved window summaries · {snapshot['alert_total']:,} alerts")
        kpis = st.columns(4, gap="medium")
        with kpis[0].container(border=True):
            st.metric("Alerts stored", f"{snapshot['alert_total']:,}", help="Total alerts saved by the demo pipeline.")
        with kpis[1].container(border=True):
            st.metric("Window summaries", f"{snapshot['window_total']:,}", help="Metric windows saved in MongoDB.")
        with kpis[2].container(border=True):
            st.metric("Events · last 5 min", f"{events_last_five:,}", help="Generated events in recent one-minute windows.")
        with kpis[3].container(border=True):
            st.metric("Mean latency · last 5 min", latency_label, help="Weighted from generated app-action events; not this dashboard's response time.")

        st.subheader("Pipeline activity")
        st.caption("Charts use the selected saved-data window. Hover over a line or bar for exact values.")
        if metrics.empty:
            st.info("No metric windows are available in this time range. Start the streaming demo to publish synthetic events.")
        else:
            count_rows = metrics[metrics["metric_name"] == "events_per_region_band_1m"].copy()
            latency_rows = metrics[metrics["metric_name"] == "usage_latency_errors_5m"].copy()
            left_chart, right_chart = st.columns(2, gap="large")

            if not count_rows.empty and {"window_start", "band_id", "event_count"}.issubset(count_rows.columns):
                chart_data = (
                    count_rows.dropna(subset=["window_start", "band_id"])
                    .groupby(["window_start", "band_id"], as_index=False)["event_count"]
                    .sum()
                    .sort_values("window_start")
                )
                event_chart = (
                    alt.Chart(chart_data)
                    .mark_line(point=alt.OverlayMarkDef(size=42, filled=True), strokeWidth=2.5)
                    .encode(
                        x=alt.X("window_start:T", title="Time (IST)", axis=alt.Axis(format="%H:%M")),
                        y=alt.Y("event_count:Q", title="Events per minute", scale=alt.Scale(zero=True)),
                        color=alt.Color("band_id:N", title="SPHEREx band", scale=alt.Scale(scheme="tableau10")),
                        tooltip=[alt.Tooltip("window_start:T", title="Time", format="%d %b %H:%M"), "band_id:N", alt.Tooltip("event_count:Q", title="Events", format=",")],
                    )
                    .properties(height=285)
                    .interactive()
                )
                with left_chart.container(border=True):
                    st.markdown("#### Events by spectral band")
                    st.caption("One-minute generated event counts, combined across sky regions.")
                    st.altair_chart(event_chart, width="stretch")
            else:
                with left_chart.container(border=True):
                    st.markdown("#### Events by spectral band")
                    st.info("No event-count windows are available in this time range.")

            required_latency = {"window_start", "request_count", "average_latency_ms"}
            if not latency_rows.empty and required_latency.issubset(latency_rows.columns):
                latency_rows = latency_rows.dropna(subset=["window_start", "request_count", "average_latency_ms"])
                latency_rows["weighted_ms"] = latency_rows["request_count"] * latency_rows["average_latency_ms"]
                aggregations: dict[str, tuple[str, str]] = {
                    "weighted_ms": ("weighted_ms", "sum"),
                    "request_count": ("request_count", "sum"),
                }
                if "p95_latency_ms" in latency_rows:
                    aggregations["max_region_p95_ms"] = ("p95_latency_ms", "max")
                latency_chart = latency_rows.groupby("window_start", as_index=False).agg(**aggregations)
                latency_chart["weighted_average_ms"] = latency_chart["weighted_ms"] / latency_chart["request_count"]
                latency_series = ["weighted_average_ms"]
                if "max_region_p95_ms" in latency_chart:
                    latency_series.append("max_region_p95_ms")
                latency_long = latency_chart.melt(
                    id_vars="window_start", value_vars=latency_series, var_name="measure", value_name="latency_ms"
                )
                latency_long["measure"] = latency_long["measure"].map(
                    {"weighted_average_ms": "Weighted average", "max_region_p95_ms": "Highest regional p95"}
                )
                latency_viz = (
                    alt.Chart(latency_long)
                    .mark_line(point=alt.OverlayMarkDef(size=42, filled=True), strokeWidth=2.5)
                    .encode(
                        x=alt.X("window_start:T", title="Time (IST)", axis=alt.Axis(format="%H:%M")),
                        y=alt.Y("latency_ms:Q", title="Latency (ms)"),
                        color=alt.Color("measure:N", title="Measure", scale=alt.Scale(range=["#58a6ff", "#c084fc"])),
                        tooltip=[alt.Tooltip("window_start:T", title="Time", format="%d %b %H:%M"), "measure:N", alt.Tooltip("latency_ms:Q", title="Milliseconds", format=".0f")],
                    )
                    .properties(height=285)
                    .interactive()
                )
                with right_chart.container(border=True):
                    st.markdown("#### Request latency")
                    st.caption("Weighted average and, when present, the highest regional p95. Synthetic app activity.")
                    st.altair_chart(latency_viz, width="stretch")
            else:
                with right_chart.container(border=True):
                    st.markdown("#### Request latency")
                    st.info("No usage latency windows are available in this time range.")

            error_rows = metrics[metrics["metric_name"] == "usage_latency_errors_5m"].copy()
            if not error_rows.empty and {"window_start", "request_count", "error_count"}.issubset(error_rows.columns):
                error_rows = error_rows.dropna(subset=["window_start", "request_count", "error_count"])
                error_chart = error_rows.groupby("window_start", as_index=False).agg(
                    errors=("error_count", "sum"), requests=("request_count", "sum")
                )
                error_chart["error_rate_percent"] = 100 * error_chart["errors"] / error_chart["requests"].clip(lower=1)
                error_viz = (
                    alt.Chart(error_chart)
                    .mark_area(line={"color": "#fb7185", "strokeWidth": 2}, color="#fb7185", opacity=0.16)
                    .encode(
                        x=alt.X("window_start:T", title="Time (IST)", axis=alt.Axis(format="%H:%M")),
                        y=alt.Y("error_rate_percent:Q", title="Requests with errors (%)", scale=alt.Scale(domainMin=0)),
                        tooltip=[alt.Tooltip("window_start:T", title="Time", format="%d %b %H:%M"), alt.Tooltip("error_rate_percent:Q", title="Error rate", format=".2f"), alt.Tooltip("errors:Q", title="Errors", format=","), alt.Tooltip("requests:Q", title="Requests", format=",")],
                    )
                    .properties(height=250)
                    .interactive()
                )
            alert_left, alert_right = st.columns(2, gap="large")
            with alert_left.container(border=True):
                st.markdown("#### HTTP error rate")
                if not error_rows.empty and {"window_start", "request_count", "error_count"}.issubset(error_rows.columns):
                    st.altair_chart(error_viz, width="stretch")
                    st.caption("Share of generated app actions returning HTTP 400 or higher.")
                else:
                    st.info("No HTTP error counts are available in this time range.")

            with alert_right.container(border=True):
                st.markdown("#### Alerts by reason")
                if not alerts.empty and "alert_reason" in alerts:
                    reason_counts = alerts["alert_reason"].fillna("unknown").value_counts().rename_axis("alert_reason").reset_index(name="alerts")
                    reasons_viz = (
                        alt.Chart(reason_counts)
                        .mark_bar(cornerRadiusEnd=5, color="#a78bfa")
                        .encode(
                            y=alt.Y("alert_reason:N", sort="-x", title=None),
                            x=alt.X("alerts:Q", title="Saved alerts", scale=alt.Scale(zero=True)),
                            tooltip=["alert_reason:N", alt.Tooltip("alerts:Q", title="Alerts", format=",")],
                        )
                        .properties(height=250)
                    )
                    st.altair_chart(reasons_viz, width="stretch")
                else:
                    st.info("No alerts in this time range.")

            region_left, region_right = st.columns(2, gap="large")
            with region_left.container(border=True):
                st.markdown("#### Alerts over time")
                if not alerts.empty and "created_at" in alerts:
                    alert_times = alerts.dropna(subset=["created_at"]).copy()
                    if not alert_times.empty:
                        alert_times["minute"] = alert_times["created_at"].dt.floor("min")
                        per_minute = alert_times.groupby("minute").size().rename("alerts").reset_index()
                        alerts_viz = (
                            alt.Chart(per_minute.sort_values("minute"))
                            .mark_line(point=True, strokeWidth=2.5, color="#fbbf24")
                            .encode(
                                x=alt.X("minute:T", title="Time (IST)", axis=alt.Axis(format="%H:%M")),
                                y=alt.Y("alerts:Q", title="Alerts", scale=alt.Scale(zero=True)),
                                tooltip=[alt.Tooltip("minute:T", title="Time", format="%d %b %H:%M"), alt.Tooltip("alerts:Q", title="Alerts", format=",")],
                            )
                            .properties(height=250)
                            .interactive()
                        )
                        st.altair_chart(alerts_viz, width="stretch")
                    else:
                        st.info("Alerts have no timestamps to chart.")
                else:
                    st.info("No alert timestamps are available in this time range.")

            with region_right.container(border=True):
                st.markdown("#### Busiest sky regions")
                if not count_rows.empty and {"region_key", "event_count"}.issubset(count_rows.columns):
                    busy = (
                        count_rows.groupby("region_key", as_index=False)["event_count"]
                        .sum()
                        .nlargest(10, "event_count")
                        .sort_values("event_count")
                    )
                    regions_viz = (
                        alt.Chart(busy)
                        .mark_bar(cornerRadiusEnd=5, color="#38bdf8")
                        .encode(
                            y=alt.Y("region_key:N", sort="-x", title=None),
                            x=alt.X("event_count:Q", title="Events in selected range", scale=alt.Scale(zero=True)),
                            tooltip=["region_key:N", alt.Tooltip("event_count:Q", title="Events", format=",")],
                        )
                        .properties(height=250)
                    )
                    st.altair_chart(regions_viz, width="stretch")
                    st.caption("Approximate RA/Dec grid cells, not named astronomical regions.")
                else:
                    st.info("No region event counts are available in this time range.")

            with st.expander("Show the two streaming summary tables"):
                count_tab, usage_tab = st.tabs(["Events per region and band", "Usage latency and errors"])
                with count_tab:
                    table = without_empty_columns(count_rows.sort_values("window_start", ascending=False).head(200))
                    if table.empty:
                        st.info("No event-count rows are available.")
                    else:
                        st.dataframe(table, width="stretch", hide_index=True)
                with usage_tab:
                    table = without_empty_columns(latency_rows.sort_values("window_start", ascending=False).head(200))
                    if table.empty:
                        st.info("No usage-window rows are available.")
                    else:
                        st.dataframe(table, width="stretch", hide_index=True)

        with st.expander("Recent alerts"):
            if alerts.empty:
                st.info("No alerts in this time range. Synthetic anomalies are generated by the demo producer.")
            else:
                st.dataframe(without_empty_columns(alerts.head(200)), width="stretch", hide_index=True)
    except Exception as exc:
        st.error(f"Cannot read the live stream store ({type(exc).__name__}).")
        st.caption("Check that MONGODB_URI in the repository-root .env points to a reachable local MongoDB or Atlas cluster.")


with st.sidebar:
    st.header("Dashboard controls")
    selected_range = st.selectbox("Show recent data", list(TIME_RANGES), index=len(TIME_RANGES) - 1)
    refresh_enabled = st.toggle("Auto-refresh every 5 seconds", value=True)

live_panel(selected_range, refresh_enabled)



