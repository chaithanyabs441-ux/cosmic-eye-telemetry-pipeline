"""Live charts for synthetic Spark streaming summaries and alerts."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient

ROOT = Path(__file__).resolve().parents[1]
LOCAL_TZ = "Asia/Kolkata"
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="Cosmic Eye Live", page_icon="🌌", layout="wide")
st.title("Cosmic Eye — live pipeline")
st.caption("Synthetic events → Redpanda → Spark streaming → MongoDB → this dashboard")
st.info(
    "This page visualizes the streaming demo. Its exposure quality values and app events are synthetic; "
    "it is not a live NASA telescope feed."
)

TIME_RANGES: dict[str, int | None] = {
    "5 minutes": 5,
    "15 minutes": 15,
    "30 minutes": 30,
    "60 minutes": 60,
    "All available": None,
}


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
            f"Showing {range_label.lower()} of saved stream results · "
            f"last database read {pd.Timestamp(snapshot['read_at']).tz_convert(LOCAL_TZ).strftime('%H:%M:%S')} IST"
        )
        kpis = st.columns(5)
        kpis[0].metric("Alerts stored", f"{snapshot['alert_total']:,}")
        kpis[1].metric("Window summaries stored", f"{snapshot['window_total']:,}")
        kpis[2].metric("Events in last 5 min", f"{events_last_five:,}")
        kpis[3].metric("Average latency · last 5 min", latency_label)
        kpis[4].metric("MongoDB", "Connected")

        st.subheader("What the stream is measuring")
        if metrics.empty:
            st.info("No metric windows are available in this time range. Start the streaming demo to publish synthetic events.")
        else:
            st.markdown("#### 1. Events per minute by SPHEREx band")
            count_rows = metrics[metrics["metric_name"] == "events_per_region_band_1m"].copy()
            if not count_rows.empty and {"window_start", "band_id", "event_count"}.issubset(count_rows.columns):
                chart_data = (
                    count_rows.dropna(subset=["window_start", "band_id"])
                    .groupby(["window_start", "band_id"], as_index=False)["event_count"]
                    .sum()
                    .sort_values("window_start")
                )
                st.line_chart(
                    chart_data,
                    x="window_start",
                    y="event_count",
                    color="band_id",
                    x_label="Window start · India time",
                    y_label="Events",
                )
                st.caption("Each line is a band; counts are combined across sky regions in one-minute windows.")
            else:
                st.info("No event-count windows are available in this time range.")

            st.markdown("#### 2. Average and p95 request latency")
            latency_rows = metrics[metrics["metric_name"] == "usage_latency_errors_5m"].copy()
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
                st.line_chart(
                    latency_chart.sort_values("window_start"),
                    x="window_start",
                    y=latency_series,
                    x_label="Window start · India time",
                    y_label="Milliseconds",
                )
                if "max_region_p95_ms" in latency_chart:
                    st.caption("Average is weighted by request count across regions; p95 line is the maximum regional p95.")
                else:
                    st.caption("Average is weighted by request count across regions; p95 was not present in this data.")
            else:
                st.info("No usage latency windows are available in this time range.")

            st.markdown("#### 3. HTTP error rate")
            error_rows = metrics[metrics["metric_name"] == "usage_latency_errors_5m"].copy()
            if not error_rows.empty and {"window_start", "request_count", "error_count"}.issubset(error_rows.columns):
                error_rows = error_rows.dropna(subset=["window_start", "request_count", "error_count"])
                error_chart = error_rows.groupby("window_start", as_index=False).agg(
                    errors=("error_count", "sum"), requests=("request_count", "sum")
                )
                error_chart["error_rate_percent"] = 100 * error_chart["errors"] / error_chart["requests"].clip(lower=1)
                st.line_chart(
                    error_chart.sort_values("window_start"),
                    x="window_start",
                    y="error_rate_percent",
                    x_label="Window start · India time",
                    y_label="HTTP errors (%)",
                )
                st.caption("An error is a synthetic app event with an HTTP status of 400 or higher.")
            else:
                st.info("This metric has no HTTP error counts to chart.")

            left, right = st.columns(2)
            with left:
                st.markdown("#### 4. Alerts by reason")
                if not alerts.empty and "alert_reason" in alerts:
                    reason_counts = alerts["alert_reason"].fillna("unknown").value_counts().rename_axis("alert_reason").reset_index(name="alerts")
                    st.bar_chart(reason_counts, x="alert_reason", y="alerts", horizontal=True)
                else:
                    st.info("No alerts in this time range.")
            with right:
                st.markdown("#### 5. Alerts over time")
                if not alerts.empty and "created_at" in alerts:
                    alert_times = alerts.dropna(subset=["created_at"]).copy()
                    if not alert_times.empty:
                        alert_times["minute"] = alert_times["created_at"].dt.floor("min")
                        per_minute = alert_times.groupby("minute").size().rename("alerts").reset_index()
                        st.line_chart(per_minute.sort_values("minute"), x="minute", y="alerts", x_label="Minute · India time")
                    else:
                        st.info("Alerts have no timestamps to chart.")
                else:
                    st.info("No alert timestamps are available in this time range.")

            st.markdown("#### 6. Busiest sky regions")
            if not count_rows.empty and {"region_key", "event_count"}.issubset(count_rows.columns):
                busy = (
                    count_rows.groupby("region_key", as_index=False)["event_count"]
                    .sum()
                    .nlargest(10, "event_count")
                    .sort_values("event_count")
                )
                st.bar_chart(busy, x="event_count", y="region_key", horizontal=True, x_label="Events in selected range")
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
        st.error(f"Cannot read live MongoDB results ({type(exc).__name__}). Check the local .env and Atlas access list.")


with st.sidebar:
    st.header("Dashboard controls")
    selected_range = st.selectbox("Show recent data", list(TIME_RANGES), index=1)
    refresh_enabled = st.toggle("Auto-refresh every 5 seconds", value=True)

live_panel(selected_range, refresh_enabled)
