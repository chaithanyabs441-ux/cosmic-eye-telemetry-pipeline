"""Live view of Spark Structured Streaming metrics and alerts in MongoDB."""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="Cosmic Eye Live", page_icon="🌌", layout="wide")
st.title("Cosmic Eye — live pipeline")
st.caption("Live Redpanda → Spark Structured Streaming → MongoDB summaries and alerts")


@st.cache_resource
def get_mongo() -> MongoClient:
    uri = os.getenv("MONGODB_URI", "").strip()
    if not uri:
        raise RuntimeError("MONGODB_URI is missing from the local .env file")
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


@st.fragment(run_every="5s")
def live_panel() -> None:
    database_name = os.getenv("MONGODB_DATABASE", "cosmic_eye").strip()
    try:
        client = get_mongo()
        client.admin.command("ping")
        db = client[database_name]
        alert_collection = db["live_alerts"]
        metric_collection = db["live_metrics"]

        alert_count = alert_collection.count_documents({})
        a, b, c = st.columns(3)
        a.metric("Live alerts stored", alert_count)
        c.metric("Aggregate windows stored", metric_collection.count_documents({}))
        b.success("MongoDB connected")

        st.subheader("Recent windowed metrics")
        metrics = list(
            metric_collection.find({}, {"_id": 0})
            .sort([("window_start", DESCENDING), ("updated_at", DESCENDING)])
            .limit(50)
        )
        if metrics:
            st.dataframe(metrics, use_container_width=True, hide_index=True)
        else:
            st.info("No live metric windows yet. Start pipeline/streaming.py, then publish events.")

        st.subheader("Recent alerts")
        alerts = list(
            alert_collection.find({}, {"_id": 0})
            .sort("created_at", DESCENDING)
            .limit(20)
        )
        if alerts:
            st.dataframe(alerts, use_container_width=True, hide_index=True)
        else:
            st.info("No alerts yet. The demo generator injects high-noise exposure events.")
    except Exception as exc:
        st.error(f"Cannot read live MongoDB results ({type(exc).__name__}). Check the local .env and Atlas access list.")


live_panel()
