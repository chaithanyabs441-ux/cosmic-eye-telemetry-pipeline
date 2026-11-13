"""Interactive map and field guide for the Cosmic Eye telemetry pipelines."""

from __future__ import annotations

import html
import json
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="Data Flow · Cosmic Eye", page_icon="🧭", layout="wide")
st.markdown(
    """
    <style>
    .block-container {max-width: 1480px; padding: 2rem 2.4rem 3rem;}
    [data-testid="stAppViewContainer"] {background: radial-gradient(ellipse at 82% 0%, #142544 0%, #0b1220 48%, #080d16 100%);}
    [data-testid="stSidebar"] {background: rgba(11, 18, 32, .96); border-right: 1px solid #22334d;}
    h1 {letter-spacing: -.035em; font-size: 2.45rem !important;}
    div[data-testid="stVerticalBlockBorderWrapper"] {background: rgba(17, 29, 48, .72); border: 1px solid #263a58; border-radius: 14px;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Data Flow")
st.caption("Explore how data moves through Cosmic Eye. Select a pipeline, then click any stage for its inputs, processing, and output.")

path_choice = st.radio(
    "Choose a pipeline",
    ["Live Stream", "Batch Warehouse"],
    horizontal=True,
    label_visibility="collapsed",
)

live_stages = [
    {
        "title": "Live producer",
        "subtitle": "Synthetic events",
        "role": "Creates a continuous demo feed. Every record is generated; it is not a live telescope or real user feed.",
        "captures": "Common: event_id, event_type, event_ts, region_key, band_id, is_synthetic. Exposure: noise_sigma and saturated_pixel_fraction. Usage: event_name, latency_ms, http_status.",
        "output": "JSON messages with unique IDs and event timestamps.",
    },
    {
        "title": "Redpanda",
        "subtitle": "cosmic-eye-live",
        "role": "Buffers live messages so Spark can consume them continuously.",
        "captures": "The topic carries the producer's JSON messages. It is configured as a bounded transport buffer with one-day retention.",
        "output": "Kafka-compatible topic records; this is not the long-term event store.",
    },
    {
        "title": "Spark streaming",
        "subtitle": "Three live queries",
        "role": "Parses the fields needed for dashboard metrics, applies a two-minute event-time watermark, then aggregates or filters alerts.",
        "captures": "Parsed fields: event_id, event_type, event_ts, region_key, band_id, event_name, latency_ms, http_status, is_synthetic, quality_metrics.noise_sigma, quality_metrics.saturated_pixel_fraction. Other payload fields are discarded by this live schema.",
        "output": "One-minute event counts by region and band; five-minute usage latency, p95, and HTTP-error windows; threshold alerts.",
    },
    {
        "title": "MongoDB",
        "subtitle": "cosmic_eye database",
        "role": "Stores compact live summaries and triggered alerts for quick dashboard reads.",
        "captures": "live_metrics: window times, metric name, region, band, event/request counts, average and p95 latency, error count, updated_at. live_alerts: event ID/time, type, region/band, relevant quality or request fields, alert reason, created_at.",
        "output": "Derived operational records, not a copy of the full raw event history.",
    },
    {
        "title": "Command Center",
        "subtitle": "Streamlit · port 8501",
        "role": "Reads MongoDB summaries and alerts, refreshes on the configured interval, and renders pipeline status and charts.",
        "captures": "The dashboard displays counts, latency, error rate, alert reasons and volume, and busiest sky cells. Time-range filters apply to saved records.",
        "output": "An operational view of the synthetic live demo. The Warehouse Insights page reads BigQuery separately.",
    },
]

batch_stages = [
    {
        "title": "IRSA + generator",
        "subtitle": "Archive + demo inputs",
        "role": "The batch source can combine public SPHEREx archive metadata from NASA/IPAC IRSA SIA2 with generated quality values and synthetic app-usage examples.",
        "captures": "Archive fields: obs_id, wavelength bounds, RA/Dec, MJD/observation time, calibration level, collection, access URL. Generated fields include quality metrics, usage actions, event IDs/times, and provenance flags.",
        "output": "Exposure and usage event payloads. Provenance distinguishes archive metadata from generated values.",
    },
    {
        "title": "Python ingest",
        "subtitle": "Validate + batch",
        "role": "Builds events, checks the message envelope, records intentional dirty flags for ETL demonstrations, and batches load payloads in memory.",
        "captures": "run_id, event_id, event_type, event_ts, synthetic flag, region_key, band_id, source, source access time, dirty flags, and the original serialized payload.",
        "output": "Accepted and rejected records sent through a temporary Redpanda topic to BigQuery load jobs.",
    },
    {
        "title": "Redpanda",
        "subtitle": "Temporary transport",
        "role": "Carries batch records between the Python loader and the BigQuery load process.",
        "captures": "Messages are used as a transport buffer for the ingestion run; the temporary per-run topic is deleted afterward.",
        "output": "Validated event messages for the raw warehouse, plus messages that fail basic envelope checks for the reject table.",
    },
    {
        "title": "BigQuery raw",
        "subtitle": "Events + rejects",
        "role": "Preserves the accepted event envelope and original JSON before typed cleaning.",
        "captures": "raw.events: run_id, event_id, event_type, event_ts, is_synthetic, region_key, band_id, source, source_accessed_at, dirty_flags, raw_payload, ingested_at. raw.rejected_events also records raw payload and rejection_reason.",
        "output": "Raw event history for scheduled or local ETL. Invalid typed values are handled in later preprocessing.",
    },
    {
        "title": "dbt + scheduled ETL",
        "subtitle": "Clean + model",
        "role": "Casts types, normalizes and validates fields, deduplicates by event ID, and derives shared dimensions.",
        "captures": "Malformed envelopes remain in raw.rejected_events. Valid envelopes with invalid typed fields are recorded in curated.preprocessing_rejections with the original payload and reason.",
        "output": "Typed exposure and usage facts plus band, sky-cell, and calendar dimensions.",
    },
    {
        "title": "Curated warehouse",
        "subtitle": "BigQuery star schema",
        "role": "Stores cleaned facts and dimensions for warehouse analytics. This is the primary batch event store.",
        "captures": "fact_exposures: archive IDs/times, band/region/date keys, provenance, archive metadata, and explicitly synthetic quality measurements. fact_usage_events: anonymous session, action/endpoint/target, band/region/date, latency/status, synthetic flag. Dimensions describe bands, sky cells, and dates.",
        "output": "Warehouse Insights queries BigQuery. An optional sync writes compact daily aggregates—not full events—to MongoDB.",
    },
]

stages = live_stages if path_choice == "Live Stream" else batch_stages
accent = "#70aaff" if path_choice == "Live Stream" else "#42d6c2"
stage_payload = json.dumps(
    [{key: stage[key] for key in ("title", "role", "captures", "output")} for stage in stages],
    ensure_ascii=True,
).replace("</", "<\\/")
cards = "".join(
    f'<button class="stage" type="button" data-stage="{i}" aria-pressed="false">'
    f'<span class="step">STAGE {i + 1:02}</span>'
    f'<strong>{html.escape(stage["title"])}</strong>'
    f'<span class="subtitle">{html.escape(stage["subtitle"])}</span>'
    "</button>"
    for i, stage in enumerate(stages)
)
diagram = f"""
<!doctype html><html><head><meta charset="utf-8"><style>
* {{ box-sizing:border-box; }}
body {{ margin:0; color:#e7eefb; background:transparent; font:14px/1.45 Arial,sans-serif; }}
.shell {{ padding:18px; border:1px solid #263a58; border-radius:16px; background:linear-gradient(145deg,rgba(18,32,53,.96),rgba(11,20,35,.96)); }}
.hint {{ margin:0 0 15px; color:#9eb3d0; font-size:13px; }}
.flow {{ display:grid; grid-template-columns:repeat({len(stages)},minmax(0,1fr)); gap:24px; align-items:stretch; }}
.stage {{ position:relative; display:flex; flex-direction:column; align-items:flex-start; min-width:0; min-height:108px; padding:13px; text-align:left; cursor:pointer; color:#e7eefb; background:#111e30; border:1px solid #354b68; border-radius:12px; transition:transform .15s ease,border-color .15s ease,background .15s ease; }}
.stage:not(:last-child)::after {{ content:'→'; position:absolute; z-index:2; top:39px; right:-22px; width:20px; color:{accent}; font-size:21px; text-align:center; }}
.stage:hover {{ transform:translateY(-2px); border-color:{accent}; }}
.stage.active {{ border-color:{accent}; background:rgba(39,75,119,.45); box-shadow:0 0 0 1px {accent} inset,0 7px 24px rgba(0,0,0,.18); }}
.step {{ margin-bottom:10px; color:{accent}; font-size:10px; font-weight:700; letter-spacing:.12em; }}
.stage strong {{ font-size:14px; line-height:1.25; }}
.subtitle {{ margin-top:5px; color:#9eb3d0; font-size:11px; }}
.detail {{ margin-top:16px; padding:16px 18px; border:1px solid #293e5b; border-radius:12px; background:rgba(8,15,27,.7); }}
.detail-head {{ display:flex; align-items:center; gap:10px; margin-bottom:10px; }}
.detail-kicker {{ color:{accent}; font-size:10px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; }}
.detail h2 {{ margin:0; font-size:18px; }}
.detail-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:16px; }}
.detail-grid div {{ color:#b7c6da; font-size:12px; }}
.detail-grid b {{ display:block; margin-bottom:4px; color:#e5edf8; font-size:11px; letter-spacing:.04em; text-transform:uppercase; }}
@media(max-width:780px) {{ .flow {{ grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }} .stage::after {{ display:none; }} .detail-grid {{ grid-template-columns:1fr; gap:10px; }} }}
</style></head><body>
<div class="shell"><p class="hint">Click a stage to inspect what it receives, what it does, and what it writes.</p>
<div class="flow">{cards}</div>
<section class="detail" aria-live="polite"><div class="detail-head"><span class="detail-kicker">Selected stage</span><h2 id="detail-title"></h2></div>
<div class="detail-grid"><div><b>Role</b><span id="detail-role"></span></div><div><b>Fields & inputs</b><span id="detail-captures"></span></div><div><b>Output</b><span id="detail-output"></span></div></div></section></div>
<script>
const stages={stage_payload};
const buttons=[...document.querySelectorAll('.stage')];
function selectStage(index) {{
  const stage=stages[index];
  buttons.forEach((button,i)=>{{button.classList.toggle('active',i===index);button.setAttribute('aria-pressed',i===index?'true':'false');}});
  document.getElementById('detail-title').textContent=stage.title;
  document.getElementById('detail-role').textContent=stage.role;
  document.getElementById('detail-captures').textContent=stage.captures;
  document.getElementById('detail-output').textContent=stage.output;
}}
buttons.forEach((button,index)=>button.addEventListener('click',()=>selectStage(index)));
selectStage(0);
</script></body></html>
"""
st.iframe(diagram, height=410, tab_index=0, alt=f"Interactive {path_choice.lower()} flowchart")

st.subheader("Field catalog")
st.caption("Open a category for the complete table or message fields. The flow above stays focused on the selected path.")
catalog_tabs = st.tabs(["Live event fields", "MongoDB outputs", "BigQuery warehouse", "Retention & provenance"])

with catalog_tabs[0]:
    exposure, usage = st.columns(2, gap="large")
    with exposure.container(border=True):
        st.markdown("#### Exposure event")
        st.markdown("`event_id`, `event_type`, `event_ts`, `obs_id`, `source_obs_id`, `band_id`, `region_key`, `is_synthetic`, `metadata_is_synthetic`, `quality_metrics_synthetic`.")
        st.markdown("`observation`: `em_min`, `em_max`, `s_ra`, `s_dec`, `t_min_mjd`, `observation_time`, `calib_level`, `access_url`, `obs_collection`.")
        st.markdown("`quality_metrics`: `background_level`, `noise_sigma`, `saturated_pixel_fraction`, `pointing_error_arcsec`, `processing_latency_ms`.")
        st.markdown("`provenance`: `source`, `source_accessed_at`, `synthetic_fields`, `generator_seed`; optional `dirty_flags` in test data.")
    with usage.container(border=True):
        st.markdown("#### App-usage event")
        st.markdown("`event_id`, `event_type`, `event_ts`, `session_id` (anonymous and synthetic), `event_name`, `endpoint`, `target`, `band_id`, `region_key`, `latency_ms`, `http_status`, `is_synthetic`.")
        st.markdown("`provenance`: `source`, `source_accessed_at`, `synthetic_fields`, `generator_seed`; optional `dirty_flags` in test data.")

with catalog_tabs[1]:
    st.markdown("**`live_metrics` · event-count windows**")
    st.code("metric_name · window_start · window_end · region_key · band_id · event_count · updated_at", language=None)
    st.markdown("**`live_metrics` · usage windows**")
    st.code("metric_name · window_start · window_end · region_key · request_count · average_latency_ms · p95_latency_ms · error_count · updated_at", language=None)
    st.markdown("**`live_alerts`**")
    st.code("event_id · event_at · event_type · region_key · band_id · event_name · noise_sigma · saturated_pixel_fraction · latency_ms · http_status · alert_reason · created_at", language=None)
    st.caption("Optional batch sync writes `exposure_daily_summaries` and `usage_daily_summaries` from BigQuery aggregates; MongoDB does not receive the full warehouse event history.")

with catalog_tabs[2]:
    st.markdown("**`raw.events`**")
    st.code("run_id · event_id · event_type · event_ts · is_synthetic · region_key · band_id · source · source_accessed_at · dirty_flags · raw_payload · ingested_at", language=None)
    st.markdown("**`raw.rejected_events`**")
    st.code("run_id · event_id · event_type · raw_payload · rejection_reason · ingested_at", language=None)
    st.markdown("**`curated.dim_band` · `dim_region` · `dim_date`**")
    st.write("Band wavelength/name/source fields; sky-cell key, RA/Dec indexes and bounds; date key, date, year/quarter/month/week/day fields.")
    st.markdown("**`curated.fact_exposures`**")
    st.code("event_id · run_id · obs_id · band_id · region_key · date_key · event_ts · synthetic/provenance flags · source/access time · calib_level · em_min_m · em_max_m · t_min_mjd · observation_time · access_url · obs_collection · s_ra_deg · s_dec_deg · background_level · noise_sigma · saturated_pixel_fraction · pointing_error_arcsec · processing_latency_ms · dirty_flags · ingested_at", language=None)
    st.markdown("**`curated.fact_usage_events`**")
    st.code("event_id · run_id · session_id · event_name · endpoint · target · band_id · region_key · date_key · event_ts · latency_ms · http_status · is_synthetic · source · ingested_at", language=None)
    st.markdown("**`curated.preprocessing_rejections`**")
    st.write("Original payload and identifiers, event type, run ID, ingestion time, and typed-field rejection reason. See `docs/DATA_DICTIONARY.md` for full definitions.")

with catalog_tabs[3]:
    st.markdown("- Redpanda is a bounded transport buffer. The live topic is configured for one-day retention; batch topics are temporary per run.")
    st.markdown("- Spark checkpoints keep offsets and streaming state, not a historical copy of the event payloads.")
    st.markdown("- BigQuery raw and curated tables are the batch event store, subject to the configured Sandbox 60-day partition retention.")
    st.markdown("- MongoDB stores live aggregates/alerts and optional compact batch summaries.")
    st.markdown("- IRSA SIA2 supplies public SPHEREx archive metadata. Quality measurements, app usage, and live demo traffic are synthetic; provenance fields distinguish those sources.")

st.caption("Implementation references: `docs/PHASE2_INGESTION.md` · `docs/PHASE3_WAREHOUSE.md` · `docs/PHASE6_STREAMING.md` · `docs/DATA_DICTIONARY.md`.")
