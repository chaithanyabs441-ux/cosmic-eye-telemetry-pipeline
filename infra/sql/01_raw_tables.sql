-- Raw event tables. The project uses BigQuery's default table expiration in
-- Sandbox (60 days). Batch loads are used; do not replace them with streaming
-- inserts in this Sandbox project.

CREATE TABLE IF NOT EXISTS raw.events (
  run_id STRING NOT NULL,
  event_id STRING,
  event_type STRING,
  event_ts STRING,
  is_synthetic BOOL,
  region_key STRING,
  band_id STRING,
  source STRING,
  source_accessed_at STRING,
  dirty_flags STRING,
  raw_payload STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY event_type, region_key, band_id
OPTIONS(description='Unmodified event envelope and JSON payload from Redpanda; partitioned by ingestion date.');

CREATE TABLE IF NOT EXISTS raw.rejected_events (
  run_id STRING NOT NULL,
  event_id STRING,
  event_type STRING,
  raw_payload STRING NOT NULL,
  rejection_reason STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY rejection_reason
OPTIONS(description='Raw event envelopes rejected by basic ingestion validation.');
