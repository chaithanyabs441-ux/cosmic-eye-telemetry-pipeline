{{ config(
  materialized='table',
  partition_by={"field": "ingested_at", "data_type": "timestamp", "granularity": "day"},
  cluster_by=["region_key", "band_id", "event_name"]
) }}

WITH deduplicated AS (
  SELECT *
  FROM {{ ref('clean_events') }}
  WHERE event_type = 'usage'
    AND event_id IS NOT NULL AND event_ts IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) = 1
)
SELECT
  event_id,
  run_id,
  session_id,
  event_name,
  endpoint,
  clean_target AS target,
  band_id,
  region_key,
  CAST(FORMAT_DATE('%Y%m%d', DATE(event_ts)) AS INT64) AS date_key,
  event_ts,
  latency_ms,
  http_status,
  is_synthetic,
  source,
  ingested_at
FROM deduplicated
