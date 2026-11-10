{{ config(
  materialized='table',
  partition_by={"field": "ingested_at", "data_type": "timestamp", "granularity": "day"},
  cluster_by=["region_key", "band_id", "is_synthetic"]
) }}

WITH deduplicated AS (
  SELECT *
  FROM {{ ref('clean_events') }}
  WHERE event_type = 'exposure'
    AND event_id IS NOT NULL AND event_ts IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) = 1
)
SELECT
  event_id,
  run_id,
  obs_id,
  band_id,
  region_key,
  CAST(FORMAT_DATE('%Y%m%d', DATE(event_ts)) AS INT64) AS date_key,
  event_ts,
  is_synthetic,
  metadata_is_synthetic,
  quality_metrics_synthetic,
  source,
  provenance_accessed_at AS source_accessed_at,
  calib_level,
  em_min_m,
  em_max_m,
  t_min_mjd,
  observation_time,
  access_url,
  obs_collection,
  s_ra_deg,
  s_dec_deg,
  background_level,
  noise_sigma,
  saturated_pixel_fraction,
  pointing_error_arcsec,
  processing_latency_ms,
  dirty_flags,
  ingested_at
FROM deduplicated
