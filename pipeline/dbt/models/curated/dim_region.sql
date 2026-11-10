{{ config(materialized='table') }}

WITH regions AS (
  SELECT DISTINCT region_key
  FROM {{ ref('clean_events') }}
  WHERE region_key IS NOT NULL
)
SELECT
  region_key,
  SAFE_CAST(REGEXP_EXTRACT(region_key, r'^sky_(\d+)_') AS INT64) AS ra_cell,
  SAFE_CAST(REGEXP_EXTRACT(region_key, r'^sky_\d+_(\d+)$') AS INT64) AS dec_cell,
  SAFE_CAST(REGEXP_EXTRACT(region_key, r'^sky_(\d+)_') AS INT64) * 10.0 AS ra_min_deg,
  LEAST((SAFE_CAST(REGEXP_EXTRACT(region_key, r'^sky_(\d+)_') AS INT64) + 1) * 10.0, 360.0) AS ra_max_deg,
  SAFE_CAST(REGEXP_EXTRACT(region_key, r'^sky_\d+_(\d+)$') AS INT64) * 10.0 - 90.0 AS dec_min_deg,
  LEAST(SAFE_CAST(REGEXP_EXTRACT(region_key, r'^sky_\d+_(\d+)$') AS INT64) * 10.0 - 80.0, 90.0) AS dec_max_deg,
  10.0 AS grid_size_deg,
  CURRENT_TIMESTAMP() AS updated_at
FROM regions
