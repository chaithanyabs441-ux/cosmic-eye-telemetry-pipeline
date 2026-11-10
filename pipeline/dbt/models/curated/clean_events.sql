{{ config(materialized='ephemeral') }}

SELECT
  e.*,
  CASE
    WHEN SAFE_CAST(raw_background_level AS FLOAT64) >= 0
      THEN SAFE_CAST(raw_background_level AS FLOAT64)
  END AS background_level,
  CASE
    WHEN SAFE_CAST(raw_noise_sigma AS FLOAT64) >= 0
      THEN SAFE_CAST(raw_noise_sigma AS FLOAT64)
  END AS noise_sigma,
  CASE
    WHEN SAFE_CAST(raw_saturated_pixel_fraction AS FLOAT64) BETWEEN 0 AND 1
      THEN SAFE_CAST(raw_saturated_pixel_fraction AS FLOAT64)
  END AS saturated_pixel_fraction,
  CASE
    WHEN SAFE_CAST(raw_pointing_error_arcsec AS FLOAT64) >= 0
      THEN SAFE_CAST(raw_pointing_error_arcsec AS FLOAT64)
  END AS pointing_error_arcsec,
  CASE
    WHEN SAFE_CAST(raw_processing_latency_ms AS INT64) BETWEEN 0 AND 600000
      THEN SAFE_CAST(raw_processing_latency_ms AS INT64)
  END AS processing_latency_ms,
  CASE WHEN raw_latency_ms BETWEEN 0 AND 600000 THEN raw_latency_ms END AS latency_ms,
  CASE WHEN raw_http_status BETWEEN 100 AND 599 THEN raw_http_status END AS http_status,
  NULLIF(TRIM(target), '') AS clean_target,
  SAFE_CAST(JSON_VALUE(raw_payload, '$.provenance.source_accessed_at') AS TIMESTAMP) AS provenance_accessed_at
FROM {{ ref('stg_events') }} AS e
WHERE NOT EXISTS (
  SELECT 1
  FROM {{ ref('preprocessing_rejections') }} AS r
  WHERE r.raw_payload = e.raw_payload
)
