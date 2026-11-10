{{ config(materialized='table') }}

SELECT
  run_id,
  event_id,
  event_type,
  raw_payload,
  CASE
    WHEN event_id IS NULL OR TRIM(event_id) = '' THEN 'missing_event_id'
    WHEN event_ts IS NULL THEN 'invalid_event_timestamp'
    WHEN event_type = 'exposure'
      AND raw_noise_sigma IS NOT NULL
      AND SAFE_CAST(raw_noise_sigma AS FLOAT64) IS NULL THEN 'noise_sigma_wrong_type'
    WHEN event_type = 'exposure'
      AND raw_processing_latency_ms IS NOT NULL
      AND SAFE_CAST(raw_processing_latency_ms AS INT64) IS NULL THEN 'processing_latency_wrong_type'
    WHEN event_type = 'usage'
      AND JSON_VALUE(raw_payload, '$.latency_ms') IS NOT NULL
      AND SAFE_CAST(JSON_VALUE(raw_payload, '$.latency_ms') AS INT64) IS NULL THEN 'latency_wrong_type'
  END AS rejection_reason,
  ingested_at
FROM {{ ref('stg_events') }}
WHERE event_id IS NULL OR TRIM(event_id) = '' OR event_ts IS NULL
   OR (event_type = 'exposure' AND raw_noise_sigma IS NOT NULL
       AND SAFE_CAST(raw_noise_sigma AS FLOAT64) IS NULL)
   OR (event_type = 'exposure' AND raw_processing_latency_ms IS NOT NULL
       AND SAFE_CAST(raw_processing_latency_ms AS INT64) IS NULL)
   OR (event_type = 'usage' AND JSON_VALUE(raw_payload, '$.latency_ms') IS NOT NULL
       AND SAFE_CAST(JSON_VALUE(raw_payload, '$.latency_ms') AS INT64) IS NULL)
