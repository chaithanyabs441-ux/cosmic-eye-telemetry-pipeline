-- Business question: Which app requests were slower than the overall average?
SELECT
  event_ts,
  region_key,
  event_name,
  latency_ms,
  http_status
FROM `cosmic-eye-telemetry-demo.curated.fact_usage_events`
WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  AND latency_ms > (
    SELECT AVG(latency_ms)
    FROM `cosmic-eye-telemetry-demo.curated.fact_usage_events`
    WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
      AND latency_ms IS NOT NULL
  )
ORDER BY latency_ms DESC
LIMIT 20
