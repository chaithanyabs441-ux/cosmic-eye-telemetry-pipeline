-- Business question: Which synthetic app actions have the highest latency/error rate?
SELECT
  event_name,
  COUNT(*) AS request_count,
  ROUND(AVG(latency_ms), 2) AS average_latency_ms,
  APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
  COUNTIF(http_status >= 400) AS error_count
FROM `cosmic-eye-telemetry-demo.curated.fact_usage_events`
WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
GROUP BY event_name
HAVING COUNT(*) >= 1
ORDER BY average_latency_ms DESC, request_count DESC
LIMIT 20
