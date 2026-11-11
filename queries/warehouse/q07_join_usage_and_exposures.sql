-- Business question: Compare observation volume with app activity in each sky cell and day.
WITH exposure_daily AS (
  SELECT DATE(ingested_at) AS ingestion_day, region_key, COUNT(*) AS exposure_count
  FROM `cosmic-eye-telemetry-demo.curated.fact_exposures`
  WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  GROUP BY ingestion_day, region_key
),
usage_daily AS (
  SELECT DATE(ingested_at) AS ingestion_day, region_key, COUNT(*) AS usage_event_count
  FROM `cosmic-eye-telemetry-demo.curated.fact_usage_events`
  WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  GROUP BY ingestion_day, region_key
)
SELECT
  COALESCE(e.ingestion_day, u.ingestion_day) AS ingestion_day,
  COALESCE(e.region_key, u.region_key) AS region_key,
  COALESCE(e.exposure_count, 0) AS exposure_count,
  COALESCE(u.usage_event_count, 0) AS usage_event_count
FROM exposure_daily AS e
FULL OUTER JOIN usage_daily AS u USING (ingestion_day, region_key)
ORDER BY ingestion_day, region_key
LIMIT 50
