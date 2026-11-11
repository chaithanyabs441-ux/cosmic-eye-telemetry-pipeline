-- Business question: How does the cumulative number of exposures grow by day?
WITH daily AS (
  SELECT DATE(ingested_at) AS ingestion_day, COUNT(*) AS daily_exposures
  FROM `cosmic-eye-telemetry-demo.curated.fact_exposures`
  WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  GROUP BY ingestion_day
)
SELECT
  ingestion_day,
  daily_exposures,
  SUM(daily_exposures) OVER (
    ORDER BY ingestion_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_exposure_total
FROM daily
ORDER BY ingestion_day
