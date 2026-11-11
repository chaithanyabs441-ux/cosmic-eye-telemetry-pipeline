-- Business question: Which sky regions rank highest by exposure count each day?
WITH daily_region AS (
  SELECT DATE(ingested_at) AS ingestion_day, region_key, COUNT(*) AS exposure_count
  FROM `cosmic-eye-telemetry-demo.curated.fact_exposures`
  WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  GROUP BY ingestion_day, region_key
)
SELECT
  ingestion_day,
  region_key,
  exposure_count,
  RANK() OVER (PARTITION BY ingestion_day ORDER BY exposure_count DESC) AS region_rank
FROM daily_region
QUALIFY region_rank <= 5
ORDER BY ingestion_day, region_rank, region_key
