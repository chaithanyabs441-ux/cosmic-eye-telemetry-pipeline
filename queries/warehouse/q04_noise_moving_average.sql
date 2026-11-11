-- Business question: How does daily average exposure noise compare with its 7-row moving average?
WITH daily_band_noise AS (
  SELECT
    DATE(ingested_at) AS ingestion_day,
    band_id,
    AVG(noise_sigma) AS daily_average_noise_sigma
  FROM `cosmic-eye-telemetry-demo.curated.fact_exposures`
  WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  GROUP BY ingestion_day, band_id
)
SELECT
  ingestion_day,
  band_id,
  ROUND(daily_average_noise_sigma, 4) AS daily_average_noise_sigma,
  ROUND(AVG(daily_average_noise_sigma) OVER (
    PARTITION BY band_id ORDER BY ingestion_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 4) AS moving_average_noise_sigma_7_rows
FROM daily_band_noise
ORDER BY band_id, ingestion_day
LIMIT 50
