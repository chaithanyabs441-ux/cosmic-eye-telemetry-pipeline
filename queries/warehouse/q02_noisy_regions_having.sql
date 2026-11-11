-- Business question: Which regions have enough exposures to compare noise reliably?
SELECT
  region_key,
  COUNT(*) AS exposure_count,
  ROUND(AVG(noise_sigma), 4) AS average_noise_sigma,
  ROUND(MAX(saturated_pixel_fraction), 4) AS maximum_saturation_fraction
FROM `cosmic-eye-telemetry-demo.curated.fact_exposures`
WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
GROUP BY region_key
HAVING COUNT(*) >= 2 AND AVG(noise_sigma) IS NOT NULL
ORDER BY average_noise_sigma DESC, exposure_count DESC
LIMIT 20
