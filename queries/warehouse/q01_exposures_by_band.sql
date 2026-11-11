-- Business question: Which SPHEREx bands show the most observations and noise?
SELECT
  b.band_id,
  b.wavelength_center_um,
  COUNT(*) AS exposure_count,
  COUNT(DISTINCT e.obs_id) AS distinct_observations,
  ROUND(AVG(e.noise_sigma), 4) AS average_noise_sigma
FROM `cosmic-eye-telemetry-demo.curated.fact_exposures` AS e
JOIN `cosmic-eye-telemetry-demo.curated.dim_band` AS b USING (band_id)
WHERE DATE(e.ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
GROUP BY b.band_id, b.wavelength_center_um
ORDER BY exposure_count DESC, b.band_id
LIMIT 20
