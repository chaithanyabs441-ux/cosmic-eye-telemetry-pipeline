{{ config(materialized='table') }}

WITH bands AS (
  SELECT
    band_id,
    MIN(em_min_m * 1000000.0) AS wavelength_min_um,
    MAX(em_max_m * 1000000.0) AS wavelength_max_um,
    ANY_VALUE(source) AS source
  FROM {{ ref('clean_events') }}
  WHERE band_id IS NOT NULL AND event_type = 'exposure'
  GROUP BY band_id
)
SELECT
  band_id,
  band_id AS band_name,
  wavelength_min_um,
  wavelength_max_um,
  SAFE_DIVIDE(wavelength_min_um + wavelength_max_um, 2) AS wavelength_center_um,
  source,
  CURRENT_TIMESTAMP() AS updated_at
FROM bands
