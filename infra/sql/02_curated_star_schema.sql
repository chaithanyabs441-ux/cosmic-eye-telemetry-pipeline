-- Clean, typed star schema. Dimension rows and facts are populated by Phase 4
-- ETL. The SQL here creates empty tables only; it does not invent observations.

CREATE TABLE IF NOT EXISTS curated.dim_band (
  band_id STRING NOT NULL,
  band_name STRING,
  wavelength_min_um FLOAT64,
  wavelength_max_um FLOAT64,
  wavelength_center_um FLOAT64,
  source STRING,
  updated_at TIMESTAMP
)
OPTIONS(description='One row per SPHEREx wavelength band; wavelength values are micrometres.');

CREATE TABLE IF NOT EXISTS curated.dim_region (
  region_key STRING NOT NULL,
  ra_cell INT64,
  dec_cell INT64,
  ra_min_deg FLOAT64,
  ra_max_deg FLOAT64,
  dec_min_deg FLOAT64,
  dec_max_deg FLOAT64,
  grid_size_deg FLOAT64,
  updated_at TIMESTAMP
)
OPTIONS(description='Ten-degree sky grid cells shared by exposure and usage facts.');

CREATE TABLE IF NOT EXISTS curated.dim_date (
  date_key INT64 NOT NULL,
  full_date DATE NOT NULL,
  calendar_year INT64,
  quarter INT64,
  month INT64,
  week_of_year INT64,
  day_of_month INT64,
  day_of_week INT64
)
OPTIONS(description='Calendar date dimension; date_key uses YYYYMMDD.');

CREATE TABLE IF NOT EXISTS curated.fact_exposures (
  event_id STRING NOT NULL,
  run_id STRING,
  obs_id STRING,
  band_id STRING,
  region_key STRING,
  date_key INT64,
  event_ts TIMESTAMP,
  is_synthetic BOOL,
  metadata_is_synthetic BOOL,
  quality_metrics_synthetic BOOL,
  source STRING,
  source_accessed_at TIMESTAMP,
  calib_level STRING,
  em_min_m FLOAT64,
  em_max_m FLOAT64,
  t_min_mjd FLOAT64,
  observation_time TIMESTAMP,
  access_url STRING,
  obs_collection STRING,
  s_ra_deg FLOAT64,
  s_dec_deg FLOAT64,
  background_level FLOAT64,
  noise_sigma FLOAT64,
  saturated_pixel_fraction FLOAT64,
  pointing_error_arcsec FLOAT64,
  processing_latency_ms INT64,
  dirty_flags ARRAY<STRING>,
  ingested_at TIMESTAMP
)
-- Sandbox applies a 60-day partition expiry. Partition by ingestion time so
-- backfilled observations with old event_ts values are not immediately expired.
PARTITION BY DATE(ingested_at)
CLUSTER BY region_key, band_id, is_synthetic
OPTIONS(description='One cleaned SPHEREx exposure event; quality metrics remain explicitly synthetic.');

CREATE TABLE IF NOT EXISTS curated.fact_usage_events (
  event_id STRING NOT NULL,
  run_id STRING,
  session_id STRING,
  event_name STRING,
  endpoint STRING,
  target STRING,
  band_id STRING,
  region_key STRING,
  date_key INT64,
  event_ts TIMESTAMP,
  latency_ms INT64,
  http_status INT64,
  is_synthetic BOOL,
  source STRING,
  ingested_at TIMESTAMP
)
-- Keep old event-time history through the Sandbox retention window.
PARTITION BY DATE(ingested_at)
CLUSTER BY region_key, band_id, event_name
OPTIONS(description='One cleaned synthetic anonymous app-usage event.');
