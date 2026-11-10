{{ config(materialized='table') }}

WITH dates AS (
  SELECT DISTINCT DATE(event_ts) AS full_date
  FROM {{ ref('clean_events') }}
  WHERE event_ts IS NOT NULL
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64) AS date_key,
  full_date,
  EXTRACT(YEAR FROM full_date) AS calendar_year,
  EXTRACT(QUARTER FROM full_date) AS quarter,
  EXTRACT(MONTH FROM full_date) AS month,
  EXTRACT(WEEK FROM full_date) AS week_of_year,
  EXTRACT(DAY FROM full_date) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week
FROM dates
