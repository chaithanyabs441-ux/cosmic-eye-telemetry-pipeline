# Query Appendix

Warehouse captures run on 2026-11-11 12:30 UTC. Outputs are actual query results from the configured BigQuery and MongoDB instances. Client elapsed time includes network and job setup; this small pilot makes time comparisons noisy.

---

## q01: q01_exposures_by_band.sql

**Business question:** Which SPHEREx bands show the most observations and noise?

```sql
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
```

**Captured sample output (first 3 rows):**

```json
[
  {
    "band_id": "SPHEREx-D6",
    "wavelength_center_um": 4.7105500000000005,
    "exposure_count": 43,
    "distinct_observations": 42,
    "average_noise_sigma": 0.4441
  },
  {
    "band_id": "SPHEREx-D3",
    "wavelength_center_um": 2.03235,
    "exposure_count": 36,
    "distinct_observations": 36,
    "average_noise_sigma": 0.3535
  },
  {
    "band_id": "SPHEREx-D1",
    "wavelength_center_um": null,
    "exposure_count": 34,
    "distinct_observations": 31,
    "average_noise_sigma": 0.4382
  }
]
```

**Captured result:** 6 rows; client elapsed 2127.59 ms; BigQuery bytes processed 9824.

---

## q02: q02_noisy_regions_having.sql

**Business question:** Which regions have enough exposures to compare noise reliably?

```sql
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
```

**Captured sample output (first 3 rows):**

```json
[
  {
    "region_key": "sky_25_17",
    "exposure_count": 2,
    "average_noise_sigma": 0.7062,
    "maximum_saturation_fraction": 0.0361
  },
  {
    "region_key": "sky_22_05",
    "exposure_count": 2,
    "average_noise_sigma": 0.6665,
    "maximum_saturation_fraction": 0.0133
  },
  {
    "region_key": "sky_09_04",
    "exposure_count": 2,
    "average_noise_sigma": 0.6354,
    "maximum_saturation_fraction": 0.0368
  }
]
```

**Captured result:** 18 rows; client elapsed 1377.91 ms; BigQuery bytes processed 7000.

---

## q03: q03_daily_exposure_running_total.sql

**Business question:** How does the cumulative number of exposures grow by day?

```sql
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
```

**Captured sample output (first 3 rows):**

```json
[
  {
    "ingestion_day": "2026-11-10",
    "daily_exposures": 200,
    "running_exposure_total": 200
  }
]
```

**Captured result:** 1 rows; client elapsed 1522.16 ms; BigQuery bytes processed 1600.

---

## q04: q04_noise_moving_average.sql

**Business question:** How does daily average exposure noise compare with its 7-row moving average?

```sql
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
```

**Captured sample output (first 3 rows):**

```json
[
  {
    "ingestion_day": "2026-11-10",
    "band_id": "SPHEREx-D1",
    "daily_average_noise_sigma": 0.4382,
    "moving_average_noise_sigma_7_rows": 0.4382
  },
  {
    "ingestion_day": "2026-11-10",
    "band_id": "SPHEREx-D2",
    "daily_average_noise_sigma": 0.4277,
    "moving_average_noise_sigma_7_rows": 0.4277
  },
  {
    "ingestion_day": "2026-11-10",
    "band_id": "SPHEREx-D3",
    "daily_average_noise_sigma": 0.3535,
    "moving_average_noise_sigma_7_rows": 0.3535
  }
]
```

**Captured result:** 6 rows; client elapsed 1445.75 ms; BigQuery bytes processed 5600.

---

## q05: q05_rank_regions_per_day.sql

**Business question:** Which sky regions rank highest by exposure count each day?

```sql
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
```

**Captured sample output (first 3 rows):**

```json
[
  {
    "ingestion_day": "2026-11-10",
    "region_key": "sky_14_09",
    "exposure_count": 42,
    "region_rank": 1
  },
  {
    "ingestion_day": "2026-11-10",
    "region_key": "sky_10_15",
    "exposure_count": 3,
    "region_rank": 2
  },
  {
    "ingestion_day": "2026-11-10",
    "region_key": "sky_21_09",
    "exposure_count": 3,
    "region_rank": 2
  }
]
```

**Captured result:** 18 rows; client elapsed 1376.74 ms; BigQuery bytes processed 3800.

---

## q06: q06_usage_latency_by_event.sql

**Business question:** Which synthetic app actions have the highest latency/error rate?

```sql
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
```

**Captured sample output (first 3 rows):**

```json
[
  {
    "event_name": "skymap",
    "request_count": 47,
    "average_latency_ms": 1460.5,
    "p95_latency_ms": 2420,
    "error_count": 4
  },
  {
    "event_name": "browse",
    "request_count": 44,
    "average_latency_ms": 1377.27,
    "p95_latency_ms": 2439,
    "error_count": 4
  },
  {
    "event_name": "preview",
    "request_count": 41,
    "average_latency_ms": 1340.76,
    "p95_latency_ms": 2464,
    "error_count": 7
  }
]
```

**Captured result:** 4 rows; client elapsed 1570.77 ms; BigQuery bytes processed 5745.

---

## q07: q07_join_usage_and_exposures.sql

**Business question:** Compare observation volume with app activity in each sky cell and day.

```sql
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
```

**Captured sample output (first 3 rows):**

```json
[
  {
    "ingestion_day": "2026-11-10",
    "region_key": "sky_00_04",
    "exposure_count": 0,
    "usage_event_count": 2
  },
  {
    "ingestion_day": "2026-11-10",
    "region_key": "sky_00_06",
    "exposure_count": 1,
    "usage_event_count": 0
  },
  {
    "ingestion_day": "2026-11-10",
    "region_key": "sky_00_07",
    "exposure_count": 0,
    "usage_event_count": 1
  }
]
```

**Captured result:** 50 rows; client elapsed 1395.11 ms; BigQuery bytes processed 7220.

---

## q08: q08_latency_above_overall_average.sql

**Business question:** Which app requests were slower than the overall average?

```sql
-- Business question: Which app requests were slower than the overall average?
SELECT
  event_ts,
  region_key,
  event_name,
  latency_ms,
  http_status
FROM `cosmic-eye-telemetry-demo.curated.fact_usage_events`
WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  AND latency_ms > (
    SELECT AVG(latency_ms)
    FROM `cosmic-eye-telemetry-demo.curated.fact_usage_events`
    WHERE DATE(ingested_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
      AND latency_ms IS NOT NULL
  )
ORDER BY latency_ms DESC
LIMIT 20
```

**Captured sample output (first 3 rows):**

```json
[
  {
    "event_ts": "2024-12-31 23:58:33+00:00",
    "region_key": "sky_07_10",
    "event_name": "browse",
    "latency_ms": 2489,
    "http_status": 200
  },
  {
    "event_ts": "2026-11-10 06:12:59.720000+00:00",
    "region_key": "sky_14_09",
    "event_name": "preview",
    "latency_ms": 2482,
    "http_status": 200
  },
  {
    "event_ts": "2026-11-10 06:12:25.380000+00:00",
    "region_key": "sky_35_15",
    "event_name": "search",
    "latency_ms": 2475,
    "http_status": 200
  }
]
```

**Captured result:** 20 rows; client elapsed 1307.46 ms; BigQuery bytes processed 9165.


---

## MongoDB Atlas query captures

Captured 2026-11-11 12:31 UTC from the Atlas summary and alert collections.

## n01: Show recent summaries for one sky region and wavelength band.

**MongoDB operation:** `find({'region_key': 'sky_00_06', 'band_id': 'SPHEREx-D1'}, projection={_id: 0, exposure_count: 1, average_noise_sigma: 1, partition_date: 1}).sort(partition_date DESC).limit(5)`

**Captured sample output:**

```json
[
  {
    "partition_date": "2026-11-10",
    "exposure_count": 1,
    "average_noise_sigma": 0.423395
  }
]
```

**Captured client elapsed:** 19.67 ms.

---

## n02: Aggregate exposure summaries by band.

**MongoDB operation:** `aggregate([$group: {band_id, documents, exposures, average_noise}, $sort: exposures DESC])`

**Captured sample output:**

```json
[
  {
    "_id": "SPHEREx-D6",
    "summary_documents": 28,
    "exposure_count": 43,
    "average_noise_sigma": 0.44780059285714285
  },
  {
    "_id": "SPHEREx-D3",
    "summary_documents": 23,
    "exposure_count": 36,
    "average_noise_sigma": 0.3340638447204969
  },
  {
    "_id": "SPHEREx-D1",
    "summary_documents": 34,
    "exposure_count": 34,
    "average_noise_sigma": 0.43815117647058827
  },
  {
    "_id": "SPHEREx-D5",
    "summary_documents": 25,
    "exposure_count": 31,
    "average_noise_sigma": 0.4362943733333334
  },
  {
    "_id": "SPHEREx-D2",
    "summary_documents": 25,
    "exposure_count": 30,
    "average_noise_sigma": 0.40262862666666666
  },
  {
    "_id": "SPHEREx-D4",
    "summary_documents": 26,
    "exposure_count": 26,
    "average_noise_sigma": 0.36232365384615384
  }
]
```

**Captured client elapsed:** 21.95 ms.

---

## n03: Aggregate app usage by event name and count HTTP errors.

**MongoDB operation:** `aggregate([$group: {event_name, documents, events, errors}, $sort: events DESC])`

**Captured sample output:**

```json
[
  {
    "_id": "search",
    "summary_documents": 44,
    "event_count": 48,
    "error_count": 1,
    "average_latency_ms": 1217.9806201550387
  },
  {
    "_id": "skymap",
    "summary_documents": 46,
    "event_count": 47,
    "error_count": 4,
    "average_latency_ms": 1490.9186046511627
  },
  {
    "_id": "browse",
    "summary_documents": 43,
    "event_count": 44,
    "error_count": 4,
    "average_latency_ms": 1357.2558139534883
  },
  {
    "_id": "preview",
    "summary_documents": 40,
    "event_count": 41,
    "error_count": 7,
    "average_latency_ms": 1359.5675675675675
  }
]
```

**Captured client elapsed:** 21.24 ms.

---

## n04: Retrieve one aggregate document directly by its stable MongoDB _id.

**MongoDB operation:** `find_one({_id: <stable key from the matching region/band document>}, projection)`

**Captured sample output:**

```json
{
  "partition_date": "2026-11-10",
  "region_key": "sky_00_06",
  "band_id": "SPHEREx-D1",
  "exposure_count": 1
}
```

**Captured client elapsed:** 19.44 ms.

---

## n05: Review the three most recent synthetic quality or usage alerts.

**MongoDB operation:** `find({}, projection).sort(created_at DESC).limit(3)`

**Captured sample output:**

```json
[
  {
    "event_type": "exposure",
    "region_key": "sky_23_00",
    "band_id": "SPHEREx-D3",
    "alert_reason": "exposure_quality_threshold",
    "created_at": "2026-11-11 11:41:37.097000"
  },
  {
    "event_type": "exposure",
    "region_key": "sky_23_00",
    "band_id": "SPHEREx-D3",
    "alert_reason": "exposure_quality_threshold",
    "created_at": "2026-11-11 11:41:30.799000"
  },
  {
    "event_type": "exposure",
    "region_key": "sky_23_00",
    "band_id": "SPHEREx-D3",
    "alert_reason": "exposure_quality_threshold",
    "created_at": "2026-11-11 11:41:30.799000"
  }
]
```

**Captured client elapsed:** 19.89 ms.

---

## n06: Use the compound region/band/date index for the region-band lookup.

**MongoDB operation:** `find({'region_key': 'sky_00_06', 'band_id': 'SPHEREx-D1'}).sort(partition_date DESC).hint(region_band_day_lookup).explain(executionStats)`

**Captured sample output:**

```json
{
  "index_scans": [
    "region_band_day_lookup"
  ],
  "total_docs_examined": 1,
  "total_keys_examined": 1,
  "n_returned": 1
}
```

**Captured client elapsed:** 20.27 ms.


---

## Optimization measurement: MongoDB compound index

**Measured:** 2026-11-11 12:31 UTC; 15 client round trips per plan.

The before plan forces a natural collection scan. The after plan uses the existing compound index `region_band_day_lookup` on `(region_key, band_id, partition_date DESC)`. Both use the same filter, projection, and sort.

| Plan | Median client elapsed | Documents examined | Keys examined | Rows returned |
|---|---:|---:|---:|---:|
| Before: collection scan | 20.94 ms | 161 | 0 | 1 |
| After: compound index | 20.68 ms | 1 | 1 | 1 |

Both plans returned the same result. The indexed plan examined fewer documents (1 vs 161). Client time is dominated by Atlas round-trip latency at this tiny pilot size, so the median was similar; this is a measured index-efficiency improvement, not a claim of a measurable wall-clock speedup.

Reproduce with `pipeline\.venv\Scripts\python.exe queries\nosql\run_optimization.py`.

---

## Continuous streaming query captures

Implementations and query definitions: [streaming query map](../queries/streaming/README.md) and [`pipeline/streaming.py`](../pipeline/streaming.py). The Spark job uses a five-second processing trigger and a two-minute event-time watermark. Continuous streams do not finish like SQL jobs, so no single query runtime exists. In the captured Docker logs, a micro-batch exceeded the five-second trigger at 5.105 seconds and 6.233 seconds while the local machine was processing. These are observed batch processing times, not a service-level latency guarantee.

Read-only Atlas snapshot from `pipeline\verify_streaming.py` on 2026-11-11:

| Continuous query | Stored output at capture | Sample captured output |
|---|---:|---|
| s01: one-minute event counts by region and band | 63 `events_per_region_band_1m` summaries | Region `sky_20_00`, band `SPHEREx-D6`, one event in window 11:41–11:42 UTC. |
| s02: five-minute sliding usage latency/errors | 280 `usage_latency_errors_5m` summaries | Region `sky_20_00`, one request, average/p95 latency 1,316 ms, zero errors. |
| s03: event-triggered quality and usage alerts | 18 exposure alerts + 15 usage latency/error alerts | Most recent captured exposure alert: region `sky_23_00`, band `SPHEREx-D3`, reason `exposure_quality_threshold`. |

**Combined live snapshot:** 343 metric summary documents and 33 alert documents in Atlas. The dashboard reads these collections and refreshes every five seconds. Reproduce the read-only verification with `pipeline\.venv\Scripts\python.exe pipeline\verify_streaming.py` while Atlas is reachable. The event-count/latency sample values describe synthetic demo traffic, not actual NASA quality measurements.

### s01 — One-minute tumbling counts

**Business question:** How many exposure and app events occur in each sky region and band per minute?

**Query text:**

```python
watermarked.groupBy(
    window(col("event_time"), "1 minute"), col("region_key"), col("band_id")
).agg(count("*").alias("event_count"))
```

**Captured sample output:**

```json
{"region_key":"sky_20_00","band_id":"SPHEREx-D6","event_count":1,"window_start":"2026-11-11 11:41:00","window_end":"2026-11-11 11:42:00"}
```

**Execution time:** continuous; trigger is every 5 seconds. Docker logs observed batches at 5.105 and 6.233 seconds for the running Spark process; those warnings are process-level and cannot be assigned to a specific one of the three queries.

### s02 — Five-minute sliding usage latency and error rate

**Business question:** Which regions have slow app responses or HTTP errors over recent requests?

**Query text:**

```python
usage.filter(col("event_type") == "usage").groupBy(
    window(col("event_time"), "5 minutes", "1 minute"), col("region_key")
).agg(
    count("*").alias("request_count"),
    avg("latency_ms").alias("average_latency_ms"),
    expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
    spark_sum(when(col("http_status") >= 400, 1).otherwise(0)).alias("error_count"),
)
```

**Captured sample output:**

```json
{"region_key":"sky_20_00","request_count":1,"average_latency_ms":1316.0,"p95_latency_ms":1316,"error_count":0,"window_start":"2026-11-11 11:41:00","window_end":"2026-11-11 11:46:00"}
```

**Execution time:** continuous; five-second trigger. The process-level observed batch durations are recorded under s01 above; Spark did not log a per-query timing metric in this configuration.

### s03 — Event-triggered alerts

**Business question:** Which individual exposure or app-use events cross the configured quality or response thresholds?

**Query text:**

```python
alerts = (
    parsed.withColumn("noise_sigma", col("quality_metrics.noise_sigma"))
    .withColumn("saturated_pixel_fraction", col("quality_metrics.saturated_pixel_fraction"))
    .withColumn(
        "alert_reason",
        when(
            (col("event_type") == "exposure")
            & ((col("noise_sigma") >= 0.75) | (col("saturated_pixel_fraction") >= 0.25)),
            expr("'exposure_quality_threshold'"),
        ).when(
            (col("event_type") == "usage")
            & ((col("latency_ms") >= 2000) | (col("http_status") >= 500)),
            expr("'usage_latency_or_http_error'"),
        ),
    )
    .filter(col("alert_reason").isNotNull())
)
```

Matching rows are upserted by `event_id` to `live_alerts`.

**Captured sample output:**

```json
{"event_type":"exposure","region_key":"sky_23_00","band_id":"SPHEREx-D3","noise_sigma":0.79,"saturated_pixel_fraction":0.35,"alert_reason":"exposure_quality_threshold"}
```

**Execution time:** continuous; five-second trigger. The same Spark process-level batch observations are reported above; per-query duration was not emitted by the current Spark log configuration.
