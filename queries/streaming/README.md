# Continuous streaming query definitions

The executable implementations live in [`pipeline/streaming.py`](../../pipeline/streaming.py).
The job reads JSON events from Redpanda topic `cosmic-eye-live`, uses a two-minute
event-time watermark for window aggregations, and writes through five-second
micro-batch triggers to MongoDB. A continuous query has no one-time completion
duration; observed batch trigger/processing evidence is recorded in
[`docs/QUERY_APPENDIX.md`](../../docs/QUERY_APPENDIX.md).

## s01 — One-minute tumbling event counts

Business question: How many exposure and usage events arrived per sky region
and band in each one-minute event-time window?

Implementation in `pipeline/streaming.py`:

```python
watermarked.groupBy(
    window(col("event_time"), "1 minute"), col("region_key"), col("band_id")
).agg(count("*").alias("event_count"))
```

The result is written to `cosmic_eye.live_metrics` with metric name
`events_per_region_band_1m`.

## s02 — Five-minute sliding latency and error summary

Business question: Which regions are seeing elevated request latency or HTTP
errors over recent app activity?

Implementation in `pipeline/streaming.py`:

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

The result is written to `cosmic_eye.live_metrics` with metric name
`usage_latency_errors_5m`.

## s03 — Event-triggered quality and usage alerts

Business question: Which individual exposure or app events crossed a quality,
latency, or server-error threshold?

The implemented filter in `pipeline/streaming.py` creates an alert for an
exposure with noise sigma >= 0.75 or saturated-pixel fraction >= 0.25, and for
usage with latency >= 2,000 ms or HTTP status >= 500. Alerts are keyed by
`event_id` and upserted to `cosmic_eye.live_alerts`.

The captured output and observed runtime are in the query appendix.
