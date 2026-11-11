"""Run three local Spark Structured Streaming queries from Redpanda to MongoDB."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

SPARK_VERSION = "4.3.0"
KAFKA_CONNECTOR = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"
ALERT_LATENCY_MS = int(os.getenv("ALERT_LATENCY_MS", "3000"))
ALERT_NOISE_SIGMA = float(os.getenv("ALERT_NOISE_SIGMA", "0.75"))
ALERT_SATURATED_FRACTION = float(os.getenv("ALERT_SATURATED_FRACTION", "0.25"))


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is missing from .env")
    return value


def mongo_sink(collection_name: str, key_columns: tuple[str, ...]):
    """Create a foreachBatch sink with deterministic MongoDB upserts."""

    def write_batch(batch_df: Any, batch_id: int) -> None:
        from pymongo import MongoClient, ReplaceOne

        uri = required_env("MONGODB_URI")
        database_name = os.getenv("MONGODB_DATABASE", "cosmic_eye").strip()
        collection = None
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=15000)
            client.admin.command("ping")
            collection = client[database_name][collection_name]
            operations = []
            for row in batch_df.toLocalIterator():
                document = row.asDict(recursive=True)
                window = document.pop("window", None)
                if window:
                    start = window.get("start")
                    end = window.get("end")
                    document["window_start"] = start
                    document["window_end"] = end
                    start_key = start.isoformat() if start else "unknown"
                else:
                    start_key = "event"
                identity = [str(document.get(column, "")) for column in key_columns]
                metric_name = str(document.pop("metric_name", collection_name))
                document["_id"] = json.dumps(
                    [metric_name, start_key, *identity], separators=(",", ":")
                )
                document["metric_name"] = metric_name
                document["updated_at"] = datetime.now(timezone.utc)
                operations.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))

            for offset in range(0, len(operations), 500):
                collection.bulk_write(operations[offset : offset + 500], ordered=False)
            if operations:
                print(f"{collection_name}: batch={batch_id}, summary documents upserted={len(operations)}")
        finally:
            if collection is not None:
                client.close()

    return write_batch


def alert_sink(batch_df: Any, batch_id: int) -> None:
    """Store event-triggered alerts keyed by event_id to prevent duplicates."""
    from pymongo import MongoClient, ReplaceOne

    client = MongoClient(required_env("MONGODB_URI"), serverSelectionTimeoutMS=15000)
    try:
        client.admin.command("ping")
        collection = client[os.getenv("MONGODB_DATABASE", "cosmic_eye")]["live_alerts"]
        operations = []
        for row in batch_df.toLocalIterator():
            document = row.asDict(recursive=True)
            document["_id"] = document.pop("event_id")
            document["created_at"] = datetime.now(timezone.utc)
            operations.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))
        for offset in range(0, len(operations), 500):
            collection.bulk_write(operations[offset : offset + 500], ordered=False)
        if operations:
            print(f"live_alerts: batch={batch_id}, alert documents upserted={len(operations)}")
    finally:
        client.close()


def main() -> int:
    # Select the active venv's interpreter explicitly; Windows often lacks a
    # separate `python3.exe` command even when `python.exe` is available.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        avg,
        col,
        count,
        expr,
        from_json,
        sum as spark_sum,
        to_timestamp,
        when,
        window,
    )
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    from pymongo import ASCENDING, DESCENDING, MongoClient

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
    topic = os.getenv("KAFKA_LIVE_TOPIC", "cosmic-eye-live").strip()
    database_name = os.getenv("MONGODB_DATABASE", "cosmic_eye").strip()
    uri = required_env("MONGODB_URI")
    checkpoint_root = ROOT / "spark-checkpoints"

    mongo = MongoClient(uri, serverSelectionTimeoutMS=15000)
    try:
        mongo.admin.command("ping")
        db = mongo[database_name]
        db["live_metrics"].create_index(
            [("metric_name", ASCENDING), ("window_start", DESCENDING), ("region_key", ASCENDING)],
            name="live_metric_window_region",
        )
        db["live_alerts"].create_index([("created_at", DESCENDING)], name="recent_live_alerts")
    finally:
        mongo.close()

    quality_schema = StructType(
        [
            StructField("noise_sigma", DoubleType()),
            StructField("saturated_pixel_fraction", DoubleType()),
        ]
    )
    event_schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_ts", StringType()),
            StructField("region_key", StringType()),
            StructField("band_id", StringType()),
            StructField("event_name", StringType()),
            StructField("latency_ms", IntegerType()),
            StructField("http_status", IntegerType()),
            StructField("is_synthetic", BooleanType()),
            StructField("quality_metrics", quality_schema),
        ]
    )

    spark = (
        SparkSession.builder.appName("CosmicEyeLiveAnalytics")
        .master("local[2]")
        .config("spark.jars.packages", KAFKA_CONNECTOR)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "1000")
        .load()
    )
    parsed = (
        raw_stream.select(from_json(col("value").cast("string"), event_schema).alias("event"))
        .select("event.*")
        .withColumn("event_time", to_timestamp(col("event_ts")))
        .filter(col("event_id").isNotNull() & col("event_time").isNotNull())
    )
    watermarked = parsed.withWatermark("event_time", "2 minutes")

    # Continuous query 1: one-minute tumbling event counts by region and band.
    event_counts = (
        watermarked.groupBy(window(col("event_time"), "1 minute"), col("region_key"), col("band_id"))
        .agg(count("*").alias("event_count"))
        .withColumn("metric_name", expr("'events_per_region_band_1m'"))
    )

    # Continuous query 2: five-minute sliding usage latency and error metrics.
    usage = watermarked.filter(col("event_type") == "usage")
    usage_metrics = (
        usage.groupBy(window(col("event_time"), "5 minutes", "1 minute"), col("region_key"))
        .agg(
            count("*").alias("request_count"),
            avg("latency_ms").alias("average_latency_ms"),
            expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
            spark_sum(when(col("http_status") >= 400, 1).otherwise(0)).alias("error_count"),
        )
        .withColumn("metric_name", expr("'usage_latency_errors_5m'"))
    )

    # Continuous query 3: an event-triggered alert stream for synthetic
    # exposure quality spikes and high-latency/error app events.
    alerts = (
        parsed.withColumn("noise_sigma", col("quality_metrics.noise_sigma"))
        .withColumn("saturated_pixel_fraction", col("quality_metrics.saturated_pixel_fraction"))
        .withColumn(
            "alert_reason",
            when(
                (col("event_type") == "exposure")
                & (
                    (col("noise_sigma") >= ALERT_NOISE_SIGMA)
                    | (col("saturated_pixel_fraction") >= ALERT_SATURATED_FRACTION)
                ),
                expr("'exposure_quality_threshold'"),
            )
            .when(
                (col("event_type") == "usage")
                & (col("http_status") >= 400),
                expr("'http_error'"),
            )
            .when(
                (col("event_type") == "usage")
                & (col("latency_ms") >= ALERT_LATENCY_MS),
                expr("'high_latency'"),
            ),
        )
        .filter(col("alert_reason").isNotNull())
        .select(
            "event_id",
            col("event_time").alias("event_at"),
            "event_type",
            "region_key",
            "band_id",
            "event_name",
            "noise_sigma",
            "saturated_pixel_fraction",
            "latency_ms",
            "http_status",
            "alert_reason",
        )
    )

    queries = [
        event_counts.writeStream.outputMode("update")
        .foreachBatch(mongo_sink("live_metrics", ("region_key", "band_id")))
        .option("checkpointLocation", str(checkpoint_root / "event-counts-1m"))
        .trigger(processingTime="5 seconds")
        .start(),
        usage_metrics.writeStream.outputMode("update")
        .foreachBatch(mongo_sink("live_metrics", ("region_key",)))
        .option("checkpointLocation", str(checkpoint_root / "usage-latency-5m"))
        .trigger(processingTime="5 seconds")
        .start(),
        alerts.writeStream.outputMode("append")
        .foreachBatch(alert_sink)
        .option("checkpointLocation", str(checkpoint_root / "quality-alerts"))
        .trigger(processingTime="5 seconds")
        .start(),
    ]

    print(
        f"Spark Structured Streaming started: topic={topic}; three queries active; "
        f"watermarks=2 minutes; alerts latency_ms>={ALERT_LATENCY_MS}, "
        f"noise_sigma>={ALERT_NOISE_SIGMA:g}, saturation>={ALERT_SATURATED_FRACTION:g}"
    )
    print("Press Ctrl+C to stop the streaming queries.")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping the three Spark streaming queries.")
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Spark streaming: FAIL ({type(exc).__name__}: {exc})", file=sys.stderr)
        raise SystemExit(1)
