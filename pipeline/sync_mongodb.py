"""Incrementally copy compact BigQuery aggregates into MongoDB Atlas.

The primary event rows remain in BigQuery. MongoDB receives one compact
summary document per ingestion date and region/band (exposures), or per
ingestion date and region/event name (usage). Re-running a partition replaces
the document with the same deterministic _id, so retries do not double-count.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is missing; set it in .env or the GitHub Actions secret/environment")
    return value


def validated_identifier(name: str, value: str) -> str:
    import re

    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise RuntimeError(f"{name} must contain only letters, numbers, and underscores")
    return value


def validated_project_id(value: str) -> str:
    import re

    if not re.fullmatch(r"[A-Za-z0-9-]+", value):
        raise RuntimeError("GCP_PROJECT_ID must contain only letters, numbers, and hyphens")
    return value


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def stable_id(*parts: str) -> str:
    return json.dumps(parts, ensure_ascii=True, separators=(",", ":"))


def process_collection(
    *,
    bq_client: Any,
    mongo_db: Any,
    project: str,
    dataset: str,
    table: str,
    collection_name: str,
    state_id: str,
    query: str,
    id_fields: tuple[str, ...],
    retention_cutoff: date,
) -> tuple[int, int, int, int, date | None, int | None]:
    from google.cloud import bigquery

    collection = mongo_db[collection_name]
    state_collection = mongo_db["pipeline_state"]
    state = state_collection.find_one({"_id": state_id})

    # Re-read the last fully processed day. The one-day overlap catches records
    # added late to yesterday's ingestion partition; deterministic replacements
    # make the overlap safe on retries.
    if state and state.get("last_partition_date"):
        last_day = date.fromisoformat(state["last_partition_date"])
        start_day = max(retention_cutoff, last_day - timedelta(days=1))
    else:
        start_day = retention_cutoff

    sql = query.format(project=project, dataset=dataset, table=table)
    job = bq_client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("start_date", "DATE", start_day)]
        ),
    )
    rows = [dict(row.items()) for row in job.result()]

    # The collection stores aggregate summaries only, never a copy of the
    # primary event dataset. Keep a guard against accidental high cardinality.
    max_docs = int(os.getenv("MONGODB_MAX_SUMMARY_DOCS", "10000"))
    if len(rows) > max_docs:
        raise RuntimeError(
            f"{collection_name} would contain {len(rows)} summaries, exceeding "
            f"MONGODB_MAX_SUMMARY_DOCS={max_docs}; no writes were attempted"
        )

    from pymongo import ReplaceOne

    operations = []
    newest_partition: date | None = None
    for row in rows:
        partition = row.pop("partition_date")
        partition_text = partition.isoformat()
        newest_partition = max(newest_partition, partition) if newest_partition else partition
        parts = [partition_text, *(str(row[field]) for field in id_fields)]
        document = {"_id": stable_id(*parts), "partition_date": partition_text, **row}
        for key, value in tuple(document.items()):
            if isinstance(value, datetime):
                document[key] = as_utc(value)
        operations.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))

    # Writes are chunked to keep requests comfortably below Atlas M0 limits.
    upserted = modified = 0
    for offset in range(0, len(operations), 500):
        result = collection.bulk_write(operations[offset : offset + 500], ordered=False)
        upserted += result.upserted_count
        modified += result.modified_count

    collection.delete_many({"partition_date": {"$lt": retention_cutoff.isoformat()}})

    # Advance the checkpoint only after every upsert succeeds. A failed run
    # repeats its range safely because all document keys are deterministic.
    if newest_partition:
        previous_day = state.get("last_partition_date") if state else None
        last_day = max(previous_day or "0001-01-01", newest_partition.isoformat())
        state_collection.replace_one(
            {"_id": state_id},
            {
                "_id": state_id,
                "last_partition_date": last_day,
                "updated_at": datetime.now(timezone.utc),
                "collection": collection_name,
            },
            upsert=True,
        )

    return (
        len(rows),
        upserted,
        modified,
        collection.count_documents({}),
        newest_partition,
        job.total_bytes_processed,
    )


EXPOSURE_SQL = """
SELECT
  DATE(ingested_at) AS partition_date,
  region_key,
  band_id,
  COUNT(*) AS exposure_count,
  COUNT(DISTINCT obs_id) AS observation_count,
  COUNTIF(is_synthetic) AS synthetic_count,
  AVG(noise_sigma) AS average_noise_sigma,
  MAX(noise_sigma) AS maximum_noise_sigma,
  AVG(saturated_pixel_fraction) AS average_saturation_fraction,
  MAX(pointing_error_arcsec) AS maximum_pointing_error_arcsec,
  MAX(event_ts) AS latest_event_at
FROM `{project}.{dataset}.{table}`
WHERE DATE(ingested_at) >= @start_date
  AND region_key IS NOT NULL AND band_id IS NOT NULL
GROUP BY partition_date, region_key, band_id
"""

USAGE_SQL = """
SELECT
  DATE(ingested_at) AS partition_date,
  region_key,
  COALESCE(event_name, 'unknown') AS event_name,
  COUNT(*) AS event_count,
  COUNTIF(http_status >= 400) AS error_count,
  COUNTIF(is_synthetic) AS synthetic_count,
  AVG(latency_ms) AS average_latency_ms,
  APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
  MAX(event_ts) AS latest_event_at
FROM `{project}.{dataset}.{table}`
WHERE DATE(ingested_at) >= @start_date
  AND region_key IS NOT NULL
GROUP BY partition_date, region_key, event_name
"""


def main() -> int:
    from google.cloud import bigquery
    from pymongo import MongoClient, ASCENDING, DESCENDING

    project = validated_project_id(required_env("GCP_PROJECT_ID"))
    dataset = validated_identifier(
        "BIGQUERY_CURATED_DATASET", os.getenv("BIGQUERY_CURATED_DATASET", "curated").strip()
    )
    location = os.getenv("BIGQUERY_LOCATION", "US").strip()
    credentials = Path(required_env("GOOGLE_APPLICATION_CREDENTIALS"))
    if not credentials.is_file():
        raise RuntimeError("The configured BigQuery credential file was not found")

    database_name = validated_identifier(
        "MONGODB_DATABASE", os.getenv("MONGODB_DATABASE", "cosmic_eye").strip()
    )
    uri = required_env("MONGODB_URI")
    now_utc = datetime.now(timezone.utc).date()
    retention_cutoff = now_utc - timedelta(days=59)

    bq_client = bigquery.Client(project=project, location=location)
    mongo_client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    try:
        mongo_client.admin.command("ping")
        mongo_db = mongo_client[database_name]

        exposures = mongo_db["exposure_daily_summaries"]
        usages = mongo_db["usage_daily_summaries"]
        exposures.create_index(
            [("region_key", ASCENDING), ("band_id", ASCENDING), ("partition_date", DESCENDING)],
            name="region_band_day_lookup",
        )
        usages.create_index(
            [("region_key", ASCENDING), ("event_name", ASCENDING), ("partition_date", DESCENDING)],
            name="region_event_day_lookup",
        )

        results = []
        results.append(
            (
                "exposures",
                process_collection(
                    bq_client=bq_client,
                    mongo_db=mongo_db,
                    project=project,
                    dataset=dataset,
                    table="fact_exposures",
                    collection_name="exposure_daily_summaries",
                    state_id="exposures",
                    query=EXPOSURE_SQL,
                    id_fields=("region_key", "band_id"),
                    retention_cutoff=retention_cutoff,
                ),
            )
        )
        results.append(
            (
                "usage",
                process_collection(
                    bq_client=bq_client,
                    mongo_db=mongo_db,
                    project=project,
                    dataset=dataset,
                    table="fact_usage_events",
                    collection_name="usage_daily_summaries",
                    state_id="usage",
                    query=USAGE_SQL,
                    id_fields=("region_key", "event_name"),
                    retention_cutoff=retention_cutoff,
                ),
            )
        )

        for label, (groups, upserted, modified, stored, newest_partition, bytes_processed) in results:
            print(
                f"{label}: BigQuery groups={groups}; Mongo upserted={upserted}; "
                f"changed={modified}; stored documents={stored}; "
                f"latest ingestion date={newest_partition or 'none'}; "
                f"BigQuery bytes processed={bytes_processed}"
            )

        for name in ("exposure_daily_summaries", "usage_daily_summaries"):
            collection = mongo_db[name]
            sample = collection.find_one({}, {"_id": 0})
            print(f"{name} sample: {json.dumps(sample, default=str, sort_keys=True) if sample else 'no rows'}")
            print(f"{name} indexes: {json.dumps(list(collection.index_information().keys()))}")

        state_collection = mongo_db["pipeline_state"]
        state_collection.replace_one(
            {"_id": "last_sync"},
            {
                "_id": "last_sync",
                "completed_at": datetime.now(timezone.utc),
                "exposure_summary_documents": exposures.count_documents({}),
                "usage_summary_documents": usages.count_documents({}),
            },
            upsert=True,
        )
        print("MongoDB sync: PASS (aggregate summaries only; no primary event rows copied)")
        return 0
    finally:
        mongo_client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"MongoDB sync: FAIL ({type(exc).__name__}: {exc})", file=sys.stderr)
        raise SystemExit(1)
