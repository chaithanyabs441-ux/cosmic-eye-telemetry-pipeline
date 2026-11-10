"""Check temporary BigQuery, MongoDB Atlas, and local Redpanda access.

All inserted values are a single synthetic smoke-test record. The test deletes
its temporary BigQuery table, MongoDB database, collection, and Kafka topic.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def check_bigquery() -> None:
    from google.cloud import bigquery

    project_id = os.getenv("GCP_PROJECT_ID", "").strip()
    location = os.getenv("BIGQUERY_LOCATION", "US").strip()
    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID is empty in the local .env file")

    client = bigquery.Client(project=project_id, location=location)
    dataset_name = os.getenv("BIGQUERY_SMOKE_DATASET", "phase1_smoke").strip()
    if not dataset_name:
        raise RuntimeError("BIGQUERY_SMOKE_DATASET is empty in the local .env file")
    dataset_id = f"{project_id}.{dataset_name}"
    table_id = f"{dataset_id}.check_{uuid.uuid4().hex[:10]}"
    try:
        schema = [
            bigquery.SchemaField("check_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_synthetic", "BOOL", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        # A batch load job works in BigQuery Sandbox; streaming inserts do not.
        load_job = client.load_table_from_json(
            [{"check_id": "phase1-smoke", "is_synthetic": True}],
            table_id,
            job_config=bigquery.LoadJobConfig(schema=schema),
            location=location,
        )
        load_job.result(timeout=60)
        rows = list(client.list_rows(table_id, selected_fields=schema))
        if len(rows) != 1 or rows[0]["check_id"] != "phase1-smoke":
            raise RuntimeError("BigQuery read did not return the inserted row")
    finally:
        client.delete_table(table_id, not_found_ok=True)


def check_mongodb() -> None:
    from pymongo import MongoClient

    uri = os.getenv("MONGODB_URI", "").strip()
    if not uri:
        raise RuntimeError("MONGODB_URI is empty in the local .env file")
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    db_name = f"phase1_smoke_{uuid.uuid4().hex[:10]}"
    try:
        collection = client[db_name]["check_records"]
        result = collection.insert_one(
            {"check_id": "phase1-smoke", "is_synthetic": True}
        )
        row = collection.find_one({"_id": result.inserted_id})
        if row is None or row.get("check_id") != "phase1-smoke":
            raise RuntimeError("MongoDB read did not return the inserted document")
        if collection.delete_one({"_id": result.inserted_id}).deleted_count != 1:
            raise RuntimeError("MongoDB did not delete the temporary document")
    finally:
        client.drop_database(db_name)
        client.close()


def check_redpanda() -> None:
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
    topic = f"phase1-smoke-{uuid.uuid4().hex[:10]}"
    admin = AdminClient({"bootstrap.servers": brokers})
    created = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    created[topic].result(timeout=20)
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"{topic}-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([topic])
        producer = Producer({"bootstrap.servers": brokers})
        delivered: list[object] = []
        producer.produce(topic, key=b"check", value=b"phase1-smoke", callback=lambda err, msg: delivered.append(err))
        remaining = producer.flush(15)
        if remaining or delivered != [None]:
            raise RuntimeError("Redpanda producer did not confirm delivery")

        message = None
        for _ in range(30):
            candidate = consumer.poll(0.5)
            if candidate is None:
                continue
            if candidate.error():
                raise RuntimeError(f"Redpanda consumer error: {candidate.error()}")
            message = candidate
            break
        if message is None or message.value() != b"phase1-smoke":
            raise RuntimeError("Redpanda consumer did not read the produced message")
    finally:
        consumer.close()
        admin.delete_topics([topic])[topic].result(timeout=20)


def main() -> int:
    checks = [
        ("BigQuery", check_bigquery),
        ("MongoDB Atlas", check_mongodb),
        ("Redpanda", check_redpanda),
    ]
    failures = 0
    for label, check in checks:
        try:
            check()
        except Exception as exc:  # Print an actionable failure without credentials.
            failures += 1
            print(f"{label}: FAIL ({type(exc).__name__}: {exc})")
        else:
            print(f"{label}: PASS (temporary test data deleted)")
    print(f"Smoke tests: {len(checks) - failures}/{len(checks)} PASS")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
