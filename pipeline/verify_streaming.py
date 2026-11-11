"""Print a credential-safe summary of Phase 6 MongoDB streaming results."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def main() -> int:
    uri = os.getenv("MONGODB_URI", "").strip()
    if not uri:
        raise RuntimeError("MONGODB_URI is missing from the local .env file")

    database_name = os.getenv("MONGODB_DATABASE", "cosmic_eye").strip()
    with MongoClient(uri, serverSelectionTimeoutMS=15000) as client:
        client.admin.command("ping")
        db = client[database_name]
        metrics = db["live_metrics"]
        alerts = db["live_alerts"]
        print(f"MongoDB: PASS (database={database_name})")
        print(f"Live metric summary documents: {metrics.count_documents({})}")
        print(f"Live alert documents: {alerts.count_documents({})}")
        for row in metrics.aggregate(
            [{"$group": {"_id": "$metric_name", "documents": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
        ):
            print(f"Metric {row['_id']}: {row['documents']}")
        for row in alerts.aggregate(
            [{"$group": {"_id": "$alert_reason", "documents": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
        ):
            print(f"Alert {row['_id']}: {row['documents']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
