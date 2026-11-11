"""Run six real MongoDB Atlas queries, including an indexed explain plan."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
import time
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")
CAPTURED: list[str] = []


def index_scans(node: Any) -> list[str]:
    names: list[str] = []
    if isinstance(node, dict):
        if node.get("stage") == "IXSCAN" and node.get("indexName"):
            names.append(str(node["indexName"]))
        for value in node.values():
            names.extend(index_scans(value))
    elif isinstance(node, list):
        for value in node:
            names.extend(index_scans(value))
    return names


def run(query_id: str, question: str, query_text: str, operation: Callable[[], Any]) -> None:
    started = time.perf_counter()
    result = operation()
    elapsed_ms = (time.perf_counter() - started) * 1000
    print(f"\n{query_id}: {question}")
    print(f"Query: {query_text}")
    print("Sample result:")
    print(json.dumps(result, indent=2, default=str))
    print(f"Client elapsed: {elapsed_ms:.2f} ms")
    CAPTURED.append(
        "\n".join(
            [
                f"## {query_id}: {question}",
                "",
                f"**MongoDB operation:** `{query_text}`",
                "",
                "**Captured sample output:**",
                "",
                "```json",
                json.dumps(result, indent=2, default=str),
                "```",
                "",
                f"**Captured client elapsed:** {elapsed_ms:.2f} ms.",
            ]
        )
    )


def main() -> int:
    uri = os.getenv("MONGODB_URI", "").strip()
    if not uri:
        raise RuntimeError("MONGODB_URI is missing from the repository-root .env file")
    database_name = os.getenv("MONGODB_DATABASE", "cosmic_eye").strip()

    with MongoClient(uri, serverSelectionTimeoutMS=15000) as client:
        client.admin.command("ping")
        db = client[database_name]
        exposures = db["exposure_daily_summaries"]
        usages = db["usage_daily_summaries"]
        alerts = db["live_alerts"]

        sample_key = exposures.find_one(
            {"region_key": {"$exists": True}, "band_id": {"$exists": True}},
            {"_id": 1, "region_key": 1, "band_id": 1},
        )
        if not sample_key:
            raise RuntimeError("No exposure summary documents found; run pipeline/sync_mongodb.py first")
        region = sample_key["region_key"]
        band = sample_key["band_id"]
        region_band_filter = {"region_key": region, "band_id": band}

        run(
            "n01",
            "Show recent summaries for one sky region and wavelength band.",
            f"find({region_band_filter!r}, projection={{_id: 0, exposure_count: 1, average_noise_sigma: 1, partition_date: 1}}).sort(partition_date DESC).limit(5)",
            lambda: list(
                exposures.find(
                    region_band_filter,
                    {"_id": 0, "exposure_count": 1, "average_noise_sigma": 1, "partition_date": 1},
                )
                .sort("partition_date", DESCENDING)
                .limit(5)
            ),
        )

        run(
            "n02",
            "Aggregate exposure summaries by band.",
            "aggregate([$group: {band_id, documents, exposures, average_noise}, $sort: exposures DESC])",
            lambda: list(
                exposures.aggregate(
                    [
                        {
                            "$group": {
                                "_id": "$band_id",
                                "summary_documents": {"$sum": 1},
                                "exposure_count": {"$sum": "$exposure_count"},
                                "average_noise_sigma": {"$avg": "$average_noise_sigma"},
                            }
                        },
                        {"$sort": {"exposure_count": -1}},
                        {"$limit": 10},
                    ]
                )
            ),
        )

        run(
            "n03",
            "Aggregate app usage by event name and count HTTP errors.",
            "aggregate([$group: {event_name, documents, events, errors}, $sort: events DESC])",
            lambda: list(
                usages.aggregate(
                    [
                        {
                            "$group": {
                                "_id": "$event_name",
                                "summary_documents": {"$sum": 1},
                                "event_count": {"$sum": "$event_count"},
                                "error_count": {"$sum": "$error_count"},
                                "average_latency_ms": {"$avg": "$average_latency_ms"},
                            }
                        },
                        {"$sort": {"event_count": -1}},
                        {"$limit": 10},
                    ]
                )
            ),
        )

        run(
            "n04",
            "Retrieve one aggregate document directly by its stable MongoDB _id.",
            "find_one({_id: <stable key from the matching region/band document>}, projection)",
            lambda: {
                key: value
                for key, value in (
                    exposures.find_one(
                        {"_id": sample_key["_id"]},
                        {"_id": 0, "region_key": 1, "band_id": 1, "partition_date": 1, "exposure_count": 1},
                    )
                    or {}
                ).items()
            },
        )

        run(
            "n05",
            "Review the three most recent synthetic quality or usage alerts.",
            "find({}, projection).sort(created_at DESC).limit(3)",
            lambda: list(
                alerts.find(
                    {},
                    {"_id": 0, "event_type": 1, "region_key": 1, "band_id": 1, "alert_reason": 1, "created_at": 1},
                )
                .sort("created_at", DESCENDING)
                .limit(3)
            ),
        )

        def explain_indexed_region_band() -> dict[str, Any]:
            explanation = db.command(
                "explain",
                {
                    "find": exposures.name,
                    "filter": region_band_filter,
                    "projection": {"_id": 0, "partition_date": 1, "exposure_count": 1},
                    "sort": {"partition_date": DESCENDING},
                    "hint": "region_band_day_lookup",
                },
                verbosity="executionStats",
            )
            return {
                "index_scans": sorted(set(index_scans(explanation.get("queryPlanner", {}).get("winningPlan", {})))),
                "total_docs_examined": explanation.get("executionStats", {}).get("totalDocsExamined"),
                "total_keys_examined": explanation.get("executionStats", {}).get("totalKeysExamined"),
                "n_returned": explanation.get("executionStats", {}).get("nReturned"),
            }

        run(
            "n06",
            "Use the compound region/band/date index for the region-band lookup.",
            f"find({region_band_filter!r}).sort(partition_date DESC).hint(region_band_day_lookup).explain(executionStats)",
            explain_indexed_region_band,
        )

    print("\nMongoDB queries: PASS")
    appendix_path = ROOT / "docs" / "QUERY_APPENDIX.md"
    appendix_text = appendix_path.read_text(encoding="utf-8")
    blocks = [
        block
        for block in appendix_text.split("\n---\n")
        if not (
            block.lstrip().startswith("## MongoDB Atlas query captures")
            or any(block.lstrip().startswith(f"## n0{index}:") for index in range(1, 7))
        )
    ]
    blocks.append(
        "\n## MongoDB Atlas query captures\n\n"
        f"Captured {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} from the Atlas summary and alert collections.\n\n"
        + "\n\n---\n\n".join(CAPTURED)
    )
    appendix_text = "\n---\n".join(blocks) + "\n"
    appendix_path.write_text(appendix_text, encoding="utf-8")
    print(f"Appended real MongoDB captures to {appendix_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
