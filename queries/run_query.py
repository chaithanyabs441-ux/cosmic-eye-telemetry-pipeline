"""Run one documented BigQuery query or all warehouse queries by ID."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
QUERY_DIR = ROOT / "queries" / "warehouse"
load_dotenv(ROOT / ".env")


def available_queries() -> dict[str, Path]:
    return {
        path.name.split("_")[0]: path
        for path in sorted(QUERY_DIR.glob("q*.sql"))
    }


def question_for(sql: str) -> str:
    for line in sql.splitlines():
        match = re.match(r"\s*--\s*Business question:\s*(.+)", line, re.I)
        if match:
            return match.group(1).strip()
    return "(business question not documented)"


def row_to_json(row: Any) -> dict[str, Any]:
    return {key: value for key, value in row.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query_id", help="q01 through q08, or 'all'")
    parser.add_argument(
        "--appendix",
        action="store_true",
        help="append captured query text, sample rows, and timings to docs/QUERY_APPENDIX.md",
    )
    args = parser.parse_args()

    queries = available_queries()
    requested = sorted(queries) if args.query_id.lower() == "all" else [args.query_id.lower()]
    missing = [query_id for query_id in requested if query_id not in queries]
    if missing:
        parser.error(f"unknown query ID(s): {', '.join(missing)}; available: {', '.join(sorted(queries))}")

    project = os.getenv("GCP_PROJECT_ID", "").strip()
    if not project:
        raise RuntimeError("GCP_PROJECT_ID is missing from the repository-root .env file")

    from google.cloud import bigquery

    location = os.getenv("BIGQUERY_LOCATION", "US").strip()
    client = bigquery.Client(project=project, location=location)
    appendix_path = ROOT / "docs" / "QUERY_APPENDIX.md"
    appendix_blocks: list[str] = []
    for query_id in requested:
        path = queries[query_id]
        sql = path.read_text(encoding="utf-8")
        print(f"\n{query_id}: {path.name}")
        print(f"Business question: {question_for(sql)}")
        started = time.perf_counter()
        job = client.query(
            sql,
            location=location,
            job_config=bigquery.QueryJobConfig(use_query_cache=False),
        )
        rows = list(job.result())
        elapsed_ms = (time.perf_counter() - started) * 1000
        print(json.dumps([row_to_json(row) for row in rows], indent=2, default=str))
        print(
            f"Rows: {len(rows)}; client elapsed: {elapsed_ms:.2f} ms; "
            f"BigQuery bytes processed: {job.total_bytes_processed or 0}"
        )
        if args.appendix:
            appendix_blocks.append(
                "\n".join(
                    [
                        f"## {query_id}: {path.name}",
                        "",
                        f"**Business question:** {question_for(sql)}",
                        "",
                        "```sql",
                        sql.rstrip(),
                        "```",
                        "",
                        "**Captured sample output (first 3 rows):**",
                        "",
                        "```json",
                        json.dumps([row_to_json(row) for row in rows[:3]], indent=2, default=str),
                        "```",
                        "",
                        f"**Captured result:** {len(rows)} rows; client elapsed {elapsed_ms:.2f} ms; "
                        f"BigQuery bytes processed {job.total_bytes_processed or 0}.",
                    ]
                )
            )
    if args.appendix:
        header = (
            "# Query Appendix\n\n"
            f"Warehouse captures run on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}. "
            "Outputs are actual query results from the configured BigQuery and MongoDB instances. "
            "Client elapsed time includes network and job setup; this small pilot makes time comparisons noisy.\n"
        )
        existing = appendix_path.read_text(encoding="utf-8") if appendix_path.exists() else ""
        if not existing.startswith("# Query Appendix"):
            existing = header + "\n" + existing
        blocks = [
            block
            for block in existing.split("\n---\n")
            if not any(block.lstrip().startswith(f"## {query_id}:") for query_id in requested)
        ]
        appendix_path.write_text("\n---\n".join(blocks + ["\n".join(appendix_blocks)]) + "\n", encoding="utf-8")
        print(f"Appended real warehouse captures to {appendix_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
