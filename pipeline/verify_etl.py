"""Print raw-to-curated counts and cleaning totals without exporting event rows."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def main() -> int:
    from google.cloud import bigquery

    project = os.getenv("GCP_PROJECT_ID", "").strip()
    location = os.getenv("BIGQUERY_LOCATION", "US").strip()
    raw_dataset = os.getenv("BIGQUERY_RAW_DATASET", "raw").strip()
    curated_dataset = os.getenv("BIGQUERY_CURATED_DATASET", "curated").strip()
    if not project:
        raise SystemExit("GCP_PROJECT_ID is empty in the repository-root .env file")

    client = bigquery.Client(project=project, location=location)
    raw = f"`{project}.{raw_dataset}"
    curated = f"`{project}.{curated_dataset}"
    queries = [
        (
            "Raw events and duplicate excess",
            f"""SELECT event_type, COUNT(*) AS raw_rows,
                       COUNT(DISTINCT event_id) AS distinct_event_ids,
                       COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_excess
                FROM {raw}.events`
                GROUP BY event_type ORDER BY event_type""",
        ),
        (
            "Ingestion rejects",
            f"""SELECT COUNT(*) AS ingestion_rejected_rows
                FROM {raw}.rejected_events`""",
        ),
        (
            "Curated row counts",
            f"""SELECT 'fact_exposures' AS table_name, COUNT(*) AS row_count FROM {curated}.fact_exposures`
                UNION ALL SELECT 'fact_usage_events', COUNT(*) FROM {curated}.fact_usage_events`
                UNION ALL SELECT 'preprocessing_rejections', COUNT(*) FROM {curated}.preprocessing_rejections`
                ORDER BY table_name""",
        ),
        (
            "Preprocessing rejection reasons",
            f"""SELECT rejection_reason, COUNT(*) AS rejected_rows
                FROM {curated}.preprocessing_rejections`
                GROUP BY rejection_reason ORDER BY rejection_reason""",
        ),
        (
            "Cleaned usage measurements",
            f"""SELECT COUNT(*) AS row_count,
                       COUNTIF(latency_ms IS NULL) AS null_or_invalid_latency,
                       COUNTIF(http_status IS NULL) AS null_or_invalid_http_status,
                       COUNTIF(target IS NULL) AS missing_target_values
                FROM {curated}.fact_usage_events`""",
        ),
    ]
    for title, sql in queries:
        print(f"\n{title}:")
        for row in client.query(sql, location=location).result():
            print("  " + ", ".join(f"{key}={value}" for key, value in row.items()))

    print("\nCurated partition metadata:")
    for table_name in ("fact_exposures", "fact_usage_events"):
        table = client.get_table(f"{project}.{curated_dataset}.{table_name}")
        partition = table.time_partitioning
        print(
            f"  {table_name}: rows={table.num_rows}, "
            f"partition_field={partition.field if partition else 'none'}, "
            f"partition_expiration_ms={partition.expiration_ms if partition else 'none'}"
        )
    print("\nVerification: PASS (queries read BigQuery counts only; no event rows exported).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
