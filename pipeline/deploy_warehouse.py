"""Apply Phase 3 BigQuery DDL after datasets exist and print actual metadata."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def main() -> int:
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    project = os.getenv("GCP_PROJECT_ID", "").strip()
    location = os.getenv("BIGQUERY_LOCATION", "US").strip()
    if not project:
        raise SystemExit("GCP_PROJECT_ID is empty in the repository-root .env file")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-only", action="store_true", help="apply and audit raw DDL only")
    args = parser.parse_args()

    client = bigquery.Client(project=project, location=location)
    required = ["raw"] if args.raw_only else ["raw", "curated"]
    for dataset_name in required:
        try:
            dataset = client.get_dataset(f"{project}.{dataset_name}")
        except NotFound:
            raise SystemExit(
                f"Dataset '{dataset_name}' is missing. Create it in BigQuery Console "
                f"at location {location}, then grant this service account BigQuery Data Editor "
                "on that dataset only."
            )
        if dataset.location.upper() != location.upper():
            raise SystemExit(
                f"Dataset '{dataset_name}' is in {dataset.location}; .env requests {location}. "
                "BigQuery datasets in different locations cannot be joined."
            )

    scripts = [ROOT / "infra" / "sql" / "01_raw_tables.sql"]
    if not args.raw_only:
        scripts.append(ROOT / "infra" / "sql" / "02_curated_star_schema.sql")
    for path in scripts:
        job = client.query(path.read_text(encoding="utf-8"), location=location)
        job.result()
        print(f"DDL applied: {path.relative_to(ROOT)}")

    # Apply clustering metadata to the already populated raw tables as well.
    cluster_specs = {
        "raw.events": ["event_type", "region_key", "band_id"],
        "raw.rejected_events": ["rejection_reason"],
    }
    for short_id, fields in cluster_specs.items():
        table = client.get_table(f"{project}.{short_id}")
        if table.clustering_fields != fields:
            table.clustering_fields = fields
            table = client.update_table(table, ["clustering_fields"])
        partition = table.time_partitioning.field if table.time_partitioning else None
        print(
            f"{short_id}: rows={table.num_rows}, logical_bytes={table.num_bytes}, "
            f"partition={partition}, cluster={','.join(table.clustering_fields or [])}"
        )

    names = ["events", "rejected_events"]
    if not args.raw_only:
        names += ["dim_band", "dim_region", "dim_date", "fact_exposures", "fact_usage_events"]
    total_bytes = 0
    for dataset_name, table_names in (("raw", names[:2]), ("curated", names[2:])):
        for name in table_names:
            table_id = f"{project}.{dataset_name}.{name}"
            try:
                table = client.get_table(table_id)
            except NotFound:
                continue
            total_bytes += table.num_bytes or 0
            part = table.time_partitioning.field if table.time_partitioning else "none"
            print(
                f"{dataset_name}.{name}: rows={table.num_rows}, bytes={table.num_bytes}, "
                f"partition={part}, cluster={','.join(table.clustering_fields or []) or 'none'}"
            )
    print(f"Visible raw/curated logical bytes: {total_bytes}")
    print(f"Approximate GiB in these tables: {total_bytes / (1024**3):.9f}")
    print("Sandbox storage allowance: 10 GiB lifetime; table bytes are a project snapshot, not a billing estimate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
