# Phase 3: BigQuery warehouse design

## Model and layout

- `raw.events` and `raw.rejected_events` preserve the incoming JSON payloads and
  are partitioned by `DATE(ingested_at)`.
- Raw event tables cluster by event type/region/band and rejection reason.
- `curated.dim_band`, `dim_region`, and `dim_date` are shared dimensions.
- `curated.fact_exposures` and `fact_usage_events` are event facts partitioned
  by ingestion date and clustered on region/band plus a query filter. The event
  date remains available in `event_ts` and `dim_date`.
- Phase 4 dbt ETL now populates the curated schema from BigQuery raw tables; no
  dataset files are stored locally.

The full field definitions and units are in [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md).

## Create the curated dataset (completed)

You created the dataset and granted the pipeline service account access on
`curated`. The steps below are retained for a fresh setup. To keep permissions
narrow, grant access only on this dataset:

1. Select your Sandbox project and open **BigQuery**.
2. Choose the project menu → **Create dataset**. Enter `curated`, select the
   same location as `raw` and `BIGQUERY_LOCATION` in `.env` (currently `US`),
   then create it.
3. Open the `curated` dataset's **Sharing / Permissions** panel. Add the same
   pipeline service account and grant **BigQuery Data Editor** on `curated`.
   Its project-level BigQuery Job User role is used to run DDL and audit jobs.
4. Do not enable billing or grant project-wide Editor.

## Apply the schemas

From PowerShell at the repository root, run:

```powershell
pipeline\.venv\Scripts\python.exe pipeline\deploy_warehouse.py --raw-only
```

This verifies the raw DDL and applies clustering metadata to the existing raw
tables. Then, after creating `curated` and granting access, run:

```powershell
pipeline\.venv\Scripts\python.exe pipeline\deploy_warehouse.py
```

It creates the empty curated dimensions/facts and prints each table's actual
row count, stored bytes, partition field, and clustering fields. Existing raw
rows were created before clustering was added; they are small pilot tables.
Future loaded data uses the clustering specification. BigQuery documents that
adding clustering does not automatically reorder existing rows.

## Verification queries

Run the statements in [`infra/sql/03_verify_warehouse.sql`](../infra/sql/03_verify_warehouse.sql)
in BigQuery Console. The script reports table metadata and partition counts,
then lists curated columns. The audit tool also prints actual table row counts,
logical byte totals, partition fields, and cluster fields.

The project-wide `TABLE_STORAGE_BY_PROJECT` query returned no rows even though
the dataset-scoped `INFORMATION_SCHEMA.TABLES` checks listed all seven raw and
curated base tables. The deployment script independently read BigQuery table
metadata and reported 388,747 bytes before ETL. After ETL, use
`pipeline/verify_etl.py` for current curated fact counts and partition metadata.
The project-wide snapshot remains a limitation in the phase evidence.

BigQuery Sandbox currently allows 10 GiB lifetime storage, 1 TiB monthly query
processing, and automatic 60-day expiration. Deleting rows does not restore
lifetime storage, so the one-million-event scale target must be checked against
actual storage before generating it. The curated fact partitions use ingestion
dates because the dataset's observed 60-day partition expiry would otherwise
remove older event-time backfill immediately. The table storage view is a
delayed snapshot, not an immediate bill estimate. [Sandbox limits](https://docs.cloud.google.com/bigquery/docs/sandbox),
[partition metadata](https://docs.cloud.google.com/bigquery/docs/information-schema-partitions),
[table storage metadata](https://docs.cloud.google.com/bigquery/docs/information-schema-table-storage).
