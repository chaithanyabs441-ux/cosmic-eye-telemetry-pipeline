# Phase 4: Preprocessing and scheduled ETL

## What the ETL does

`pipeline/run_etl.py` loads the local `.env` settings and runs `dbt build`.
dbt reads JSON payloads from `raw.events`, parses values into typed columns,
validates event IDs/timestamps and selected numeric types, and creates curated
dimensions and facts. It uses BigQuery CTAS/table rebuilds rather than DML, so
it works within the Sandbox design. The raw tables remain the source of truth.

The workflow:

1. Creates `curated.stg_events` as a view with typed timestamp, numeric,
   wavelength, position, provenance, and anonymous usage fields.
2. Writes invalid schema rows to `curated.preprocessing_rejections`. Basic
   envelope rejects from ingestion remain in `raw.rejected_events`.
3. Removes repeated `event_id` records from the curated facts.
4. Converts wavelength bounds from metres to micrometres in `dim_band`,
   calculates band centre, and creates date and ten-degree sky-cell dimensions.
5. Turns negative or out-of-range measurements into `NULL`; missing target
   strings stay `NULL` instead of being fabricated.
6. Builds the two typed fact tables and runs dbt not-null/unique tests.

The sample raw data had 415 accepted rows. The ETL retained 380 unique valid
fact rows (200 exposures and 180 usage rows), identified 28 duplicate excess
rows and quarantined 7 wrong-type latency rows. Five more malformed messages
were already in the ingestion dead-letter table. These are observed values
from the current BigQuery pilot, not target or fabricated counts.

## Sandbox partition choice

The first build partitioned curated facts on `event_ts`. BigQuery returned
160 exposures and 145 usage rows, although the compiled models selected 200
and 180. Inspection showed the facts had a 5,184,000,000 ms (60-day) partition
expiration; the backfill uses 2025 event timestamps. The raw rows were still
present because raw is partitioned by the 2026 ingestion timestamp.

Curated facts now partition by `ingested_at`, preserving backfilled events
through the Sandbox retention window. `event_ts` and `dim_date` still represent
the observation/application event date for analysis. Two consecutive builds
after the change each produced 200 `fact_exposures` and 180 `fact_usage_events`,
with all 15 dbt data tests passing on both runs. The second build completed in
55.77 seconds, confirming that rebuilding does not append duplicate fact rows.
The 60-day Sandbox expiry still applies; this design does not make Sandbox a
permanent archive.

## Run locally on Windows

From PowerShell at the repository root:

```powershell
pipeline\.venv\Scripts\python.exe pipeline\run_etl.py
```

This runs dbt table builds followed by data-quality tests. Verify the cloud
counts without downloading event rows:

```powershell
pipeline\.venv\Scripts\python.exe pipeline\verify_etl.py
```

The verifier prints raw rows and duplicate excess, ingestion rejects,
curated fact counts, preprocessing rejection reasons, cleaned usage metrics,
and curated partition metadata. The primary data stays in BigQuery.

## Automated GitHub Actions schedule

`.github/workflows/warehouse-etl.yml` runs hourly at minute 17 in the
`Asia/Kolkata` timezone and also supports a manual `workflow_dispatch` for
setup checks. Minute 17 avoids the start of the hour, when GitHub documents
more schedule delays and dropped jobs. Hourly scheduling is also deliberate:
one observed dbt build took about 50 seconds and ran 22 model/test nodes.
Hourly runs keep a private GitHub Free account's included runner-minute
allowance and BigQuery's monthly query-processing allowance safer than a
15-minute full rebuild.

The workflow uses `actions/checkout@v7` and `actions/setup-python@v7` to run
their Node.js 24 action runtimes and avoid GitHub's Node.js 20 deprecation
warning. This version change must be pushed to the default branch before it
affects GitHub-hosted runs.

Before the scheduled run can work:

1. Push this workflow and the dbt files to the repository's **default branch**.
   Scheduled workflows only run from the default branch.
2. In GitHub, open **Settings → Secrets and variables → Actions → New repository
   secret**. Name it `GCP_SERVICE_ACCOUNT_KEY`.
3. On your own PC, open the service-account JSON key in Notepad and paste its
   contents into that GitHub secret field. Save it only in GitHub's secret UI;
   never paste it into chat, commit it, or add it to a normal text file.
4. Confirm the service account has BigQuery Job User on the project and BigQuery
   Data Editor on the `raw` and `curated` datasets.
5. In **Actions**, verify one workflow run completes. The user reports that a
   manual `workflow_dispatch` run succeeds. Two successful runs whose event is
   `schedule` have not been observed yet; those are still required evidence for
   R4.4-b, but the user has chosen to continue other project phases while this
   schedule check remains open. A manual run does not count as scheduled proof.

The workflow writes the secret to a temporary runner file, runs dbt, and removes
that file even if the build fails. The JSON key stays out of the repository.

## Evidence still needed

The local dbt build and all 15 data tests passed. The first GitHub run rejected
the workflow before starting a job because `runner.temp` was referenced in
job-level `env`, where that context is unavailable. The workflow now references
`runner.temp` only in step-level environments; GitHub's context rules allow it
there. The user reports that manual GitHub runs now succeed. The deprecation
warning is addressed in the workflow source by upgrading checkout and setup-
python to v7; it will be verified on GitHub after that edit is pushed. Phase 4
remains PARTIAL because two successful scheduled runs have not been observed.
The user has elected to proceed with later phases while this evidence remains
open; it must be revisited before the final checklist is complete.
