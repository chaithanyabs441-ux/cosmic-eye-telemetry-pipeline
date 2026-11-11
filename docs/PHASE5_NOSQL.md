# Phase 5: BigQuery summaries to MongoDB Atlas

## Purpose and document design

BigQuery remains the authoritative store for raw events and curated facts.
This job reads compact daily aggregates from the two curated fact tables and
stores those derived summaries in MongoDB. It does not copy event rows,
anonymous session IDs, URLs, or primary-dataset files into MongoDB or onto the
local computer.

| Collection | One document represents | Typical lookup |
|---|---|---|
| `exposure_daily_summaries` | One ingestion date + sky region + wavelength band | Region and band, newest date first; exposure count and quality summaries |
| `usage_daily_summaries` | One ingestion date + sky region + app event name | Region and event type, newest date first; event/error counts and latency summary |
| `pipeline_state` | Last successfully synchronized partition date per source | Resume the next scheduled synchronization safely |

Documents have deterministic `_id` values made from their grouping keys. The
job rereads the most recent processed date and the previous date, then replaces
matching daily documents. This one-day overlap catches late arrivals; reruns
replace the same keys instead of double-counting. BigQuery predicates use the
`ingested_at` partition field, so only recent partitions are read. MongoDB
documents older than the active 59-day Sandbox window are removed.

MongoDB's document model fits these read patterns because a region/band or
region/event summary and its metrics are returned together. A graph database
would add relationship traversal the project does not need; a key-value store
would require packing and updating opaque values; a column-family store would
add schema and cluster work without helping these document lookups. Compound
indexes support the region/band/date and region/event/date filters.

## Run locally on Windows

From PowerShell at the repository root, make sure your existing `.env` has
`GCP_PROJECT_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, `BIGQUERY_LOCATION`,
`MONGODB_URI`, and `MONGODB_DATABASE`. Never paste the URI or service-account
key into chat or commit them.

```powershell
pipeline\.venv\Scripts\python.exe pipeline\sync_mongodb.py
```

The job prints one count line and one sample summary per collection, indexes,
and BigQuery bytes processed. It does not print credentials or user/session
identifiers. Repeat the command to check idempotency: document counts should
remain unchanged, with zero or few documents modified if BigQuery did not
change.

## Run the sync when needed

GitHub Actions runs the scheduled BigQuery ETL on a GitHub-hosted runner. It
does not connect to Atlas: the hosted runner's public IP changes, while Atlas
only accepts connections from addresses on its allow-list. GitHub's public
Meta API returned 8,350 Actions IP ranges on 2026-11-11; Atlas permits at most
200 project IP entries. GitHub advises against using its hosted-runner ranges
as allow-lists. Do not solve this by allowing `0.0.0.0/0`.

The course requirement R4.3 asks us to retrieve warehouse results and load
them into NoSQL; it does not require that MongoDB copy to run on a schedule.
That retrieval is already verified locally. To refresh MongoDB after the
scheduled dbt build, run this in PowerShell from the repository root while the
PC's public IP remains on the Atlas list:

```powershell
pipeline\.venv\Scripts\python.exe pipeline\sync_mongodb.py
```

The BigQuery service-account path and MongoDB URI are read from the local `.env`
file. They are not needed as GitHub repository secrets for the scheduled
workflow. Keep the BigQuery key in the GitHub secret used by the dbt build.

## Verification record

Captured from real runs against the configured BigQuery Sandbox and MongoDB
Atlas account on 2026-11-10:

```text
pipeline\.venv\Scripts\python.exe pipeline\sync_mongodb.py
exposures: BigQuery groups=161; Mongo upserted=161; changed=0; stored documents=161; latest ingestion date=2026-11-10; BigQuery bytes processed=16920
usage: BigQuery groups=173; Mongo upserted=173; changed=0; stored documents=173; latest ingestion date=2026-11-10; BigQuery bytes processed=9345
exposure_daily_summaries indexes: ["_id_", "region_band_day_lookup"]
usage_daily_summaries indexes: ["_id_", "region_event_day_lookup"]
MongoDB sync: PASS (aggregate summaries only; no primary event rows copied)
```

The script printed sample documents for each collection. The exposure sample
contained `region_key=sky_34_12`, `band_id=SPHEREx-D1`, and one exposure; the
usage sample contained `region_key=sky_15_09`, `event_name=browse`, and one
event. They are aggregate summaries, not individual event/session documents.

The repeat run returned the same 161 exposure and 173 usage groups and stored
document counts, with `Mongo upserted=0` and `changed=0` for both collections.
This verifies the stable-key upsert is idempotent for an unchanged source
partition. The two BigQuery aggregate queries processed 16,920 and 9,345 bytes
respectively on both runs.

The user reported a green GitHub-hosted run while Atlas temporarily allowed
`0.0.0.0/0`. Later scheduled runs failed at the Mongo sync with a TLS handshake
error after that wildcard was removed. The workflow now keeps the scheduled
BigQuery ETL on a GitHub-hosted runner and excludes the Mongo sync. The local
warehouse-to-Mongo sync remains verified; scheduled Mongo sync is intentionally
not part of this free, hosted-runner design.
