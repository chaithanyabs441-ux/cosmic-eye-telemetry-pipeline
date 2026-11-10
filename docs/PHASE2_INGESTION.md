# Phase 2: event generation and BigQuery ingestion

## What the pipeline does

`pipeline/ingest.py` generates two JSON event types, publishes them to a
temporary Kafka-compatible topic in Redpanda, consumes them, checks the event
envelope, and appends small in-memory batches to `raw.events` and
`raw.rejected_events` in BigQuery. It does not write event history to local
files. The broker topic is unique per run, capped at 100 MiB/one hour, and
deleted when a run exits. Stopping the broker also removes its temporary
container storage.

With `--source irsa`, a bounded query to IRSA's SIA2 endpoint fetches at most 100
SPHEREx QR2 metadata rows for a small sky cone. It reads metadata only; it never
downloads image/FITS products. Exposure event quality metrics and app-usage
events are synthetic. Each exposure sets `is_synthetic=true` because the full
record includes generated metrics, while separately setting
`metadata_is_synthetic=false` and recording NASA/IPAC IRSA, access time, and
which fields are synthetic. `--source synthetic` labels even the observation
metadata as synthetic.

The generator deliberately injects duplicate event IDs, null values,
out-of-range values, wrong metric types, wrong event-envelope types (sent to the
dead-letter table), and late event timestamps. A fixed seed makes generated
values repeatable. A rerun has a new run ID and a unique broker topic.

## Before the first ingest: grant access to `raw`

Phase 1 assigned the service account access only to `phase1_smoke`. The current
credential therefore cannot write to the `raw` dataset yet.

1. Open the Google Cloud Console and select your Sandbox project.
2. Open **BigQuery**, select the project, choose **Create dataset**, enter
   `raw`, use the same location as `BIGQUERY_LOCATION` in `.env` (currently
   `US`), and create it.
3. Open the `raw` dataset's **Sharing / Permissions** panel. Add the service
   account principal created in Phase 1 and grant **BigQuery Data Editor** on
   this dataset only. The project-level **BigQuery Job User** permission from
   Phase 1 is used to run batch-load and verification query jobs.
4. Do not grant project-wide Editor or enable billing. The batch loader creates
   `events` and `rejected_events` tables inside `raw` if missing.

If the service-account email is not known, find it under **IAM & Admin →
Service Accounts**. Keep the downloaded JSON key and email out of chat.

## Run a bounded first batch

1. Start Docker Desktop and the broker from the repository root:

   ```powershell
   ./scripts/start-pipeline.ps1
   ```

2. Use a 100-event pilot first. This produces both event types and all the
   dirty-data cases, with a small public metadata lookup:

   ```powershell
   pipeline\.venv\Scripts\python.exe pipeline\ingest.py --mode batch --count 100 --seed 42 --source irsa
   ```

3. Expected output ends with lines like these. Counts are examples of the
   output format only; use the actual counts printed by your run:

   ```text
   Produced: <actual count>
   Consumed: <actual count>
   Landed in BigQuery <project>.raw.events: <actual count>
   Rejected in BigQuery <project>.raw.rejected_events: <actual count>
   BigQuery batch load jobs: <actual count>
   Counts: PASS (produced = consumed = landed + rejected)
   ```

4. If the IRSA call fails because of network/service availability, repeat the
   command with `--source synthetic`. The output then clearly says it used the
   synthetic metadata source; do not report those metadata as NASA observations.
5. Check the actual rows in BigQuery Console by opening `raw.events` and
   `raw.rejected_events`; the run ID printed in the terminal scopes each row.
   The script also runs count queries and compares them with producer/consumer
   totals before it prints PASS.
6. Stop Redpanda after the pilot:

   ```powershell
   ./scripts/stop-pipeline.ps1
   ```

## Batch scale and live mode

- The initial command-line default is 100 rows and 50 events/second. The
  proposal's demo target is 200 events/second, but first measure actual
  throughput on the PC.
- `--mode batch --count 1000` runs a larger backfill through the same bus and
  loader. Increase in bounded steps only after checking BigQuery Sandbox
  storage. Raw records include both good and rejected rows, and Sandbox's 10
  GiB storage allowance is lifetime and not restored after deletion.
- `--mode live --count 0 --rate 50` runs until Ctrl+C. A finite live exercise
  can use `--mode live --count 100 --rate 50`. In live mode, Ctrl+C flushes
  completed in-memory load batches. For a clean measured run, prefer finite
  counts so produced/consumed/landed totals can be compared exactly.
- The single-node broker topic has one-hour retention and a 100 MiB cap. The
  raw/curated warehouse is authoritative; Redpanda is only a transport buffer.
- Do not start the one-million-row target yet. First review the measured
  average bytes/row and BigQuery storage after the pilot; the query-and-storage
  check is part of reporting the scale that the free Sandbox can actually hold.

## Captured Phase 2 pilot results

These are actual results from this project, not expected values:

| Run | Mode/source | Produced | Consumed | Raw landed | Rejected | Generator/broker rate | End-to-end rate |
|---|---|---:|---:|---:|---:|---:|---:|
| `73cbd180434449d2a42b2e10874c2702` | batch / IRSA | 100 | 100 | 99 | 1 | Not timed by the first version | Not timed by the first version |
| `47b207100c11472fa6ad6983c66f937e` | finite live / synthetic | 20 | 20 | 20 | 0 | Not timed by the first version | 1.62 events/s |
| `21424860f8644ac59bce599a63cb3237` | finite live / synthetic | 100 | 100 | 99 | 1 | 200.00 events/s over 0.500 s | 12.26 events/s |
| `dc2ae2d8feca4696932e70e02faa3fbc` | finite live / synthetic | 200 | 200 | 197 | 3 | 200.00 events/s over 1.000 s | 25.30 events/s |

A separate BigQuery count query confirmed four pilot runs contain **415 raw
rows**, **5 rejected rows**, and both event types. Total pilot volume is 420
messages across the bus, counting the rows from the producer output. The
200-event live burst published at the configured 200 events/second; BigQuery
load and query work took longer, so its full end-to-end rate was 25.30
events/second. Report the generator/broker and warehouse rates separately; do
not claim a sustained 200 events/second through the warehouse. The pilot has
not tested the one-million-row storage target.

Variety measured in the pilot: exposure metadata events, synthetic usage events,
and rejected event envelopes. The IRSA-derived exposure events are marked
`is_synthetic=true` because they include generated quality metrics; their raw
payload preserves `metadata_is_synthetic=false` and IRSA provenance. All 415
landed records are marked synthetic at the full-record level.

## If it fails

- `403` / permission error: confirm the dataset is named `raw`, in the same
  location, and the Phase 1 service account has BigQuery Data Editor on `raw`.
- `BigQuery Sandbox` error: verify that billing remains disabled and the
  project is still in Sandbox mode. This loader uses batch load jobs, not
  streaming inserts or DML.
- Kafka connection refused: start Docker Desktop and the broker, then check
  `docker compose -f infra\compose.yaml ps`.
- IRSA returns no rows: try another nearby sky position only after updating the
  documented query in code, or use `--source synthetic` and describe the exact
  source in your report.
- Share only sanitized error text and the non-secret count output. Never paste
  `.env`, an Atlas URI, a project key, or service-account JSON.
