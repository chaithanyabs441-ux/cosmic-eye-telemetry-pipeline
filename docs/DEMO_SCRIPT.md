# Live Demo Script

**Target time:** 10–12 minutes. Use a small, finite event batch. The one-million
scale ingestion already exists in BigQuery; do not rerun it for the live demo.

## Before the demo (5–10 minutes)

1. Start Docker Desktop and wait for its Linux engine to report running.
2. From PowerShell at the repository root, check available memory:

   ```powershell
   Get-CimInstance Win32_OperatingSystem | Select-Object @{N='FreeGiB';E={[math]::Round($_.FreePhysicalMemory / 1MB, 2)}}
   ```

   Leave at least 4 GiB free before starting Redpanda/Spark. If memory is
   lower, close other heavy applications or do the query/warehouse portion
   only.

3. Confirm the current PC public IP is in the MongoDB Atlas project access list.
   Keep access restricted to the current address if possible. Confirm the
   local `.env` is present but do not show or paste it.
4. Open GitHub **Actions → BigQuery raw-to-curated ETL**. Take a screenshot of
   two green runs only if both run entries say **Scheduled**. Do not label a
   manual run as scheduled proof.
5. Open BigQuery Studio in another tab and select project `cosmic-eye-telemetry-demo`.

## Demo order

### 1. Show the data path (1 minute)

Explain that the generator emits exposure and app usage JSON events, Redpanda
temporarily carries them, and BigQuery stores raw and curated history. Quality
metrics and app telemetry are synthetic; public IRSA metadata has separate
provenance.

Show `docs/PIPELINE_README.md`'s Mermaid architecture.

### 2. Show raw and curated BigQuery data (1 minute)

Run in PowerShell:

```powershell
pipeline\.venv\Scripts\python.exe pipeline\verify_etl.py
```

Expected current scale evidence includes 1,006,082 raw accepted events,
14,338 ingestion rejects, 511,456 exposure facts, and 460,310 usage facts.
Small changes can occur when scheduled ETL refreshes; state the numbers actually
printed. This command reads counts and aggregates only; it does not download
event rows.

### 3. Run one warehouse query (1 minute)

```powershell
pipeline\.venv\Scripts\python.exe queries\run_query.py q01
```

Explain the join from the exposure fact to `dim_band`, grouping by wavelength
band, and averaging synthetic noise. Point to the actual query result and
BigQuery bytes/time printed in the console.

### 4. Show MongoDB NoSQL and index (1 minute)

```powershell
pipeline\.venv\Scripts\python.exe queries\nosql\run_queries.py
```

Explain a region/band document lookup and the `region_band_day_lookup` index.
The indexed `explain` output should show the index scan and one document
examined for the current captured dataset. If Atlas times out, skip this step
and show the captured, credential-free sample in `docs/QUERY_APPENDIX.md`.

### 5. Start the live streaming dashboard (5–6 minutes)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\demo_streaming.ps1
```

The script starts Redpanda/Spark/Streamlit and launches a five-minute synthetic
incident scenario. The first two minutes are normal baseline traffic; from
minute 2 to minute 4, `sky_23_00` gets three times its normal event volume,
3–4× usage latency, and about 25% HTTP errors; the last minute shows recovery.
Open `http://localhost:8501` while it runs. Keep the dashboard time range at
15 or 30 minutes and auto-refresh on.

Point to each chart as the scenario runs:

1. **Events per minute by band:** bands are deliberately uneven (D1 40%, D2
   30%, D3 20%, D4 10%). The incident adds extra events for one region, so the
   total counts rise during minutes 2–4.
2. **Average and p95 latency:** the average is weighted by request count; the
   p95 line is the highest regional p95. Both should climb for windows
   containing the incident.
3. **HTTP error rate:** point out the error-rate spike during the incident.
4. **Alerts by reason:** `http_error` and `high_latency` should increase in the
   incident window; normal traffic has only occasional alerts.
5. **Alerts over time:** show the per-minute alert spike during minutes 2–4.
6. **Busiest sky regions:** `sky_23_00` should rise to the top during the
   incident, then stop receiving the extra events during minute 4–5.

The five-second dashboard refresh and streaming windows are demonstrations,
not a production latency promise.

If everything is already started and running, avoid starting a second stream
job. Open `http://localhost:8501` and run the exact incident command:

```powershell
pipeline\.venv\Scripts\python.exe pipeline\live_producer.py --rate 50 --duration 300 --regions 12 --scenario incident --incident-region sky_23_00 --seed 42
```

To keep generating until you stop it yourself, use `--duration 0` instead of
`--duration 300`; press Ctrl+C in the producer window when finished. The
dashboard keeps showing saved results after the producer stops.

### 6. Show query optimization and results (1 minute)

Show the optimization table in `docs/QUERY_APPENDIX.md`: collection scan
examined 161 documents; the compound index examined 1. Median elapsed time was
nearly equal because the collection is small and Atlas network time dominates.

### 7. Close with limitations and cleanup (1 minute)

State that BigQuery Sandbox currently holds about 1.12 GiB raw plus curated
logical storage and automatically expires tables/partitions after 60 days.
The GitHub Actions schedule can be delayed. Atlas transfer is local, so the PC
needs to be on for that local command and for the local live dashboard; the
hosted BigQuery ETL itself does not require the PC.

## Stop after the live demo

Stop Spark and Streamlit while leaving Redpanda up only if another course demo
is imminent:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\stop_streaming_demo.ps1
```

Stop the remaining local broker when finished:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\stop-pipeline.ps1
```

Do **not** delete BigQuery datasets, the Atlas cluster, GitHub secrets, or the
cloud project until the final viva/demo is complete and the student explicitly
chooses to tear them down.

## Internet or service failure fallback

- If IRSA is unavailable, do not fetch it live during the presentation. Explain
  the retained provenance fields and use the captured BigQuery counts.
- If Atlas is unavailable, show MongoDB query outputs and `explain` evidence in
  `docs/QUERY_APPENDIX.md`.
- If Docker/Redpanda/Spark cannot start, show the captured streaming counts and
  alert sample in the appendix and present the Spark code in `pipeline/streaming.py`.
- If BigQuery is unavailable, use actual saved outputs in the appendix and say
  that live warehouse access failed. Do not claim saved output is a current
  execution.
- If the GitHub Actions workflow has not run automatically twice, explicitly
  mark R4.4-b as still awaiting evidence; manual runs do not replace scheduled
  run proof.

## Query list for viva/demo

- `q01`: join exposure facts to wavelength bands and compare counts/noise.
- `q02`: regions passing a minimum-count `HAVING` condition.
- `q03`: cumulative daily exposure total with a running window.
- `q04`: moving average of noise by band.
- `q05`: rank regions per day.
- `q06`: request latency and error summary by synthetic app action.
- `q07`: join exposure and app activity by date/sky region.
- `q08`: app requests slower than the overall average.
- Mongo `n01`–`n06`: filters, projections, group pipelines, ID lookup,
  recent alerts, and indexed lookup/`explain`.
- Streaming `s01`–`s03`: one-minute tumbling count, five-minute sliding request
  metrics, and per-event threshold alerts.
