# Phase 8 — measured scale and warehouse insights

## Scale run

The scale target was at least 1,000,000 historical events. One IRSA-sourced
attempt was stopped before completion because it made no visible progress in
the terminal. A read-only BigQuery check found 160,000 rows under that attempt's
run ID; no throughput is claimed for that partial run. It remains in BigQuery
and is included in the final counts.

The completed continuation used the seeded synthetic source, seed 43, and
10,000-row in-memory BigQuery load batches:

```text
Run ID: 5ee403f102244aaf93a73c8869e987d7
Mode/source: batch/synthetic
Produced: 860000
Consumed: 860000
Landed in raw.events: 845667
Rejected in raw.rejected_events: 14333
BigQuery batch load jobs: 87
Generator/broker publish elapsed: 896.313 seconds
Generator/broker publish rate: 959.49 events/second
End-to-end ingest elapsed: 908.282 seconds
End-to-end ingest rate: 946.84 events/second
Counts: PASS (produced = consumed = landed + rejected)
```

The completed batch measured 946.84 end-to-end events/second. The separate
Phase 2 finite-live pilot measured a 200 events/second producer burst and
25.30 events/second end-to-end. The proposal deliberately used 200 events/second
as the configurable live target to keep the Windows/Docker setup and BigQuery
batch loads within the free Sandbox design; the batch throughput result is not
presented as live dashboard latency. Variety is exposure metadata, synthetic
app-usage events, and rejected/dirty records, retained as separate raw shapes.

## Warehouse row counts and cleanup evidence

`pipeline/verify_etl.py` queried the cloud tables without exporting event rows:

| Measure | Actual result |
|---|---:|
| `raw.events` rows | 1,006,082 |
| `raw.rejected_events` rows | 14,338 |
| `curated.fact_exposures` | 511,456 |
| `curated.fact_usage_events` | 460,310 |
| `curated.preprocessing_rejections` | 17,052 |
| Exposure duplicate excess removed by ETL | 17,165 |
| Usage duplicate excess removed by ETL | 103 |
| dbt models/tests | 22/22 PASS; 6 table models, 1 view, 15 tests |
| Full dbt run time | 54.85 seconds |

The first interrupted run used seed 42; the completed continuation used seed 43.
The ETL deduplicates across both, so the same seeded event IDs are not counted
twice in curated facts. Dirty-value rejections and duplicate excess are kept
separately visible in the warehouse.

## Cloud-service usage against free limits

BigQuery table metadata returned **935,214,301 logical bytes** across raw and
**264,115,523 logical bytes** across curated, **1,199,329,824 bytes total
(about 1.12 GiB)**. This is about 11.2% of the BigQuery Sandbox's documented
10 GiB lifetime storage limit. The largest observed dbt model scans were about
700.9–861.9 MiB; the documented monthly query-processing allowance is 1 TiB.
The Sandbox automatically expires tables/partitions after 60 days. See the
[BigQuery Sandbox limits](REFERENCES.md) and [BigQuery load-job quotas](REFERENCES.md).

Atlas `dbStats` returned 210,248 bytes of logical document data, 229,376 bytes
of collection storage, and 303,104 bytes of indexes across 5 collections and 9
indexes. The storage plus index footprint is about 0.51 MiB, around 0.1% of the
Atlas M0 0.5 GB limit. The four pipeline collections contain 161 exposure
summaries, 173 usage summaries, 343 live metric summaries, and 33 live alerts.

## Warehouse insights page

Added `dashboard/pages/1_Warehouse_Insights.py`. Start the main dashboard with
`pipeline\\.venv\\Scripts\\python.exe -m streamlit run dashboard\\app.py`; the
new page appears in Streamlit's page navigation. It runs read-only BigQuery
queries and uses built-in Streamlit charts. Results are cached for five minutes
and each query has a 4 GiB bytes-processed guard. Five current, captured
takeaways from BigQuery:

1. SPHEREx-D3 has the most exposures: 91,139; mean synthetic noise sigma is 0.400.
2. `sky_10_10` has the highest mean synthetic noise sigma (0.425) among regions with at least two exposures.
3. The preview action has the highest p95 latency: 2,377 ms.
4. Skymap has the largest count of 4xx/5xx responses: 9,234.
5. `sky_14_09` has the most combined activity: 68,188 exposures and 716 synthetic app events.

Quality metrics and usage events in these charts are synthetic. IRSA metadata
fields keep their source and access-time provenance.

## Verification record

- `pipeline\\.venv\\Scripts\\python.exe pipeline\\ingest.py --mode batch --count 860000 --seed 43 --source synthetic --load-batch-size 10000` — actual run output above; count reconciliation passed.
- `pipeline\\.venv\\Scripts\\python.exe pipeline\\run_etl.py` — `PASS=22 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=22`.
- `pipeline\\.venv\\Scripts\\python.exe pipeline\\verify_etl.py` — actual raw/curated/reject counts above; verification passed.
- PowerShell AppTest invocation — 0 exceptions, 5 insight headings, metric cards `511,456`, `460,310`, and `10`, and the five takeaways above:

  ```powershell
  @'
  from streamlit.testing.v1 import AppTest
  app = AppTest.from_file('dashboard/pages/1_Warehouse_Insights.py', default_timeout=120).run()
  print('App exceptions:', len(app.exception))
  print('Insight headings:', ' | '.join(x.value for x in app.subheader))
  print('Metrics:', ' | '.join(x.value for x in app.metric))
  '@ | pipeline\.venv\Scripts\python.exe -
  ```
- Temporary local dashboard check at port 8502 returned HTTP 200; the normal dashboard health endpoint at port 8501 returned HTTP 200.
- Python compilation and `git diff --check` passed.

## Limits and interpretation

The 860,000-event continuation ran for about 15 minutes on the Windows PC and
landed data directly in BigQuery using Redpanda as a bounded transport buffer.
This is a measured course-scale batch, not a measured one-million-events-per-
second streaming claim. The dashboard queries use the existing curated tables;
the live Spark service was paused during bulk ingestion and restarted afterward.
