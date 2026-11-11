# Course Requirements Checklist

**Overall status:** PARTIAL - Phase 9 documentation is complete, but student names/screenshots remain, two successful automatic schedule runs still need explicit evidence, and free-tier usage/teardown requirements continue through the viva. GitHub-hosted ETL runs BigQuery only; Atlas sync is verified locally.

| Requirement ID | What will satisfy it | File / query / evidence | Status |
|---|---|---|---|
| G-TEAM | Exactly two students; both can explain all stages | `docs/PROPOSAL.md`, `docs/FINAL_REPORT.md`, `docs/PRESENTATION_OUTLINE.md`; names and two-member rehearsal remain student actions | Partial |
| G-FREE | Free tiers only; monitor usage; tear down resources | Proposal and Phase 1/2 guides; Sandbox, Atlas, and local broker tested; no billing account linked | Partial - monetary GCP budget alert unavailable in billing-disabled Sandbox; teardown remains |
| G-CITE | Cite datasets, libraries, and external resources | `docs/REFERENCES.md`; report links references and query appendix | PASS |
| G-PRIVACY | No real personal/sensitive app-usage data | `docs/PROPOSAL.md`; generator uses anonymous synthetic sessions and explicit event provenance | PASS by schema/design |
| R4.1-a | Use synthetic records and/or replay public data | `pipeline/ingest.py`; bounded IRSA QR2 metadata run and explicitly synthetic usage/quality records | PASS |
| R4.1-b | Generate in batch and continuous modes | `pipeline/ingest.py`; batch and finite-live runs both passed count checks | PASS |
| R4.1-c | State volume, velocity, variety | `docs/PROPOSAL.md`, `docs/PHASE2_INGESTION.md`, and Phase 8 scale run: 1,006,082 raw event rows; completed continuation measured 946.84 events/s end-to-end; exposure, usage, reject shapes | PASS for measured course-scale run; higher continuous-live target remains a separate demo limit |
| R4.2-a | Store raw and curated data only in a cloud warehouse | `raw.events`, `raw.rejected_events`, and all five curated star-schema tables are in BigQuery; Phase 4 dbt fills curated tables | PASS |
| R4.2-b | Do not store the primary dataset locally | Generator publishes to temporary Redpanda topics; loader uses in-memory batches; 1,006,082 rows loaded to BigQuery without local event history files | PASS by implementation and scale run |
| R4.2-c | Verify free-tier limits and use an eligible free option | `docs/REFERENCES.md`; successful Sandbox smoke test and BigQuery pilot loads | PASS for Sandbox use and documented limits |
| R4.3-a | Retrieve warehouse results into at least one NoSQL database | `pipeline/sync_mongodb.py`; BigQuery daily summary groups 161 exposures + 173 usage; Mongo stored exactly 161 + 173 documents | PASS |
| R4.3-b | Justify NoSQL type by access patterns | `docs/PHASE5_NOSQL.md`; compact region/band and region/event documents with compound indexes | PASS |
| R4.4-a | Build ETL with dbt/Spark/Airflow/Glue or equivalent | `pipeline/dbt/`; local `dbt build`: 6 table models, 1 view, 15 tests; 22/22 nodes successful | PASS |
| R4.4-b | Run ETL automatically or on a schedule | `.github/workflows/warehouse-etl.yml` schedules BigQuery dbt hourly on GitHub-hosted `ubuntu-latest`; user reports a passing run after fix, but two successful `schedule` event runs are not independently captured in repo evidence | Partial |
| R4.5 | Clean, deduplicate, validate schema, handle missing/noisy values, convert formats, and engineer fields | `pipeline/dbt/models/`; `pipeline/verify_etl.py`; scale run: 17,268 duplicate excess removed, 14,338 ingestion rejects, 17,052 preprocessing rejects, 511,456 exposure and 460,310 usage facts; 15 dbt tests pass | PASS at scale; streaming late-event watermark is Phase 6 |
| R4.6-a | Implement a streaming layer | Docker logs: Spark Structured Streaming started on `cosmic-eye-live`; three queries active; Redpanda healthy | PASS |
| R4.6-b | Demonstrate windows and alerts/filters | Atlas verification: 42 event-count summaries, 180 usage-window summaries, 12 exposure alerts, 10 usage alerts; 2-minute watermark in Spark logs | PASS |
| R4.6-c | Show streaming results on a live dashboard/output | `Invoke-WebRequest http://localhost:8501/_stcore/health` returned HTTP 200; Streamlit displays Atlas metrics/alerts with 5-second refresh | PASS |
| R4.7-a | At least 8 analytical warehouse SQL queries covering required patterns | `queries/warehouse/q01`–`q08`; all eight ran live in BigQuery with real sample rows, bytes, and client elapsed time in `docs/QUERY_APPENDIX.md` | PASS |
| R4.7-b | At least 5 NoSQL queries, including indexed query | `queries/nosql/run_queries.py`; six Atlas queries ran; n06 `explain` used `region_band_day_lookup` and examined 1 doc/key | PASS |
| R4.7-c | At least 3 continuous stream queries, including window and alert/filter | `pipeline/streaming.py`, `queries/streaming/README.md`; live verification: 63 event-count summaries, 280 latency-window summaries, 18 exposure alerts, 15 usage alerts | PASS |
| R4.7-d | Optimization with before/after execution times | `queries/nosql/run_optimization.py`; 15 iterations: collection scan median 20.94 ms / 161 docs, index median 20.68 ms / 1 doc; results equal; appendix records small-pilot timing limitation | PASS (index efficiency shown; no material wall-time gain claimed) |
| R4.7-e | Document every query with question, text, sample output, and timing; scripts in GitHub | `docs/QUERY_APPENDIX.md` contains captured SQL, MongoDB, and streaming outputs/timing; query scripts pushed to `main` in commit `1e7c3c7` | PASS |
| R4.7-f | Run and explain queries live | `queries/README.md` gives commands; `queries/run_query.py q01` and `all` and Mongo runner were executed live | PASS for runnable demo; team practice remains |
| R6-a | Proposal with problem, architecture, tools, dataset | `docs/PROPOSAL.md`; implementation, project ID, hardware, and GitHub URL recorded | PASS; fill student names |
| R6-b | Working GitHub implementation with README/setup instructions | GitHub repository; root README links `docs/PIPELINE_README.md`; setup and run docs exist | PASS for published code and instructions |
| R6-c | Final report with results and learnings | `docs/FINAL_REPORT.md`; measured results and limitations recorded; student names/screenshots marked | Partial until student additions |
| R6-d | Live end-to-end demo and presentation | `docs/DEMO_SCRIPT.md`, `docs/PRESENTATION_OUTLINE.md`; dashboard and stream previously live-verified; final rehearsal/screenshots remain | Partial until rehearsal evidence |
| R6-e | Optional demo video | Optional; not required and excluded from planned scope | Not required |
| R7-ARCH | Prioritize architecture and design (20%) | `docs/PROPOSAL.md`; implemented architecture and current trade-offs are documented | PASS; team names remain |
| R7-IMPL | Complete all pipeline stages (25%) | Phases 1-8 implemented and measured; GitHub scheduled-run evidence remains partial under R4.4-b | Partial |
| R7-STREAM | Prioritize real-time streaming (20%) | Spark Structured Streaming, Redpanda, MongoDB live metrics/alerts, and Streamlit dashboard were run and verified in Phase 6/7 | PASS |
| R7-SCALE | Report scale/volume/technical depth (10%) | `docs/PHASE8_SCALE_INSIGHTS.md`: 1,006,082 BigQuery raw events; 946.84 events/s completed-run rate; 1.12 GiB raw+curated; Atlas 0.51 MiB data+indexes | PASS |
| R7-INSIGHTS | Provide useful insights and visualization (10%) | `dashboard/pages/1_Warehouse_Insights.py`; five live BigQuery charts and captured takeaways in `docs/PHASE8_SCALE_INSIGHTS.md` | PASS |
| R7-REPORT | Complete report (10%) | `docs/FINAL_REPORT.md`, `docs/QUERY_APPENDIX.md`, `docs/REFERENCES.md`; add team names/screenshots | Partial until student additions |
| R7-DEMO | Prepare presentation/demo (5%) | `docs/PRESENTATION_OUTLINE.md`, `docs/DEMO_SCRIPT.md`; both-member rehearsal not yet evidenced | Partial |

## Phase 1 evidence

| Phase 1 item | Evidence | Status |
|---|---|---|
| Local environment variables and secret/data ignore rules | `.env.example`, `.gitignore` | Prepared; user must keep real values local |
| Redpanda broker and start/stop scripts | `infra/compose.yaml`, PowerShell start and stop scripts; Docker container started for Phase 2 | Prepared; start verified |
| Cloud, database, Java, Python, Docker, GitHub instructions | `docs/PHASE1_SETUP.md` | BigQuery/Atlas/broker smoke verified; Java/GitHub setup not verified |
| BigQuery/MongoDB/Redpanda smoke checks | `pipeline/smoke_test.py`; user supplied `Smoke tests: 3/3 PASS` | PASS |
| Automated scheduler | GitHub Actions design in proposal; actual ETL workflow deferred until Phase 4 code exists | Not implemented in Phase 1 |

## Phase 2 evidence

| Phase 2 item | Evidence | Status |
|---|---|---|
| Batch/live generator, provenance, fixed seed, dirty records | `pipeline/ingest.py`; in-memory generator preview plus finite-live BigQuery runs | PASS |
| Public SPHEREx metadata source | IRSA SIA2 QR2 run `73cbd180434449d2a42b2e10874c2702`; 100 produced, 99 raw landed, 1 rejected | PASS; metadata only, no image products downloaded |
| Redpanda-to-BigQuery loader and dead-letter table | `pipeline/ingest.py`; pilot rows in `raw.events` and `raw.rejected_events` | PASS |
| Producer/consumer/warehouse count comparison | Per-run output and independent BigQuery count query; across four runs: 420 produced, 420 consumed, 415 landed, 5 rejected | PASS |
| Pilot VVV measurement | `docs/PHASE2_INGESTION.md`; 200 events/s publish rate over a 1-second burst; 25.30 events/s end-to-end for that run; exposure, usage, and rejected shapes | PASS; high-volume target pending |
| No local event history | Generator sends directly to Redpanda; loader uses RAM batches; run topics deleted at exit; data-file formats ignored | PASS by implementation |

## Phase 3 evidence

| Phase 3 item | Evidence | Status |
|---|---|---|
| Raw BigQuery layout | `infra/sql/01_raw_tables.sql`, `pipeline/deploy_warehouse.py`; `raw.events`: 415 rows / 385,408 logical bytes; `raw.rejected_events`: 5 rows / 3,339 logical bytes | PASS; both tables partitioned on ingestion date and clustered |
| Raw partitions | BigQuery `INFORMATION_SCHEMA.PARTITIONS`: `events` partition `20261110`, 415 rows / 385,408 bytes; `rejected_events` partition `20261110`, 5 rows / 3,339 bytes | PASS |
| Curated star schema | `infra/sql/02_curated_star_schema.sql`, `docs/DATA_DICTIONARY.md`; deployment output confirms five tables, then Phase 4 ETL populated them | PASS |
| Curated dataset access | `pipeline/deploy_warehouse.py` successfully applied curated DDL | PASS; user granted the required dataset access |
| Storage snapshot | Visible raw table total 388,747 bytes (0.000362049 GiB); project-wide `TABLE_STORAGE_BY_PROJECT` denied to service account; owner query in `infra/sql/03_verify_warehouse.sql` | Partial; project-wide total pending |
| R4.2-a raw and curated cloud storage | Raw tables and five curated star-schema tables are deployed in BigQuery; Phase 4 facts are populated | PASS |

## Phase 4 evidence

| Phase 4 item | Evidence | Status |
|---|---|---|
| dbt project/profile parse | `pipeline\\.venv\\Scripts\\python.exe pipeline\\run_etl.py --parse-only` → `ETL project parse: PASS` | PASS |
| BigQuery raw-to-curated build | `pipeline\\.venv\\Scripts\\python.exe pipeline\\run_etl.py` → two corrected-partition builds passed `22/22` nodes each; 6 table models, 1 view, 15 tests; second build completed in 55.77 seconds with unchanged fact counts | PASS |
| Deduplication and schema validation | `pipeline\\verify_etl.py` → exposure 219 rows/200 distinct (19 duplicates); usage 196/187 (9 duplicates); 7 `latency_wrong_type` rejections | PASS |
| Curated data | `pipeline\\verify_etl.py` → 200 exposure facts + 180 usage facts; 7 out-of-range latencies represented as NULL; all current fact partition timestamps are `ingested_at`, with 60-day expiration | PASS |
| Automated schedule | `.github/workflows/warehouse-etl.yml` hourly GitHub-hosted dbt build and data-quality tests; temporary GCP key cleanup | Workflow update must be pushed and two successful scheduled runs captured |

## Phase 5 evidence

| Phase 5 item | Evidence | Status |
|---|---|---|
| Incremental and idempotent BigQuery-to-MongoDB retrieval | `pipeline/sync_mongodb.py`; one-day overlap and stable partition/group `_id`; repeat run upserted 0 and changed 0 docs | PASS |
| Exposure summaries | BigQuery returned 161 daily region/band groups; Mongo contains 161 summary documents | PASS |
| Usage summaries | BigQuery returned 173 daily region/event groups; Mongo contains 173 summary documents | PASS |
| Mongo indexes and sample documents | `docs/PHASE5_NOSQL.md`; indexes `region_band_day_lookup`, `region_event_day_lookup`; sample aggregate docs captured | PASS |
| BigQuery-to-Mongo retrieval | `pipeline/sync_mongodb.py`; locally verified against BigQuery and Atlas. Hosted Actions schedule excludes this step because Atlas cannot allow-list the changing hosted-runner IP | PASS for retrieval; not scheduled |

## Phase 6 evidence

| Phase 6 item | Evidence | Status |
|---|---|---|
| Spark Structured Streaming from Redpanda | Docker logs report 3 active queries; 40-event bursts consumed; `pipeline/verify_streaming.py` confirms Atlas writes | PASS |
| Tumbling/sliding windows and watermark | Atlas contains 42 event-count and 180 usage-window summaries; Spark log confirms 2-minute watermark | PASS |
| Event-triggered alert and live dashboard | Atlas contains 12 exposure and 10 usage alerts; Streamlit health endpoint returned HTTP 200 | PASS |
| One-command demo and shutdown | `scripts/demo_streaming.ps1` published 40 synthetic events and opened the dashboard; stop script is available | PASS |

## Phase 7 evidence

| Phase 7 item | Evidence | Status |
|---|---|---|
| Eight warehouse analytics queries | `pipeline\\.venv\\Scripts\\python.exe queries\\run_query.py all`; actual output from all eight queries in `docs/QUERY_APPENDIX.md`, including question, SQL, sample rows, returned row count, client time, and bytes processed | PASS |
| Six MongoDB query patterns and indexed query | `pipeline\\.venv\\Scripts\\python.exe queries\\nosql\\run_queries.py` â†’ `MongoDB queries: PASS`; n06 `explain`: `region_band_day_lookup`, 1 document examined, 1 key examined, 1 returned | PASS |
| Three continuous streaming query definitions and live output | `queries/streaming/README.md`; `pipeline\\.venv\\Scripts\\python.exe pipeline\\verify_streaming.py` â†’ 343 metric documents (63 + 280) and 33 alerts (18 + 15) | PASS |
| Optimization comparison | `pipeline\\.venv\\Scripts\\python.exe queries\\nosql\\run_optimization.py` â†’ 15 iterations; collection scan median 20.94 ms / 161 documents examined; compound index median 20.68 ms / 1 document examined; same one result | PASS; wall-time difference is within network noise at pilot scale |
| Query appendix | `docs/QUERY_APPENDIX.md`; live samples and timings captured for warehouse and Atlas queries; streaming query sample counts and observed batch timing documented | PASS for local appendix |
| Query scripts available for GitHub | `git push origin main` â†’ `c8b6eea..1e7c3c7 main -> main`; query files and appendix included; `actions-runner/` was not included | PASS |
| Live query demo instructions | `queries/README.md`; run one or all warehouse queries, all Mongo queries, or inspect current streaming state | PASS |

## Phase 8 evidence

| Phase 8 item | Evidence | Status |
|---|---|---|
| Scale volume | BigQuery metadata: `raw.events=1,006,082`, `raw.rejected_events=14,338`; accepted plus rejected rows total 1,020,420; `docs/PHASE8_SCALE_INSIGHTS.md` records the interrupted partial attempt transparently | PASS for ≥1M historical raw event rows |
| Measured ingestion rate | Completed run ID `5ee403f102244aaf93a73c8869e987d7`: 860,000 produced/consumed; 845,667 landed + 14,333 rejected; 87 load jobs; 959.49 publish events/s and 946.84 end-to-end events/s | PASS |
| Preprocessing and ETL at scale | `pipeline\\.venv\\Scripts\\python.exe pipeline\\run_etl.py` â†’ 22/22 nodes successful in 54.85s; `pipeline\\verify_etl.py` â†’ 511,456 exposures, 460,310 usage facts, 17,052 preprocessing rejects | PASS |
| BigQuery usage against free limit | Read-only table metadata: raw 0.871 GiB + curated 0.246 GiB = about 1.12 GiB versus Sandbox 10 GiB lifetime storage; dbt model jobs reported 700.9–861.9 MiB scans; reference docs record 1 TiB monthly query allowance | PASS |
| MongoDB Atlas usage against free limit | `dbStats`: 210,248 data bytes, 229,376 storage bytes, 303,104 index bytes; 5 collections and 9 indexes; pipeline collection counts 161, 173, 343, 33 | PASS |
| Five business-insight visualizations | `dashboard\\pages\\1_Warehouse_Insights.py`; AppTest: 0 exceptions, 5 chart headings, actual metrics 511,456 exposures / 460,310 app events / 10 active regions, and five data-derived takeaways recorded in `docs/PHASE8_SCALE_INSIGHTS.md` | PASS |
| Dashboard and stream resumed | Temporary insights app HTTP 200; main dashboard health HTTP 200; Redpanda healthy and Spark container restarted after the scale run | PASS |


## Phase 9 evidence

| Phase 9 item | Evidence | Status |
|---|---|---|
| README and pipeline guide | Root `README.md` links `docs/PIPELINE_README.md`; the guide links architecture, setup, phase reports, queries, and references | PASS |
| Final report | `docs/FINAL_REPORT.md` covers problem, architecture, provenance, design, free limits, results, lessons, and limitations; team names/screenshots marked for the students | Partial until manual additions |
| Presentation and demo | `docs/PRESENTATION_OUTLINE.md`, `docs/DEMO_SCRIPT.md`, optional `docs/DEMO_VIDEO_STORYBOARD.md` | PASS for documents; rehearsal remains |
| Proposal and status audit | `docs/PROPOSAL.md` updated with deployed project/repository and current architecture; workflow, phase docs, handoff notes, and checklist reviewed | PASS |
| Automatic schedule evidence | User reports a passing run after the workflow fix; private Actions history could not be inspected here (`gh` CLI unavailable). Two successful `schedule` event runs are still required | Partial |
