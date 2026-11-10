# Course Requirements Checklist

**Phase 4 status:** PARTIAL - dbt ETL and 15 data tests pass in BigQuery; GitHub initially rejected the workflow before a job started. The runner-temp reference is fixed locally; push it and provide two successful scheduled runs. The project-wide storage view still returns no rows.

| Requirement ID | What will satisfy it | File / query / evidence | Status |
|---|---|---|---|
| G-TEAM | Exactly two students; both can explain all stages | `docs/PROPOSAL.md` team fields are placeholders; names still needed | Planned |
| G-FREE | Free tiers only; monitor usage; tear down resources | Proposal and Phase 1/2 guides; Sandbox, Atlas, and local broker tested; no billing account linked | Partial - monetary GCP budget alert unavailable in billing-disabled Sandbox; teardown remains |
| G-CITE | Cite datasets, libraries, and external resources | `docs/REFERENCES.md` | Draft |
| G-PRIVACY | No real personal/sensitive app-usage data | `docs/PROPOSAL.md` synthetic anonymous sessions and provenance design | Planned |
| R4.1-a | Use synthetic records and/or replay public data | `pipeline/ingest.py`; bounded IRSA QR2 metadata run and explicitly synthetic usage/quality records | PASS |
| R4.1-b | Generate in batch and continuous modes | `pipeline/ingest.py`; batch and finite-live runs both passed count checks | PASS |
| R4.1-c | State volume, velocity, variety | `docs/PROPOSAL.md` and pilot measurements in `docs/PHASE2_INGESTION.md` | PASS for stated targets and pilot evidence; million-row scale remains pending |
| R4.2-a | Store raw and curated data only in a cloud warehouse | `raw.events`, `raw.rejected_events`, and all five curated star-schema tables are in BigQuery; Phase 4 dbt fills curated tables | PASS |
| R4.2-b | Do not store the primary dataset locally | Generator publishes to temporary Redpanda topics; loader uses in-memory batches; no local history files | PASS by implementation; full-scale run pending |
| R4.2-c | Verify free-tier limits and use an eligible free option | `docs/REFERENCES.md`; successful Sandbox smoke test and BigQuery pilot loads | PASS for Sandbox use and documented limits |
| R4.3-a | Retrieve warehouse results into at least one NoSQL database | `docs/PROPOSAL.md` scheduled BigQuery-to-Atlas retrieval | Planned |
| R4.3-b | Justify NoSQL type by access patterns | `docs/PROPOSAL.md` compact region/observation/alert documents in MongoDB | Draft |
| R4.4-a | Build ETL with dbt/Spark/Airflow/Glue or equivalent | `pipeline/dbt/`; local `dbt build`: 6 table models, 1 view, 15 tests; 22/22 nodes successful | PASS |
| R4.4-b | Run ETL automatically or on a schedule | `.github/workflows/warehouse-etl.yml`; first GitHub attempt failed workflow validation before starting a job; step-scoped runner-temp fix is local, awaiting push and two successful `schedule` run records | Partial |
| R4.5 | Clean, deduplicate, validate schema, handle missing/noisy values, convert formats, and engineer fields | `pipeline/dbt/models/`; `pipeline/verify_etl.py`; 415 raw accepted → 28 duplicate excess removed, 7 schema rejections, 380 curated facts; 7 out-of-range latencies nulled, 8 missing targets kept null; 15 tests pass | PASS for pilot evidence; streaming late-event watermark is Phase 6 |
| R4.6-a | Implement a streaming layer | `docs/PROPOSAL.md` local Redpanda broker | Planned |
| R4.6-b | Demonstrate windows and alerts/filters | `docs/PROPOSAL.md` three PySpark Structured Streaming queries | Planned |
| R4.6-c | Show streaming results on a live dashboard/output | `docs/PROPOSAL.md` local Streamlit dashboard | Planned |
| R4.7-a | At least 8 analytical warehouse SQL queries covering required patterns | `queries/warehouse/` to be created | Not started |
| R4.7-b | At least 5 NoSQL queries, including indexed query | `queries/nosql/` to be created | Not started |
| R4.7-c | At least 3 continuous stream queries, including window and alert/filter | `queries/streaming/` to be created | Planned |
| R4.7-d | Optimization with before/after execution times | Partitioning/clustering proposed; timings must be captured later | Planned |
| R4.7-e | Document every query with question, text, sample output, and timing; scripts in GitHub | Query appendix/scripts to be created; no GitHub remote is available here | Not started |
| R4.7-f | Run and explain queries live | Demo instructions to be created; team practice still required | Not started |
| R6-a | Proposal with problem, architecture, tools, dataset | `docs/PROPOSAL.md`; awaiting approval | Draft |
| R6-b | Working GitHub implementation with README/setup instructions | Existing web app exists; pipeline and GitHub remote not yet available | Not started |
| R6-c | Final report with results and learnings | `docs/FINAL_REPORT.md` to be created in Phase 9 | Not started |
| R6-d | Live end-to-end demo and presentation | `docs/DEMO_SCRIPT.md` and presentation outline planned for Phase 9 | Not started |
| R6-e | Optional demo video | Optional; not required and excluded from planned scope | Not required |
| R7-ARCH | Prioritize architecture and design (20%) | Phase 0 proposal and approval gate | In progress |
| R7-IMPL | Complete all pipeline stages (25%) | Later implementation evidence | Not started |
| R7-STREAM | Prioritize real-time streaming (20%) | Broker, continuous calculations, alert, and dashboard plan | Planned |
| R7-SCALE | Report scale/volume/technical depth (10%) | VVV and measured scale evidence planned | Planned |
| R7-INSIGHTS | Provide useful insights and visualization (10%) | Streamlit live dashboard and warehouse query insights planned | Planned |
| R7-REPORT | Complete report (10%) | Phase 9 report planned | Not started |
| R7-DEMO | Prepare presentation/demo (5%) | Phase 9 materials and team rehearsal planned | Not started |

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
| Automated schedule | `.github/workflows/warehouse-etl.yml` hourly schedule and temporary secret-file cleanup | Workflow implemented; waiting for GitHub publication/secret setup and two successful `schedule`-triggered run records |

