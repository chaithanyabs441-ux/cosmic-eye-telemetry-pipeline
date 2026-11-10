# Course Requirements Checklist

**Phase 1 status:** local setup scaffold and walkthrough are prepared; external accounts and credentials remain user setup, and all three smoke checks await the user run. Planned is not a completion claim.

| Requirement ID | What will satisfy it | File / query / evidence | Status |
|---|---|---|---|
| G-TEAM | Exactly two students; both can explain all stages | `docs/PROPOSAL.md` team fields are placeholders; names still needed | Planned |
| G-FREE | Free tiers only; monitor usage; tear down resources | `docs/PROPOSAL.md` cost controls; BigQuery Sandbox and Atlas limits in `docs/REFERENCES.md` | Partial — limits checked; no accounts/resources created; budget alert conflict recorded |
| G-CITE | Cite datasets, libraries, and external resources | `docs/REFERENCES.md` | Draft |
| G-PRIVACY | No real personal/sensitive app-usage data | `docs/PROPOSAL.md` synthetic anonymous sessions and provenance design | Planned |
| R4.1-a | Use synthetic records and/or replay public data | `docs/PROPOSAL.md` public IRSA metadata plus explicitly marked synthetic records | Planned |
| R4.1-b | Generate in batch and continuous modes | `docs/PROPOSAL.md` generator and broker design | Planned |
| R4.1-c | State volume, velocity, variety | `docs/PROPOSAL.md` VVV target table; targets not measured yet | Draft |
| R4.2-a | Store raw and curated data only in a cloud warehouse | `docs/PROPOSAL.md` BigQuery architecture; no warehouse created | Planned |
| R4.2-b | Do not store the primary dataset locally | `docs/PROPOSAL.md` in-memory batches, bounded broker retention, cleanup plan | Planned |
| R4.2-c | Verify free-tier limits and use an eligible free option | `docs/REFERENCES.md` BigQuery Sandbox limits; Cloud Billing alert limitation documented | Partial — current limits verified; actual project and usage not verified |
| R4.3-a | Retrieve warehouse results into at least one NoSQL database | `docs/PROPOSAL.md` scheduled BigQuery-to-Atlas retrieval | Planned |
| R4.3-b | Justify NoSQL type by access patterns | `docs/PROPOSAL.md` compact region/observation/alert documents in MongoDB | Draft |
| R4.4-a | Build ETL with dbt/Spark/Airflow/Glue or equivalent | `docs/PROPOSAL.md` dbt-bigquery SQL + Python validation | Planned |
| R4.4-b | Run ETL automatically or on a schedule | `docs/PROPOSAL.md` scheduled GitHub Actions design; no workflow yet | Planned |
| R4.5 | Clean, deduplicate, validate, handle missing/noisy/late values, convert formats, engineer fields | `docs/PROPOSAL.md` Python/Pydantic, dbt, and Spark stream-window plan | Planned |
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
| Redpanda broker and start/stop scripts | `infra/compose.yaml`, `scripts/start-pipeline.ps1`, `scripts/stop-pipeline.ps1` | Prepared; Docker daemon unavailable for verification in this environment |
| Cloud, database, Java, Python, Docker, GitHub instructions | `docs/PHASE1_SETUP.md` | Prepared; accounts and installs not verified |
| BigQuery/MongoDB/Redpanda smoke checks | `pipeline/smoke_test.py` | Prepared; user must run and return redacted output |
| Automated scheduler | GitHub Actions design in proposal; actual ETL workflow deferred until Phase 4 code exists | Not implemented in Phase 1 |

