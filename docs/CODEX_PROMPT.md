# PROMPT FOR CODEX — Convert "Cosmic Eye" into the Big Data Analytics course project

You are my coding partner and teacher. I am a **beginner**. This repository ("Cosmic Eye") is currently a web app (FastAPI + React) that shows NASA SPHEREx observations. My course (Big Data Analytics – 23BSDA5D54, Jain University) requires an **End-to-End Big Data Pipeline** project. Your job: extend this repo into that pipeline, and do **only what the course notice requires**. No extra features, no gold-plating, no optional extras (ML, graph analytics, etc.) unless I ask later.

---

## 0. HOW YOU MUST WORK (read twice, applies to every phase)

1. **Work in phases (listed in section 3). Never start a phase until I reply "continue".**
2. **Verify as you go.** After every step, run a check and show me the real output. Never write "done" without evidence (command + actual output, or a screenshot/row-count/query result). If you cannot verify something (e.g. you have no access to my cloud accounts), say so plainly and give me the exact command to run and what output I should see, then wait for me to paste the result. Never fake or assume output.
3. **Beginner mode.** Whenever *I* must do something (create an account, click in a web console, install software, set a password/key), give numbered steps, exact commands, where to click, and "you should now see ___". Explain any term the first time you use it in one plain sentence. Tell me what to do if it fails. Website UIs change, so check the official docs for current steps; if unsure, say so.
4. **Secrets:** never ask me to paste passwords/keys into chat. Put them in a `.env` file I fill in myself. Commit only `.env.example`. Add `.env`, service-account keys and any data files to `.gitignore`.
5. **After every phase, stop and send a PHASE REPORT in exactly this format:**
   - **Phase N – name – status (PASS / PARTIAL / FAIL)**
   - **What I did** (plain words, 5–10 lines)
   - **Files created/changed** (paths)
   - **Verification evidence** (each check: command → actual output → pass/fail)
   - **Notice requirements covered** (IDs from section 2, e.g. R4.2-a)
   - **What YOU must do or confirm now** (beginner steps)
   - **Explain it like I'm presenting it** (5 lines I could say in the viva) + 2 likely viva questions with answers
   - **Problems / risks / free-tier usage so far**
   - End with: "Reply `continue` to start Phase N+1."
6. Update `docs/REQUIREMENTS_CHECKLIST.md` every phase (table: requirement ID → what satisfies it → file/query/evidence → status). At the end every row must be PASS with evidence.
7. Keep the existing app working. Put all new work in new folders (`pipeline/`, `queries/`, `docs/`, `dashboard/`, `infra/`). Don't refactor the web app. Run `scripts/check.sh` at Phase 0 and Phase 10 to prove nothing broke.
8. My laptop is weak (≈8 GB RAM, Intel i5, Linux). Plan so that not everything must run at once; measure RAM before choosing heavy tools and tell me.
9. **Free tiers only.** Before I create anything, check the *current* free-tier limits/eligibility from the official docs and tell me. Help me set budget/usage alerts. Never suggest anything that needs paid billing without warning me.
10. Do not use real personal/sensitive data anywhere. All user/usage data is synthetic with anonymous IDs.
11. Keep the final summary short; no filler.

---

## 1. THE PROJECT TO BUILD

**Title:** *Cosmic Eye Telemetry Pipeline — real-time analytics for SPHEREx observation quality and app usage.* (Confirm or let me rename in Phase 0.)

**Data (two streams, sharing keys so SQL joins are meaningful):**
- **Exposure/observation events:** built from the real SPHEREx metadata this repo already queries (see `backend/app/adapters/irsa_spherex.py`, `archive_service.py`: obs_id, band em_min/em_max, s_ra/s_dec, t_min, calib_level, access_url), replayed as a live feed and scaled up with synthetic records in the same schema, plus synthetic quality metrics (e.g. background level, noise sigma, saturated-pixel fraction, pointing error, processing latency). Check the official IRSA/SPHEREx docs for what bulk metadata access exists. **Every row must carry `is_synthetic` (true/false) and provenance** (source, access date). Never invent fake data and present it as real NASA data.
- **App usage events:** synthetic search/browse/preview/skymap events (anonymous session id, endpoint, target, region, band, latency_ms, http_status, timestamp).
- Generator must emit in **batches (history back-fill) and continuously (live)**, and deliberately inject dirty data (duplicates, nulls, out-of-range values, wrong types, late events) so preprocessing is demonstrable.
- **State volume, velocity and variety explicitly** (target: ≥ 1 million historical rows, ideally several million; live rate of at least a few hundred events/second; ≥ 3 formats/shapes of data). Adjust to what the free tier and my laptop can handle, and justify the numbers.

**Default stack (change only with a written justification in Phase 0):**
| Layer | Tool |
|---|---|
| Message bus | Redpanda or Kafka (Docker) — or Confluent Cloud free tier if RAM is a problem |
| Cloud warehouse (raw + curated) | **Google BigQuery Sandbox** (use datasets `raw`, `curated`) |
| ETL + scheduling | dbt-bigquery for SQL transforms + an **automated scheduler** (Airflow if RAM allows; otherwise a lighter equivalent such as Prefect or a scheduled GitHub Actions workflow). Must run on a schedule without me clicking anything. |
| Preprocessing | PySpark and/or dbt + schema validation (pydantic/pandera/dbt tests) |
| NoSQL | **MongoDB Atlas free tier (M0)** — document store; justify vs. Cassandra/Redis/etc. based on access patterns |
| Streaming | Spark Structured Streaming (or Kafka Streams/Flink) reading from the bus |
| Dashboard | Streamlit (live) |

**Hard rule – cloud only:** the raw and curated data must live **only in the cloud warehouse**. No local database, no local copies of the dataset (no CSV/Parquet/SQLite of the primary data committed or kept). Data flows generator → bus → BigQuery directly; any temporary buffer must be deleted automatically. Check the current BigQuery Sandbox limits (e.g. whether streaming inserts/DML are allowed, table expiry, storage and query quotas) and design around them (e.g. micro-batch load jobs). Tell me the limits you found and what they mean for the report.

---

## 2. REQUIREMENTS FROM THE COURSE NOTICE (the checklist you must satisfy — nothing more)

**General:** team of 2; use only free tiers/credits, monitor usage, set budget alerts, **shut down/delete cloud resources after use**; cite all datasets/libraries/resources; no sensitive/personal data in the cloud; both members must be able to explain every part in the viva/demo.

- **R4.1 Data generation & ingestion:** (a) synthetic or replayed public data; (b) continuous or batch flow simulating real-world; (c) volume, velocity, variety stated in the report.
- **R4.2 Cloud data warehouse (cloud only):** (a) raw AND curated data stored only in the cloud warehouse; (b) no local storage of the primary dataset; (c) free-tier option used, limits verified.
- **R4.3 Retrieval & NoSQL:** (a) retrieve from the warehouse and load into ≥ 1 NoSQL DB; (b) justify the NoSQL type (document/column/key-value/graph) by use case & access patterns.
- **R4.4 ETL:** (a) workflows built with Spark/Airflow/dbt/Glue or equivalent; (b) **automated or scheduled**, not manual steps.
- **R4.5 Preprocessing:** cleaning, deduplication, missing & noisy values, schema validation, format conversion, feature engineering (as relevant).
- **R4.6 Real-time streaming (compulsory):** (a) streaming layer (Kafka/Redpanda/Confluent free/Spark SS/Flink/Kafka Streams); (b) real-time processing: windowed aggregations, anomaly detection, live metrics or alerts; (c) results on a **live dashboard**.
- **R4.7 Querying (every layer, not just loading):**
  - (a) **Warehouse SQL: ≥ 8 analytical queries** collectively using: joins, aggregations, GROUP BY with HAVING, subqueries or CTEs, window functions (ranking, running total, moving average).
  - (b) **NoSQL: ≥ 5 queries** suited to the store (filters/projections, aggregation pipelines, key lookups), **≥ 1 using an index/partition key**.
  - (c) **Streaming: ≥ 3 continuous queries**, including **a windowed aggregation (tumbling, sliding or session)** and **one event-triggered alert or filter**.
  - (d) **Optimisation:** ≥ 1 technique (partitioning, clustering, indexing, caching, query rewriting) with **before/after execution times**.
  - (e) **Documentation for EVERY query:** business question, query text, sample output, execution time → in the report appendix, and query scripts pushed to GitHub.
  - (f) **Demo:** I must be able to run any query live and explain it.
- **R6 Deliverables:** (a) project proposal with architecture diagram, tool choices, dataset description; (b) working implementation on GitHub with README + setup instructions; (c) final report: problem statement, architecture, design choices, challenges, results, learnings; (d) live demo + presentation showing the pipeline end-to-end; (e) optional short demo video (only a script/storyboard, not required).
- **R7 Marking (use to prioritise effort):** architecture & design 20%, implementation completeness (all stages) 25%, real-time streaming 20%, scale/volume/technical depth 10%, insights & visualisation 10%, report 10%, presentation/demo 5%.

---

## 3. PHASES

**Phase 0 – Audit, plan, proposal** (R6a, R4.1c, R4.2c)
- Read the whole repo and `docs/` notice if present (the notice may be at `docs/Big_Data_Analytics_Project_Notice.docx`; if missing, rely on section 2). Run `scripts/check.sh` and report the baseline.
- Check free-tier limits for every chosen service; measure laptop RAM/CPU/disk and Docker/Java availability; propose a RAM budget.
- Ask me (once, in one list) for: team member names, GCP project name preference, GitHub repo URL, and whether I accept the default stack.
- Create: `docs/PROPOSAL.md` (problem statement, domain, architecture diagram in Mermaid, tool choices with reasons, dataset description, volume/velocity/variety targets, star-schema sketch with ≥ 3 related tables e.g. `fact_exposures`, `fact_usage_events`, `dim_band`, `dim_region`, `dim_date`), the repo layout, `docs/REQUIREMENTS_CHECKLIST.md`, `docs/REFERENCES.md` (cite NASA/IPAC IRSA SPHEREx, every library, every doc used).
- Gate: I approve the proposal.

**Phase 1 – Accounts & environment setup (beginner walkthrough)**
- Walk me step by step through: Python venv, Docker, Java (for Spark), Git/GitHub, Google Cloud project + BigQuery Sandbox + service account + budget/usage alert, MongoDB Atlas free cluster (+ IP allow-list + DB user), the message bus, the scheduler. Create `.env.example`, `.gitignore` updates, `requirements`/`docker-compose` files, a `Makefile` or `scripts/` for one-command start/stop.
- Write `pipeline/smoke_test.py` that checks: BigQuery write+read+delete a tiny table, Mongo insert+read+delete, bus produce+consume. I run it; you verify output.
- Gate: all three smoke tests PASS.

**Phase 2 – Data generation & ingestion** (R4.1)
- Build the generator (batch back-fill mode + continuous live mode, configurable rate), producing the two streams with `is_synthetic` + provenance, deliberate dirty data, and a fixed random seed for reproducibility. Publish to the bus; ingest to BigQuery `raw` via a loader (with a dead-letter table `raw.rejected_events` for records failing basic checks). Nothing persisted locally.
- Verify: counts produced vs consumed vs landed in BigQuery (query the cloud, show numbers); confirm no dataset files are left on disk.
- Report the measured volume, velocity, variety.

**Phase 3 – Cloud warehouse design** (R4.2)
- Create `raw` and `curated` datasets, DDL scripts in `infra/sql/`, partitioning by date and clustering on sensible keys (this is also used for R4.7d), the star schema in `curated`, and the data-dictionary doc.
- Verify: table list, schemas, row counts, partition info shown from the cloud; storage used vs free-tier limit.

**Phase 4 – Preprocessing + automated ETL** (R4.4, R4.5)
- Implement raw → curated: dedup, missing-value handling, outlier/noise handling, schema validation (rejects go to the dead-letter table), type/format conversion (e.g. MJD → timestamp, metres → µm, JSON → typed columns), feature engineering (e.g. band centre wavelength, wavelength-band label, hour-of-day, region cell id, latency bucket, rolling noise score). Add data-quality tests.
- Wire it into the **scheduler** so it runs on its own. Prove ≥ 2 scheduled runs happened without manual action (show run history/logs).
- Verify: before/after row counts per cleaning rule, test results, run history.

**Phase 5 – Warehouse → NoSQL** (R4.3)
- Write the retrieval job (BigQuery → MongoDB Atlas), incremental and idempotent, scheduled together with the ETL. Design the document model from the access patterns (e.g. per-region and per-observation documents, latest alerts) and write the **justification** (why document store over column-family/key-value/graph) in the docs. Create the indexes.
- Verify: document counts match the warehouse query counts; show sample documents and the indexes.

**Phase 6 – Real-time streaming analytics + live dashboard** (R4.6)
- Streaming job reading from the bus: windowed aggregations (e.g. 1-min tumbling window of events per region/band; 5-min sliding window of API latency/error rate), anomaly/alert rules (e.g. noise or saturation spike per exposure, pointing-error drift, archive error-rate or latency breach), handling of late events (watermark). Write alerts to MongoDB (and BigQuery if allowed).
- Streamlit dashboard: live metrics, live alerts, auto-refresh.
- Verify: start the generator, show the dashboard updating, inject an anomaly on purpose and show the alert firing within seconds; give me a screenshot or a text capture. Provide a one-command demo mode.

**Phase 7 – Querying, optimisation, documentation** (R4.7a–f)
- Create `queries/warehouse/` (≥ 8, covering joins, aggregation, GROUP BY + HAVING, subquery, CTE, ranking, running total, moving average — map each requirement to a query in a table), `queries/nosql/` (≥ 5, ≥ 1 index-backed, show `explain()`), `queries/streaming/` (≥ 3: ≥ 1 windowed aggregation, ≥ 1 alert/filter).
- Optimisation: run a chosen query **before** and **after** partitioning/clustering/indexing (and one query-rewrite if time), record bytes processed and execution times (run each several times, report median).
- `queries/run_query.py <id>` runs any query and prints result + time, so I can demo live.
- Generate `docs/QUERY_APPENDIX.md`: for every query → business question, query text, sample output (real, captured), execution time. Never fabricate outputs.

**Phase 8 – Scale, insights, visualisation** (R7 scale + insights)
- Run a scale test at the target volume; record throughput, latency, and BigQuery/Mongo usage vs free limits. Add a second Streamlit page with 4–5 business insights from the warehouse queries (clear charts, one-line takeaway each).

**Phase 9 – README, report, presentation, demo script** (R6b–e)
- `README.md` (setup from zero, run commands, architecture diagram, structure, credits), `docs/FINAL_REPORT.md` (problem statement, architecture, design choices & trade-offs, volume/velocity/variety, free-tier limits found, challenges, results, learnings, references, appendix link; mark clearly where team-member names/screenshots must be filled in by me), `docs/PRESENTATION_OUTLINE.md` (slide-by-slide with what to show), `docs/DEMO_SCRIPT.md` (exact command order, timing, fallback if the internet fails, and the list of queries I can run live), short video storyboard (optional).
- Update the existing README minimally to link to the pipeline.

**Phase 10 – Final audit & teardown**
- Fresh-clone test of the setup instructions (or a dry run of them). Run `scripts/check.sh` again. Walk the whole `REQUIREMENTS_CHECKLIST.md` and show evidence for every row; list anything still missing.
- Create `scripts/teardown.sh` + a manual checklist to **stop containers and delete/shut down every cloud resource** (BigQuery datasets/project, Atlas cluster, any bus/cloud service, service-account keys). **Do NOT run teardown until I confirm my final demo/viva is finished** — remind me, and list exactly what I will lose.
- Final report to me: what's complete, what I must still fill in manually (names, screenshots), and a 10-question viva prep list covering every stage.

---

## 4. START NOW

Begin **Phase 0 only**. First say in one sentence what you're about to do, then do it, then send the Phase 0 report and wait for my "continue".
