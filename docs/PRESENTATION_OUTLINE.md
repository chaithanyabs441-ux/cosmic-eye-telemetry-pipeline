# Presentation Outline — Cosmic Eye Telemetry Pipeline

**Suggested duration:** 8–10 minutes plus questions. Both students should take
turns explaining the stages and be able to answer questions about the full
pipeline.

| Slide | Title | What to say | What to show |
|---:|---|---|---|
| 1 | Problem and objective | Cosmic Eye displays public SPHEREx observations; the project adds analytics for observation metadata and synthetic application activity. | Project title, course, and both team members' names. Fill names before presenting. |
| 2 | Data and provenance | Explain real archive metadata vs. synthetic quality fields and synthetic usage. No personal usage data is collected. | `docs/PHASE8_SCALE_INSIGHTS.md` provenance table; show `is_synthetic` and `source` in BigQuery, with credentials hidden. |
| 3 | Architecture | Follow the event path from Python to Redpanda, BigQuery, dbt, MongoDB, Spark, and Streamlit. | Mermaid architecture in `docs/FINAL_REPORT.md` or `docs/PIPELINE_README.md`. |
| 4 | Ingestion and scale | Describe the completed 860k synthetic batch plus the 160k partial IRSA-sourced attempt. State the measured rates and count reconciliation exactly. | Terminal excerpt in `docs/PHASE8_SCALE_INSIGHTS.md`, plus BigQuery row counts. |
| 5 | Warehouse and ETL | Explain raw vs curated, deduplication, type validation, dimensions/facts, and dbt tests. | BigQuery table list/counts and a successful dbt run summary. |
| 6 | Queries and optimization | Present examples of the SQL patterns and show why the Mongo compound index reduces examined documents. | Run `queries/run_query.py q01`; show the Mongo `explain` output and appendix medians. |
| 7 | Streaming | Explain tumbling counts, sliding latency/error windows, watermark, and threshold alerts. | Start live demo and show Streamlit live metric and alert rows. Explain occasional >5s local batches honestly. |
| 8 | Business insights | Interpret five trends; explicitly say noise and app events are synthetic. | Streamlit **1_Warehouse_Insights** page and its charts. |
| 9 | Limits and lessons | Explain Sandbox retention/storage, Atlas size, local-vs-hosted runner decision, and schedule delays. | Phase 8 usage table; GitHub Actions run history. Show two successful scheduled runs if captured. |
| 10 | Conclusion and questions | Recap that the course pipeline runs from ingestion through live analytics; state remaining constraints and cleanup plan. | Checklist summary and references. Do not run teardown before the viva. |

## Presenter split

- **Student 1:** slides 1–5 (problem, data, architecture, ingestion, warehouse).
- **Student 2:** slides 6–10 (queries, stream, dashboard, limits, conclusion).
- Swap roles during rehearsal so both students can explain generation,
  transformations, indexes, windows, and limitations.

## Before presenting

- Fill both team names and take project screenshots.
- Confirm the `.github/workflows/warehouse-etl.yml` recent history. Use actual
  scheduled successes only when the event type says `schedule`/Scheduled; a
  manual dispatch run is not automatic-schedule proof.
- Hide `.env`, database URIs, service account JSON, IP address, and GitHub
  secret screens from recordings/screenshots.
- Keep the live demo small; do not rerun the 860,000-event scale batch during a
  presentation.
