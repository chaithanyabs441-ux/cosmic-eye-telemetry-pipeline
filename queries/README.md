# Phase 7 query guide

Run commands from the repository root in PowerShell. The Python virtual
environment and `.env` are from Phase 1; `.env` must remain private.

```powershell
# Run one BigQuery query or all eight; output includes captured rows, time, bytes.
pipeline\.venv\Scripts\python.exe queries\run_query.py q01
pipeline\.venv\Scripts\python.exe queries\run_query.py all

# Run six MongoDB Atlas query examples and the indexed explain query.
pipeline\.venv\Scripts\python.exe queries\nosql\run_queries.py

# Measure collection scan vs. indexed lookup (15 repetitions each).
pipeline\.venv\Scripts\python.exe queries\nosql\run_optimization.py

# Read the current streaming output counts from Atlas (streaming job must have run).
pipeline\.venv\Scripts\python.exe pipeline\verify_streaming.py
```

SQL text, questions, actual sample output and measured durations are recorded
in [`docs/QUERY_APPENDIX.md`](../docs/QUERY_APPENDIX.md). SQL mapping is in
[`warehouse/README.md`](warehouse/README.md), Mongo operations in
[`nosql/README.md`](nosql/README.md), and continuous Spark query definitions in
[`streaming/README.md`](streaming/README.md).
