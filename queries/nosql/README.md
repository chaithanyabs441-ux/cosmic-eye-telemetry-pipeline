# MongoDB query map

Run from the repository root:

```powershell
pipeline\.venv\Scripts\python.exe queries\nosql\run_queries.py
```

The script reads only aggregate summary and live alert documents. It does not
print the Atlas URI or anonymous session IDs. Query `n06` forces the existing
compound `region_band_day_lookup` index and prints `explain("executionStats")`
evidence including index scans, examined keys/documents, and returned rows.

| ID | Access pattern | MongoDB operation |
|---|---|---|
| n01 | Filter and project by sky cell and band | `find`, projection, sort, limit |
| n02 | Summarize exposure quality by band | `$group` aggregation |
| n03 | Summarize app usage and errors by action | `$group` aggregation |
| n04 | Direct summary retrieval | `_id` key lookup |
| n05 | Inspect newest live alerts | `find`, sort, limit |
| n06 | Indexed region and band lookup | Compound index plus `explain("executionStats")` |

Captured outputs and execution times are in [the query appendix](../../docs/QUERY_APPENDIX.md).
