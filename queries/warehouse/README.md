# Warehouse query map

Run one query from the repository root:

```powershell
pipeline\.venv\Scripts\python.exe queries\run_query.py q01
```

Use `all` to run all eight. Each query has a business-question comment; the
runner prints captured rows, client elapsed time, and BigQuery bytes processed.
The SQL reads BigQuery directly and does not export event data to local files.

| ID | Business question | SQL techniques |
|---|---|---|
| q01 | Which wavelength bands have the most exposures and noise? | Join, aggregation, dimension lookup |
| q02 | Which regions have enough exposures to compare quality? | `GROUP BY`, `HAVING` |
| q03 | How does the cumulative exposure total grow by day? | CTE, running-total window |
| q04 | How does noise compare with a seven-row moving average? | CTE, moving-average window |
| q05 | Which regions rank highest per day? | CTE, `RANK`, `QUALIFY` |
| q06 | Which app actions have the highest latency/error rate? | Aggregation, approximate percentile, `HAVING` |
| q07 | Compare exposure and app activity by date and sky cell | Two CTEs, full outer join |
| q08 | Which requests exceed the overall average latency? | Scalar subquery, filter |

Captured outputs and timings are in [the query appendix](../../docs/QUERY_APPENDIX.md).
