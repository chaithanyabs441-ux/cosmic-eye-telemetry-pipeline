# Optional Demo Video Storyboard (60–90 seconds)

This is a recording outline only; a video is optional and has not been
recorded.

| Time | Screen / narration |
|---:|---|
| 0–8 sec | Title: Cosmic Eye Telemetry Pipeline. State the course problem and that synthetic quality/app values are labeled. |
| 8–20 sec | Show the architecture diagram. Trace Python → Redpanda → BigQuery → dbt → curated. |
| 20–35 sec | Show the verified BigQuery row counts and scale metrics from the report. Do not expose project credentials. |
| 35–48 sec | Show one SQL query result and the Mongo compound index `explain` result. |
| 48–68 sec | Show Streamlit live window metrics and one event-triggered alert. |
| 68–80 sec | Switch to warehouse insights and explain one chart. |
| 80–90 sec | State free-tier limits, schedule caveat, and that cleanup waits until after the viva. |

Before recording, fill team member names, confirm the current Actions status,
and hide `.env`, Atlas URI, service-account JSON, IP addresses, and secrets.
