# Cosmic Eye Telemetry Pipeline data dictionary

The raw tables preserve event payloads and provenance. Curated tables use typed
columns and are populated by the Phase 4 cleaning workflow. Dimension keys are
logical keys; BigQuery does not enforce declared primary/foreign key
constraints.

## Raw dataset

### `raw.events`

One row per event accepted by basic envelope validation. Duplicate event IDs
are retained here intentionally so Phase 4 can demonstrate deduplication.

| Field | Type | Meaning |
|---|---|---|
| `run_id` | STRING | Unique pipeline invocation identifier |
| `event_id` | STRING | Event identifier; may repeat in intentionally duplicated input |
| `event_type` | STRING | `exposure` or `usage` |
| `event_ts` | STRING | Source event timestamp in ISO-8601 form |
| `is_synthetic` | BOOL | True if any event content is generated; real metadata paired with generated quality fields is therefore true |
| `region_key` | STRING | Ten-degree RA/Dec grid cell identifier |
| `band_id` | STRING | SPHEREx band label, for example `SPHEREx-D1` |
| `source` | STRING | Public archive or synthetic generator provenance |
| `source_accessed_at` | STRING | UTC time the source metadata was queried/generated |
| `dirty_flags` | STRING | JSON array describing intentionally injected dirty conditions |
| `raw_payload` | STRING | Full original event serialized as JSON text |
| `ingested_at` | TIMESTAMP | UTC time the loader consumed the event; daily partition column |

### `raw.rejected_events`

One row per message rejected by basic envelope checks. Its raw JSON and reason
are retained so the rejection can be examined without losing source context.

| Field | Type | Meaning |
|---|---|---|
| `run_id` | STRING | Unique pipeline invocation identifier |
| `event_id` | STRING, nullable | Event ID when it is a string |
| `event_type` | STRING, nullable | Event type when it is a string |
| `raw_payload` | STRING | Original message envelope as JSON text |
| `rejection_reason` | STRING | Basic validation failure |
| `ingested_at` | TIMESTAMP | UTC rejection time; daily partition column |

## Curated star schema

### Dimensions

| Table | Grain | Key fields |
|---|---|---|
| `curated.dim_band` | One row per SPHEREx wavelength band | `band_id`, `wavelength_min_um`, `wavelength_max_um`, `wavelength_center_um` |
| `curated.dim_region` | One row per 10-degree sky cell | `region_key`, RA/Dec cell indexes and bounds |
| `curated.dim_date` | One row per calendar date | `date_key` in YYYYMMDD form, `full_date`, year/quarter/month/week/day values |

### Facts

| Table | Grain | Important fields |
|---|---|---|
| `curated.fact_exposures` | One cleaned exposure event | Event/observation IDs; band, region, and date keys; event and archive times; provenance flags; archive metadata; explicitly synthetic quality measurements |
| `curated.fact_usage_events` | One cleaned synthetic app event | Anonymous session ID; event name/endpoint/target; band, region, and date keys; latency/status; synthetic flag |

`fact_exposures` retains the SIA wavelength values as `em_min_m` / `em_max_m`
and stores the Phase 4 converted values in `dim_band` as micrometres. `t_min_mjd`
is the archive observation time in Modified Julian Date; `observation_time` is
its typed timestamp conversion. The `event_ts` is the pipeline event time and
is kept separate from the archive observation time.

## Synthetic and provenance rules

- Usage rows are always synthetic, with anonymous generated session IDs.
- Exposure events from IRSA contain public archive metadata but synthetic
  quality metrics. Their overall `is_synthetic` is true and
  `metadata_is_synthetic` is false; `provenance.source` and
  `provenance.source_accessed_at` identify the real metadata source/time.
- Exposure rows created without IRSA set both `is_synthetic` and
  `metadata_is_synthetic` to true and say “not NASA data” in provenance.
- Rejected events keep the exact serialized input and a validation reason.

## Phase 4 cleaning and retention

- Repeated event IDs are reduced to the latest ingested copy in each fact.
- Non-numeric latency/noise fields that are present but have the wrong type are
  sent to `curated.preprocessing_rejections`. Basic malformed-message rejects
  stay in `raw.rejected_events`.
- Negative latency/background/noise/pointing values, saturation outside
  `[0, 1]`, latency above 600,000 ms, and HTTP status outside `[100, 599]` are
  converted to `NULL` in curated fields.
- Missing values remain `NULL`; blank usage targets are normalized to `NULL`.
- JSON fields are safely cast to typed BigQuery columns. Wavelength bounds in
  metres are converted to micrometres; band centre, date keys, calendar fields,
  and sky-cell bounds are derived.
- dbt tests enforce non-null and unique keys for dimensions and fact event IDs.

Curated facts are partitioned by ingestion time, not event time. BigQuery
Sandbox applied 60-day partition expiration to event-time partitions and
immediately removed the 2025 backfill partitions. Partitioning by `ingested_at`
retains historical rows during the Sandbox retention window; use `event_ts`
and `date_key` for event-time analysis. Sandbox retention still applies.

### `curated.preprocessing_rejections`

One row per valid-envelope raw event rejected by curated schema validation.
The original payload, identifier, type, run ID, ingestion time, and rejection
reason are retained. This is separate from basic ingestion rejects in
`raw.rejected_events`.
