-- Run each statement in the BigQuery console with the query location set to US.
-- TABLE_STORAGE is regional/project-scoped; TABLES, PARTITIONS, and COLUMNS
-- can be inspected at dataset scope.

-- Confirm the raw and curated tables exist. These are metadata-only checks.
SELECT table_name, table_type
FROM `cosmic-eye-telemetry-demo.raw.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;

SELECT table_name, table_type
FROM `cosmic-eye-telemetry-demo.curated.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;

-- Table row counts and logical bytes for the project's raw and curated datasets.
-- Requires project-level BigQuery Metadata Viewer. Replace region-us if location changes.
SELECT table_schema AS dataset_name, table_name, total_rows, total_logical_bytes
FROM `cosmic-eye-telemetry-demo.region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT`
WHERE table_schema IN ('raw', 'curated')
ORDER BY dataset_name, table_name;

-- Per-partition counts/bytes for the raw event tables.
SELECT table_name, partition_id, total_rows, total_logical_bytes
FROM raw.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name IN ('events', 'rejected_events')
ORDER BY table_name, partition_id;

SELECT table_name, partition_id, total_rows, total_logical_bytes
FROM `cosmic-eye-telemetry-demo.curated.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name IN ('fact_exposures', 'fact_usage_events')
ORDER BY table_name, partition_id;

-- Column types for the curated star schema.
SELECT table_name, column_name, data_type, is_nullable
FROM curated.INFORMATION_SCHEMA.COLUMNS
ORDER BY table_name, ordinal_position;

-- Current project-wide logical storage snapshot.
-- Requires project-level BigQuery Metadata Viewer; query location must be US.
SELECT COUNT(*) AS tables_visible,
       COALESCE(SUM(total_logical_bytes), 0) AS project_logical_bytes
FROM `cosmic-eye-telemetry-demo.region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT`;
