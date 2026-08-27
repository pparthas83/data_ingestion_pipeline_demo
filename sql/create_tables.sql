-- =============================================================================
-- BigQuery DDL: Target, DLQ & Logging Audit Tables Initialization
-- =============================================================================

-- 1. Create Analytics Dataset
CREATE SCHEMA IF NOT EXISTS `analytics_ds`
OPTIONS (
  location = 'US',
  description = 'Analytics Dataset for GCS to BQ Ingestion Pipeline'
);

-- 2. Create Logging & Observability Dataset (for Cloud Logging Log Sink)
CREATE SCHEMA IF NOT EXISTS `logs_ds`
OPTIONS (
  location = 'US',
  description = 'Long-term Audit and Observability Dataset for Pipeline Log Sinks'
);

-- 3. Target Clean Records Table
CREATE TABLE IF NOT EXISTS `analytics_ds.target_records` (
    id STRING NOT NULL OPTIONS(description="Unique record identifier"),
    timestamp TIMESTAMP OPTIONS(description="Event / transaction timestamp"),
    category STRING OPTIONS(description="Normalized record category"),
    amount NUMERIC OPTIONS(description="Transaction amount"),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Pipeline ingestion timestamp")
)
PARTITION BY DATE(timestamp)
CLUSTER BY category, id
OPTIONS (
    description = "Partitioned and clustered clean records target table populated by Dataflow ETL",
    require_partition_filter = false
);

-- 4. Dead-Letter Queue (DLQ) Error Table
CREATE TABLE IF NOT EXISTS `analytics_ds.target_records_dlq` (
    raw_record STRING OPTIONS(description="Raw string row from source file that failed validation"),
    error_message STRING OPTIONS(description="Exception or validation failure reason"),
    source_file STRING OPTIONS(description="GCS URI or path pattern of source file"),
    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when failure occurred")
)
PARTITION BY DATE(failed_at)
OPTIONS (
    description = "Dead-Letter Queue (DLQ) table storing malformed or schema-invalid rows"
);
