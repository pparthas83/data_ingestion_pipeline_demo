# GCP Production Data Ingestion Pipeline - Implementation Plan

Building an enterprise-grade batch data ingestion pipeline using **GCS, Dataflow (Apache Beam), Cloud Composer (Airflow), and BigQuery**, complete with Dead Letter Queue (DLQ) error isolation, automated testing, and Cloud Monitoring.

---

## 🏗️ Architecture Overview

```text
[ GCS Raw Landing Bucket ] (CSV Files: gs://<raw-bucket>/landing/YYYYMMDD/*.csv)
       │
       ▼
[ Cloud Composer (Airflow DAG) ] ── (GCS Sensor + Flex Template Operator)
       │
       ▼
[ Dataflow Flex Template Job ] (Apache Beam Python Pipeline)
       ├──► Valid Records  ──► [ BigQuery Target Table ] (Partitioned & Clustered)
       └──► Corrupt Records ─► [ BigQuery DLQ Table ]
                                      │
                                      ▼
                        [ Airflow Post-ETL DLQ Check ]
                                (Fails if DLQ > Threshold)
```

---

## 📋 Component Specifications

### 1. Expected CSV Input Schema (`id, timestamp, category, amount`)
- **Format**: Comma-Separated Values (CSV)
- **Columns**: `id` (string), `timestamp` (datetime/iso), `category` (string), `amount` (float)
- **Validation Rules**:
  - `id`: Non-empty required string.
  - `timestamp`: Datetime formatted as `YYYY-MM-DD HH:MM:SS` or ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`).
  - `category`: String (defaults to `UNSPECIFIED` if blank).
  - `amount`: Non-negative numeric (`>= 0.00`).
- **DLQ Trigger**: Any row violating these constraints is captured and written to `analytics_ds.target_records_dlq`.

### 2. BigQuery Dataset & Tables (`analytics_ds`)


#### Target Table: `analytics_ds.target_records`
- **Partitioning**: Day-partitioned by `DATE(timestamp)`
- **Clustering**: Clustered by `category`
- **Schema**:
  - `id` (STRING, REQUIRED)
  - `timestamp` (TIMESTAMP, REQUIRED)
  - `category` (STRING, NULLABLE)
  - `amount` (NUMERIC, NULLABLE)
  - `ingested_at` (TIMESTAMP, REQUIRED)

#### Dead Letter Queue (DLQ) Table: `analytics_ds.target_records_dlq`
- **Partitioning**: Day-partitioned by `DATE(failed_at)`
- **Schema**:
  - `raw_record` (STRING, NULLABLE)
  - `error_message` (STRING, NULLABLE)
  - `source_file` (STRING, NULLABLE)
  - `failed_at` (TIMESTAMP, REQUIRED)

---

## 🚀 Verification Strategy

1. **Local Unit Tests**:
   - Beam `TestPipeline` for valid, corrupt, and mixed CSV records.
   - Airflow `DagBag` test for DAG syntax and task dependency structure.
2. **Cloud Execution**:
   - Build & deploy Flex Template container image to Artifact Registry.
   - Stage Flex Template spec JSON in GCS.
   - Launch Dataflow job and verify BigQuery target & DLQ row counts.
