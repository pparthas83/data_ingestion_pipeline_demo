# GCP Production Data Ingestion Pipeline
**GCS to BigQuery Batch Pipeline via Cloud Composer, Dataflow Flex Templates & Enterprise Observability**

This repository provides an enterprise-ready starter template for ingesting batch data landing in Google Cloud Storage (GCS) into BigQuery using Apache Beam on Dataflow, orchestrated by Cloud Composer (Apache Airflow), complete with **production logging, custom metrics, SLA callbacks, and observability dashboards**.

---

## 🏢 Architecture & Observability Overview

```
[ GCS Bucket ] (Landing CSV/JSON)
      │
      ▼
[ Cloud Composer (Airflow) ] ── (Deferrable Sensor + Failure/SLA Callbacks)
      │
      ▼ (DataflowStartFlexTemplateOperator)
[ Dataflow (Apache Beam) ] ──► (Beam Metric Counters & Structured JSON Logs)
      ├──► Valid Records  ──► [ BigQuery Target Table ] (Partitioned & Clustered)
      └──► Corrupt Records ─► [ BigQuery DLQ Table ] ──► [ Airflow Post-ETL DLQ Check ]
                                                                  │
                                                        (Exceeds Threshold?)
                                                                  │
                                                        [ Trigger SLA Alert ]

[ Cloud Monitoring Dashboard ] ◄── (Beam Counters + Dataflow vCPU + BQ Write Rates)
[ BigQuery Log Sink ] ◄─────────── (Cloud Logging Export: logs_ds.pipeline_audit_logs)
```

### Key Highlights
* **Apache Beam Custom Telemetry**: Exposes `processed_records`, `valid_records`, and `dlq_records` metric counters directly to Cloud Monitoring.
* **Airflow Failure & SLA Callbacks**: Configures `on_failure_callback` and `sla_miss_callback` to route incident alerts immediately.
* **Post-ETL Data Quality Gate**: Includes an Airflow BigQuery validation task (`audit_dlq_quality_threshold`) that fails the pipeline if DLQ record count exceeds `MAX_DLQ_THRESHOLD`.
* **Unified Observability Dashboard**: Custom Cloud Monitoring dashboard definition (`monitoring/dashboard.json`) visualizing Beam throughput, Dataflow CPU/memory, Composer DAG status, and BQ write rates.
* **Cloud Logging Audit Sink**: Long-term audit trail exporting pipeline logs into BigQuery (`logs_ds.pipeline_audit_logs`).

---

## 📁 Repository Structure

```
.
├── dags/
│   └── gcs_to_bq_dag.py        # Composer DAG with deferrable sensor, callbacks & DLQ quality check
├── pipeline/
│   ├── main.py                 # Apache Beam Python ETL script with custom counters & JSON logging
│   ├── Dockerfile              # Flex Template launcher Dockerfile
│   ├── requirements.txt        # Beam SDK & GCP dependencies
│   └── metadata.json           # Dataflow Flex Template spec parameters
├── monitoring/
│   └── dashboard.json          # Cloud Monitoring unified dashboard JSON configuration
├── sql/
│   └── create_tables.sql       # BigQuery schema setup for Target, DLQ & Logging datasets
├── scripts/
│   └── build_and_deploy.sh     # Automated build, template staging, DAG & dashboard deploy script
└── README.md
```

---

## 🚀 Deployment Guide

### Step 1: Initialize BigQuery Datasets & Tables
```bash
bq query --use_legacy_sql=false < sql/create_tables.sql
```

### Step 2: Build & Deploy All Pipeline Components
Set your GCP environment variables and run the deployment script:
```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-central1"
export TEMP_BUCKET="your-gcs-temp-bucket"
export COMPOSER_DAG_BUCKET="us-central1-my-composer-bucket-gcs"

./scripts/build_and_deploy.sh
```

---

## 📊 Viewing Observability & Metrics

1. **Cloud Monitoring Dashboard**:
   Navigate to **GCP Console > Monitoring > Dashboards** and select `GCS to BigQuery Ingestion - Observability Dashboard`.
2. **BigQuery Audit Log Sink**:
   Query pipeline logs stored in `logs_ds`:
   ```sql
   SELECT timestamp, jsonPayload.event, jsonPayload.error_message
   FROM `your-project.logs_ds.dataflow_step_*`
   WHERE jsonPayload.event = 'dlq_record_captured'
   ORDER BY timestamp DESC LIMIT 50;
   ```
