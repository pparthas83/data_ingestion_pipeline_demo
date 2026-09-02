# Enterprise Medallion Architecture Pipeline Design

**BigQuery Medallion Data Platform (Bronze ➔ Silver 1 ➔ Silver 2) Across 5 GCP Environments**

---

## 🎯 Architecture Executive Summary

This document defines the complete technical architecture and pipeline design for an enterprise **Medallion Data Ingestion Platform** in BigQuery across **5 isolated GCP environments**: `Sandbox`, `Dev`, `Test`, `Stage`, and `Prod`.

```text
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                           5 ISOLATED GCP PROJECTS                                         │
│  company-sandbox  │   company-dev   │   company-test   │   company-stage  │  company-prod  │
└─────────┬─────────────────┬──────────────────┬──────────────────┬─────────────────┬───────┘
          │                 │                  │                  │                 │
          ▼                 ▼                  ▼                  ▼                 ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                           MEDALLION DATA PIPELINE (PER PROJECT)                           │
│                                                                                           │
│   GCS Landing Bucket ──► Dataflow Flex Template (Apache Beam)                             │
│                               │                                                           │
│                               ├──► BRONZE TIER   (`bronze_ds.raw_transactions`)           │
│                               │    Raw payload preservation, append-only, lineage         │
│                               │                                                           │
│                               ├──► SILVER 1 TIER (`silver1_ds.validated_transactions`)    │
│                               │    Cleaned, typed, deduplicated + DLQ Error Isolation    │
│                               │                                                           │
│                               └──► BigQuery SQL / dbt Transformation                      │
│                                    │                                                      │
│                                    └──► SILVER 2 TIER (`silver2_ds.enriched_summary`)    │
│                                         Business metrics, aggregations & domain models    │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏛️ 1. Environment & Infrastructure Isolation

Each of the 5 environments runs in its own **dedicated GCP Project**, guaranteeing strict resource, billing, security, and data boundary isolation:

| Environment | GCP Project ID | Primary Purpose & Usage |
| :--- | :--- | :--- |
| **Sandbox** | `company-sandbox` | Developer experimental environment for ad-hoc pipeline testing. |
| **Dev** | `company-dev` | Integrated development environment for active PR development. |
| **Test** | `company-test` | Automated integration testing and quality regression testing (`unittest`). |
| **Stage** | `company-stage` | Production replica / pre-prod environment for performance and SLA benchmarking. |
| **Prod** | `company-prod` | Live production workload with strict IAM least-privilege controls. |

---

## 🥇 2. Medallion Layer Specifications

### Tier 1: Bronze Layer (`bronze_ds`)
* **Objective**: Raw data preservation with zero transformation.
* **Table**: `bronze_ds.raw_transactions`
* **Ingestion Method**: Apache Beam (Dataflow Flex Template) streams GCS raw files directly into Bronze.
* **Schema**:
  - `raw_payload` (STRING/JSON): Unparsed raw input row.
  - `source_file` (STRING): GCS file URI.
  - `ingested_at` (TIMESTAMP): UTC timestamp of ingestion.
  - `batch_id` (STRING): Execution batch ID.
* **Partitioning & Clustering**: Day-partitioned by `DATE(ingested_at)`, clustered by `batch_id`.

---

### Tier 2: Silver 1 Layer (`silver1_ds`)
* **Objective**: Data cleaning, schema validation, type casting, deduplication, and error isolation.
* **Table**: `silver1_ds.validated_transactions`
* **Dead Letter Queue Table**: `dlq_ds.corrupt_transactions_dlq`
* **Ingestion Method**: Apache Beam `ParseAndValidateCSVDoFn` uses TaggedOutputs:
  - Valid rows ➔ `silver1_ds.validated_transactions`
  - Malformed/corrupt rows ➔ `dlq_ds.corrupt_transactions_dlq`
* **Schema**:
  - `id` (STRING, REQUIRED)
  - `timestamp` (TIMESTAMP, REQUIRED)
  - `category` (STRING, REQUIRED)
  - `amount` (NUMERIC, REQUIRED)
  - `ingested_at` (TIMESTAMP, REQUIRED)
* **Partitioning & Clustering**: Day-partitioned by `DATE(timestamp)`, clustered by `category`.

---

### Tier 3: Silver 2 Layer (`silver2_ds`)
* **Objective**: Business logic, domain modeling, dimensional joins, and KPI aggregations.
* **Table**: `silver2_ds.enriched_summary`
* **Transformation Engine**: BigQuery SQL / dbt executed by Cloud Composer `BigQueryInsertJobOperator`.
* **Schema**:
  - `transaction_date` (DATE)
  - `category` (STRING)
  - `total_transactions` (INT64)
  - `total_amount` (NUMERIC)
  - `avg_transaction_amount` (NUMERIC)
  - `updated_at` (TIMESTAMP)
* **Partitioning & Clustering**: Day-partitioned by `transaction_date`, clustered by `category`.

---

## 🔄 3. Cloud Composer (Airflow) DAG Workflow

The workflow is orchestrated end-to-end by Cloud Composer in each environment:

```text
[ GCS Sensor: wait_for_landing_files ]
                 │
                 ▼
[ Dataflow Flex Template: gcs_to_bronze_and_silver1 ]
                 │
                 ├─────────────────────────────────────────┐
                 ▼                                         ▼
[ BigQuery Target: silver1_ds ]          [ BigQuery DLQ: dlq_ds ]
                 │                                         │
                 ▼                                         ▼
[ BigQuery SQL: transform_silver1_to_silver2 ]  [ Quality Task: audit_dlq_threshold ]
                 │                                         │
                 └────────────────────┬────────────────────┘
                                      │
                                      ▼
                        [ Task: send_pipeline_metrics ]
```

---

## 🚀 4. CI/CD Promotion & Deployment Strategy

Branching and environment promotion are managed via **GitHub Actions / Cloud Build**:

```text
Feature Branch ──► PR to `dev` ──────► PR to `test` ──────► PR to `stage` ─────► Release Tag ──► `prod`
      │                │                     │                   │                               │
      ▼                ▼                     ▼                   ▼                               ▼
`company-sandbox` `company-dev`       `company-test`      `company-stage`                 `company-prod`
```

1. **Feature Branches** (`feature/*`): Deploys to `company-sandbox` for developer testing.
2. **`dev` Branch**: Deploys Flex Templates, Airflow DAGs, and BQ schemas to `company-dev`.
3. **`test` Branch**: Deploys to `company-test` and triggers automated integration tests (`unittest`).
4. **`stage` Branch**: Deploys to `company-stage` for SLA and load benchmarking.
5. **`main` / Release Tags** (`v1.x.x`): Deploys to `company-prod` via manual approval gate.

---

## 🛡️ 5. Security & Observability Controls

* **Private IP Isolation**: Dataflow workers enforce `"ipConfiguration": "WORKER_IP_PRIVATE"`.
* **IAM Least Privilege**: Each environment uses a dedicated service account (`sa-dataflow@<project>.iam.gserviceaccount.com`).
* **Cloud Monitoring Dashboard**: Real-time tracking of:
  - Beam counters (`processed_records`, `valid_records`, `dlq_records`).
  - BigQuery row count rates across Bronze, Silver 1, and Silver 2.
  - Dataflow vCPU and memory utilization.
  - Airflow task SLA status.
