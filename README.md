# Enterprise GCP Data Ingestion Pipeline

**GCS to BigQuery Batch ETL via Apache Beam (Dataflow Flex Template), Cloud Composer (Airflow), & Enterprise Observability**

This repository provides an enterprise-ready, production-grade template for ingesting CSV batch data landing in Google Cloud Storage (GCS) into BigQuery using Apache Beam on Google Cloud Dataflow, orchestrated by Cloud Composer (Apache Airflow), complete with **Dead Letter Queue (DLQ) error isolation, automated unit testing, Cloud Monitoring dashboards, and post-ETL data quality auditing**.

---

## 🎯 Business Objective

The primary objective of this pipeline is to ingest CSV transactions landing in GCS into BigQuery with **zero data loss**:
* **Valid Records**: Filtered, validated, and streamed into a partitioned/clustered BigQuery target table (`analytics_ds.target_records`).
* **Corrupt/Invalid Records**: Schema violations, invalid dates, negative amounts, or malformed rows are caught by `TaggedOutput` routing and safely written to a Dead Letter Queue table (`analytics_ds.target_records_dlq`) along with error messages and source metadata.
* **Data Quality Gate**: The Airflow DAG audits the DLQ error count after each run and triggers an SLA failure alert if errors exceed a configurable threshold (`MAX_DLQ_THRESHOLD`).

---

## 📋 Expected CSV Input Structure & Validation Rules

The pipeline is currently coded and tested for a **4-column CSV schema**:

```csv
id,timestamp,category,amount
TXN-100001,2026-08-27 10:00:00,ELECTRONICS,150.00
TXN-100002,2026-08-27 10:05:00,HOME,42.50
```

### Field Definitions & Validation Rules

| Column Index | Field Name | Target BigQuery Type | Requirement / Validation Rule |
| :---: | :--- | :--- | :--- |
| `0` | `id` | `STRING` | **REQUIRED**. Cannot be empty or whitespace. |
| `1` | `timestamp` | `TIMESTAMP` | **REQUIRED**. Must match ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) or standard format (`YYYY-MM-DD HH:MM:SS`). |
| `2` | `category` | `STRING` | **OPTIONAL**. Product/transaction category. Defaults to `UNSPECIFIED` if empty. |
| `3` | `amount` | `NUMERIC` | **REQUIRED**. Must be a valid non-negative number (`>= 0.00`). |

### Dead Letter Queue (DLQ) Routing
If a CSV row violates any rule above (e.g. empty ID, invalid date string, negative amount, or fewer than 4 columns), the transform catches the exception and routes the row to the **Dead Letter Queue (DLQ)** table (`analytics_ds.target_records_dlq`) with the raw text, error message, and source file metadata.

> [!NOTE]
> **Adapting for Different or Dynamic CSV Schemas**:
> To support a different CSV structure or dynamic column layouts, update `ParseAndValidateCSVDoFn` in [pipeline/gcs_etl/transforms.py](file:///usr/local/google/home/pradeepsarathy/AntiGravity_Projects/Project_3/coned_demo/pipeline/gcs_etl/transforms.py) or use `csv.DictReader` to parse headers dynamically.

---

## 🏗️ End-to-End Architecture


```text
                                ┌───────────────────────────────────┐
                                │   GCS Raw Data Landing Bucket     │
                                │   `gs://<bucket>/landing/YYYYMMDD`│
                                └─────────────────┬─────────────────┘
                                                  │
                                                  ▼
 ┌─────────────────────────┐           ┌────────────────────────────────────┐
 │ Cloud Composer          │ ────────► │ GCS Sensor                         │
 │ (Apache Airflow)        │           │ (Deferrable Non-Blocking Sensor)   │
 └────────────┬────────────┘           └──────────────────┬─────────────────┘
              │                                           │
              ▼                                           ▼
 ┌──────────────────────────────────────────────────────────────────────────┐
 │ Dataflow Flex Template Job (Apache Beam Python Pipeline)                 │
 │                                                                          │
 │   CSV File ──► ParseAndValidateDoFn (Schema & Field Validation)          │
 │                      ├── Valid Rows   ──► WriteToBigQuery (Target)        │
 │                      └── Corrupt Rows ──► WriteToBigQuery (DLQ)           │
 └───────────────────────────────┬──────────────────────────────────────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
 ┌───────────────────────────────┐   ┌───────────────────────────────┐
 │ BigQuery Target Table         │   │ BigQuery DLQ Table            │
 │ `analytics_ds.target_records` │   │ `analytics_ds.target_records` │
 └───────────────────────────────┘   └───────────────┬───────────────┘
                                                     │
                                                     ▼
                                     ┌───────────────────────────────┐
                                     │ Airflow Data Quality Audit    │
                                     │ (Fails if DLQ > Threshold)    │
                                     └───────────────────────────────┘
```

---

## 📁 Repository Structure

```text
.
├── pipeline/                   # Apache Beam ETL Pipeline & Docker Setup
│   ├── gcs_etl/
│   │   ├── __init__.py
│   │   └── transforms.py       # Custom DoFn CSV validation & TaggedOutput DLQ logic
│   ├── main.py                 # Pipeline entrypoint with setup.py worker distribution
│   ├── setup.py                # Setuptools config for worker package staging
│   ├── Dockerfile              # Flex Template launcher container image definition
│   ├── requirements.txt        # Beam SDK and GCP dependencies
│   └── metadata.json           # Dataflow Flex Template UI parameters spec
├── dags/
│   └── gcs_to_bq_dag.py        # Cloud Composer DAG (Deferrable Sensor + Flex Template + DLQ Audit)
├── sql/
│   └── create_tables.sql       # BigQuery DDL (Target table, DLQ table, Partitioning & Clustering)
├── tests/                      # Automated Unit Test Suite
│   ├── test_pipeline.py        # Apache Beam TestPipeline unit tests (Valid, Corrupt, Mixed)
│   └── test_dag.py             # Airflow DagBag unit tests (DAG compilation & task dependencies)
├── scripts/                    # Deployment & Automation Scripts
│   ├── build_and_deploy.sh     # Docker build, Artifact Registry push, & Flex Template staging
│   ├── setup_gcp_resources.sh  # Provision GCS buckets, BQ datasets, tables & IAM
│   └── run_gcp_test.sh         # End-to-end sandbox execution script
├── monitoring/
│   └── dashboard.json          # Cloud Monitoring Dashboard definition
├── sample_data/
│   ├── generate_sample_data.py # Mock CSV generator (Valid, Corrupt, Mixed batches)
│   ├── valid_batch.csv
│   └── corrupt_batch.csv
├── dataform_pipeline/          # Standalone BigQuery Dataform Core & Composer Transformation Pipeline
│   ├── README.md               # Architecture and usage guide for Dataform pipeline
│   ├── dataform/               # Dataform Core SQLX definitions, settings, & assertions
│   ├── dags/                   # Cloud Composer DAG for Dataform orchestration
│   ├── scripts/                # Dataform pipeline deployment script
│   └── tests/                  # Standalone Dataform Airflow DAG unit tests
└── docs/                       # Project Documentation & Verification Proofs
    ├── implementation_plan.md  # System Architecture & Technical Specifications
    ├── task.md                 # Project Checklist & Progress Log
    └── walkthrough.md          # Empirical Test Proofs & Live Cloud Results
```

---

## 💻 Developer Quick-Start & Local Environment Setup

### 1. Prerequisites
- **Python 3.11+**
- **Google Cloud SDK (`gcloud`)** authenticated with active GCP credentials (`gcloud auth login` and `gcloud auth application-default login`).
- **Docker** (for building Flex Template launcher container images).

### 2. Local Virtual Environment Setup
```bash
# Clone the repository
git clone git@github.com:pparthas83/data_ingestion_pipeline_demo.git
cd data_ingestion_pipeline_demo

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r pipeline/requirements.txt
pip install apache-airflow apache-airflow-providers-google apache-airflow-providers-apache-beam

# Initialize local Airflow metadata database (for DAG parsing test)
airflow db migrate
```

---

## 🧪 How to Run Tests (Offline Verification)

We provide a complete automated unit test suite that validates **Beam pipeline transforms** and **Airflow DAG compilation** locally without incurring GCP cloud costs.

### Run All Unit Tests
```bash
.venv/bin/python3 -m unittest discover -s tests -v
```

### Expected Output
```text
test_dag_loaded (test_dag.TestGCSNotificationDAG.test_dag_loaded) ... ok
test_dag_structure_and_task_count (test_dag.TestGCSNotificationDAG.test_dag_structure_and_task_count) ... ok
test_task_dependencies (test_dag.TestGCSNotificationDAG.test_task_dependencies) ... ok
test_failure_scenario_all_corrupt_records (test_pipeline.TestGCSIngestionPipeline) ... ok
test_mixed_records_routing (test_pipeline.TestGCSIngestionPipeline) ... ok
test_success_scenario_all_valid_records (test_pipeline.TestGCSIngestionPipeline) ... ok

----------------------------------------------------------------------
Ran 6 tests in 6.350s

OK
```

---

## 🚀 How to Deploy to GCP Cloud

### Step 1: Set Environment Variables
```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-central1"
export RAW_DATA_BUCKET="your-raw-data-bucket"
export TEMP_BUCKET="your-temp-bucket"
export COMPOSER_DAG_BUCKET="gs://us-central1-my-composer-bucket/dags"  # Optional if using Composer
```

### Step 2: Provision GCP Resources (BigQuery Tables & Buckets)
```bash
./scripts/setup_gcp_resources.sh
```

### Step 3: Build & Stage Dataflow Flex Template
This builds the Docker launcher image, pushes it to Artifact Registry (`us-central1-docker.pkg.dev/${GCP_PROJECT_ID}/dataflow-templates/gcs-to-bq-etl:v1.0.0`), and uploads the Flex Template spec JSON to `gs://${TEMP_BUCKET}/templates/gcs_to_bq_template.json`:
```bash
./scripts/build_and_deploy.sh
```

### Step 4: Deploy Cloud Monitoring Dashboard
```bash
gcloud monitoring dashboards create \
    --config-from-file=monitoring/dashboard.json \
    --project $GCP_PROJECT_ID
```

---

## ☁️ How to Run & Test in GCP Cloud

### Option A: Run End-to-End Test Script
Generates mock CSV batches, uploads them to GCS landing, and launches the Dataflow Flex Template:
```bash
./scripts/run_gcp_test.sh
```

### Option B: Trigger Dataflow Job Directly via `gcloud`
```bash
gcloud dataflow flex-template run "gcs-to-bq-etl-manual-run" \
    --template-file-gcs-location="gs://${TEMP_BUCKET}/templates/gcs_to_bq_template.json" \
    --region="${GCP_REGION}" \
    --parameters input_pattern="gs://${RAW_DATA_BUCKET}/landing/20260827/*.csv" \
    --parameters output_table="${GCP_PROJECT_ID}:analytics_ds.target_records" \
    --parameters dlq_table="${GCP_PROJECT_ID}:analytics_ds.target_records_dlq" \
    --project="${GCP_PROJECT_ID}"
```

---

## 📊 Verification & BigQuery Results

Once the Dataflow job status reaches **`JOB_STATE_DONE`**, verify row counts in BigQuery:

```sql
-- Query Target Table (Valid Rows)
SELECT COUNT(1) AS valid_count FROM `${GCP_PROJECT_ID}.analytics_ds.target_records`;

-- Query DLQ Table (Corrupt Rows)
SELECT raw_record, error_message, source_file, failed_at
FROM `${GCP_PROJECT_ID}.analytics_ds.target_records_dlq`
ORDER BY failed_at DESC;
```

---

## 📑 Additional Documentation & Proofs

- [Medallion Architecture Design Guide](docs/medallion_pipeline_design.md): End-to-end design for Bronze ➔ Silver 1 ➔ Silver 2 pipeline across 5 GCP environments.
- [Target Audience & Beneficiaries](docs/who_is_it_for.md): Detailed guide on who benefits from this repository and primary use cases solved.
- [Implementation Plan](docs/implementation_plan.md): Technical architecture and BigQuery DDL schemas.
- [Task Progress Checklist](docs/task.md): Component breakdown and verification steps.
- [Verification Walkthrough](docs/walkthrough.md): Empirical proof of live Dataflow executions and unit test results.
- [Code Scan & Security Audit Results](docs/code_scan_and_test_results.md): Official bandit SAST security scan, ruff linter, and unit test execution report.



