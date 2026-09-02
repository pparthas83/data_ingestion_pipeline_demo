# Enterprise GCP Medallion Data Platform Template

**Production-grade Data Ingestion & Transformation Platform on Google Cloud (Dataflow, Dataform, BigQuery, & Cloud Composer)**

Welcome to the Enterprise GCP Medallion Data Platform repository. This platform provides an end-to-end modular architecture for batch data ingestion, schema validation, error isolation, SQL modeling, and workflow orchestration across BigQuery Medallion layers (Bronze ➔ Silver 1 ➔ Silver 2).

---

## 🏛️ End-to-End Medallion Architecture

```text
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                MEDALLION DATA PLATFORM                                    │
│                                                                                           │
│ 1. INGESTION (PIPELINE A)               2. CLEANING & QUALITY         3. TRANSFORMATION     │
│    GCS Landing Bucket                   Bronze / Silver 1             (PIPELINE B)          │
│            │                                     │                    Dataform Core         │
│            ▼                                     ▼                          │               │
│    Dataflow Flex Template (Beam) ──►  `silver1_ds.validated`     ──►            ▼               │
│            │                             │   │                        `silver2_ds.summary`   │
│            └─ DLQ Error Isolation ───────┘   └── DLQ Audit Gate       (Partitioned/Clustered)│
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Modular Pipelines Hub

This repository is organized into independent, decoupled pipeline modules:

| Module | Core Technology | Primary Responsibility | Documentation Link |
| :--- | :--- | :--- | :--- |
| 📥 **GCS-to-BigQuery Ingestion** | Apache Beam (Python), Dataflow Flex Template, Airflow | CSV parsing, field validation, TaggedOutput DLQ routing, and Bronze / Silver 1 landing | 📖 [Pipeline A Documentation](pipeline/README.md) |
| 🔄 **Dataform Table Transformations** | Dataform Core (SQLX), BigQuery, Cloud Composer | Table-to-table migrations, incremental MERGE modeling, assertions, and Silver 2 KPI summary | 📖 [Pipeline B Documentation](dataform_pipeline/README.md) |

---

## 📁 Repository Directory Layout

```text
.
├── pipeline/                   # [Pipeline A] Apache Beam ETL Pipeline & Docker Launcher
│   ├── README.md               # Pipeline A architecture and usage guide
│   ├── gcs_etl/                # Custom Beam DoFn validation and TaggedOutputs
│   ├── main.py                 # Dataflow Flex Template entrypoint
│   └── Dockerfile              # Container launcher image spec
│
├── dataform_pipeline/          # [Pipeline B] Standalone BigQuery Dataform Core Transformation Pipeline
│   ├── README.md               # Pipeline B architecture and usage guide
│   ├── dataform/               # Dataform Core SQLX definitions, settings, & assertions
│   ├── dags/                   # Dedicated Cloud Composer DAG for Dataform
│   ├── scripts/                # Dataform pipeline deployment script
│   └── tests/                  # Dataform Airflow DAG unit tests
│
├── dags/                       # [Pipeline A] Cloud Composer DAG for Dataflow Ingestion
├── sql/                        # BigQuery DDL DDL definitions & partitioning setup
├── tests/                      # Automated unit test suite for Beam & Airflow DAGs
├── scripts/                    # Platform setup, build, and execution scripts
├── docs/                       # Platform design documents and technical specifications
│   ├── medallion_pipeline_design.md # Multi-tier GCP environment design
│   ├── implementation_plan.md  # Technical specifications & architecture
│   └── archive/                # Historical documentation archive
│       └── README_legacy_v1.md # Legacy single-pipeline README archive
└── monitoring/                 # Cloud Monitoring Dashboard spec
```

---

## 💻 Developer Quick-Start & Testing Hub

### 1. Environment Setup
```bash
# Clone the repository
git clone git@github.com:pparthas83/data_ingestion_pipeline_demo.git
cd data_ingestion_pipeline_demo

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r pipeline/requirements.txt
pip install apache-airflow apache-airflow-providers-google apache-airflow-providers-apache-beam
```

### 2. Run All Automated Unit Tests

```bash
# Run Pipeline A (Apache Beam & Ingestion DAG) Unit Tests
.venv/bin/python3 -m unittest discover -s tests -v

# Run Pipeline B (Dataform Transformation DAG) Unit Tests
.venv/bin/python3 -m unittest discover -s dataform_pipeline/tests -v
```

---

## 📑 Platform Documentation & Design Guides

- 📖 [Medallion Architecture & Multi-Environment Design](docs/medallion_pipeline_design.md)
- 📖 [Target Audience & Key Beneficiaries](docs/who_is_it_for.md)
- 📖 [Verification Walkthrough & Test Results](docs/walkthrough.md)
- 📖 [Security Audit & SAST Scan Results](docs/code_scan_and_test_results.md)
- 📦 [Archived Legacy README (v1)](docs/archive/README_legacy_v1.md)
