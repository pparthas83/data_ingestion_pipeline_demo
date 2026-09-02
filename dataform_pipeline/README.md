# Enterprise BigQuery Dataform & Cloud Composer Transformation Pipeline

**Automated BigQuery Table-to-Table Modeling, Assertions, and Orchestration via Dataform Core & Cloud Composer**

This module provides a standalone, production-grade template for performing **BigQuery data transformations, table-to-table migrations, incremental aggregations, and data quality assertions** using **Google Cloud Dataform Core** orchestrated by **Cloud Composer (Apache Airflow)**.

---

## 🏗️ Architecture & Component Flow

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                CLOUD COMPOSER (AIRFLOW DAG)                                  │
│                 `dataform_pipeline/dags/dataform_bq_transform_dag.py`                      │
│                                                                                             │
│  ┌────────────────────────┐      ┌───────────────────────────┐      ┌────────────────────┐  │
│  │ Dataform Compilation   │ ───► │ Dataform Workflow         │ ───► │ Dataform Invocation│  │
│  │ Operator               │      │ Invocation Operator       │      │ Sensor             │  │
│  └────────────────────────┘      └───────────────────────────┘      └────────────────────┘  │
└────────────────────────────────────────────────┬────────────────────────────────────────────┘
                                                 │ API Triggers
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 GOOGLE CLOUD DATAFORM REPOSITORY                            │
│                                  `dataform_pipeline/dataform/`                              │
│                                                                                             │
│   definitions/sources/                 definitions/silver2/               definitions/      │
│   source_validated_transactions.sqlx   enriched_summary.sqlx              assertions/       │
│   (Upstream BQ Source Declaration) ──► (Incremental/Table Transform) ──► (Built-in Quality │
│                                                                           Assertions & Gate)│
└────────────────────────────────────────────────┬────────────────────────────────────────────┘
                                                 │ Generates & Executes SQL
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                     BIGQUERY DATASET LAYER                                  │
│                                                                                             │
│   Source Table:                               Target Table:                                 │
│   `silver1_ds.validated_transactions` ──────► `silver2_ds.enriched_summary`                  │
│   (Partitioned by DATE(timestamp))            (Partitioned by transaction_date, Clustered)   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 File & Module Layout

```text
dataform_pipeline/
├── README.md                                # Setup & architecture documentation
├── dataform/                                # Dataform Core repository root
│   ├── workflow_settings.yaml               # Target GCP project, default dataset, & location
│   ├── package.json                         # Dataform CLI dependencies
│   └── definitions/
│       ├── sources/
│       │   └── source_validated_transactions.sqlx # Upstream BQ source declaration
│       ├── silver2/
│       │   └── enriched_summary.sqlx        # Incremental SQLX transformation model
│       └── assertions/
│           └── unique_category_date.sqlx    # Quality assertion rule
├── dags/
│   └── dataform_bq_transform_dag.py         # Cloud Composer DAG
├── scripts/
│   └── deploy_dataform_pipeline.sh          # Helper deployment script
└── tests/
    └── test_dataform_dag.py                 # Airflow DAG unit tests
```

---

## 🚀 Key Features

1. **Declarative Incremental Loading**: Utilizes Dataform's `${when(incremental(), ...)}` logic to update target BigQuery tables efficiently without full scans.
2. **Quality Assertions**: Built-in non-null and uniqueness checks executed automatically before publishing data.
3. **Airflow Orchestration**:
   - `DataformCreateCompilationResultOperator`: Compiles SQLX code dynamically per execution.
   - `DataformCreateWorkflowInvocationOperator`: Dispatches transformation execution to GCP.
   - `DataformWorkflowInvocationSensor`: Monitors job completion in Cloud Composer.

---

## 🧪 Local Testing & Verification

Run the unit test suite to verify the Airflow DAG syntax and task graph:

```bash
.venv/bin/python3 -m unittest discover -s dataform_pipeline/tests -v
```
