# GCS to BigQuery Batch Ingestion Pipeline (Apache Beam & Dataflow)

**Batch CSV Ingestion, Validation, TaggedOutput Routing, and Dead Letter Queue (DLQ) Error Isolation**

This module provides the production-grade Apache Beam Python pipeline for ingesting CSV files landing in Google Cloud Storage (GCS) into BigQuery with zero data loss.

---

## 🎯 Engine & Pipeline Design

```text
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

## 📋 CSV Input Rules & Validation

Expects a 4-column CSV structure: `id,timestamp,category,amount`.

| Field Name | Target BQ Type | Validation Rule |
| :--- | :--- | :--- |
| `id` | `STRING` | **REQUIRED**. Non-empty string. |
| `timestamp` | `TIMESTAMP` | **REQUIRED**. ISO 8601 or `YYYY-MM-DD HH:MM:SS`. |
| `category` | `STRING` | **OPTIONAL**. Defaults to `UNSPECIFIED`. |
| `amount` | `NUMERIC` | **REQUIRED**. Non-negative float (`>= 0.00`). |

---

## 💻 Local Testing & Verification

Run the Beam pipeline unit test suite (testing valid rows, corrupt rows, and mixed batches):

```bash
.venv/bin/python3 -m unittest discover -s tests -v
```

---

## 🚀 Building & Deploying Flex Template

```bash
# Build launcher container image, push to Artifact Registry & stage Flex Template spec
./scripts/build_and_deploy.sh
```
