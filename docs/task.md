# Implementation Task Checklist

- [x] Create project directory structure (`dags/`, `pipeline/`, `sample_data/`, `tests/`, `sql/`, `scripts/`, `monitoring/`)
- [x] Implement Apache Beam ETL pipeline with DLQ tagged output & metric counters (`pipeline/main.py`)
- [x] Create Flex Template Dockerfile & requirements (`pipeline/Dockerfile`, `pipeline/requirements.txt`, `pipeline/metadata.json`)
- [x] Implement Airflow DAG with deferrable GCS sensor, SLA callbacks & DLQ quality check (`dags/gcs_to_bq_dag.py`)
- [x] Create Cloud Monitoring Dashboard JSON (`monitoring/dashboard.json`)
- [x] Write BigQuery DDL scripts for target, DLQ, and logging tables (`sql/create_tables.sql`)
- [x] Create sample CSV test data generator & static CSV files (`sample_data/generate_sample_data.py`, `valid_batch.csv`, `corrupt_batch.csv`, `mixed_batch.csv`)
- [x] Implement PyTest pipeline test suite for DirectRunner (`tests/test_pipeline.py`) - **PASSED (3/3 tests)**
- [x] Create GCP resource setup and integration test runner scripts (`scripts/setup_gcp_resources.sh`, `scripts/run_gcp_test.sh`, `scripts/build_and_deploy.sh`)
- [x] Write comprehensive project documentation (`README.md`)
