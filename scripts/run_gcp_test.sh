#!/usr/bin/env bash
# =============================================================================
# GCP Integration Test Runner: Success & Failure Scenarios
# =============================================================================
set -euo pipefail

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
RAW_BUCKET="${RAW_DATA_BUCKET:-your-raw-data-bucket}"
TARGET_DATASET="${TARGET_DATASET:-analytics_ds}"
TARGET_TABLE="${TARGET_TABLE_NAME:-target_records}"

DATE_PREFIX=$(date +%Y%m%d)
GCS_LANDING_PREFIX="gs://${RAW_BUCKET}/landing/${DATE_PREFIX}"

echo "====================================================================="
echo "  GCP Pipeline Integration Test: Success & Failure Verification"
echo "====================================================================="

echo "=== 1. Generating Fresh Sample Datasets ==="
python3 sample_data/generate_sample_data.py

echo "=== 2. Uploading Sample CSV Datasets to GCS Landing Zone ==="
gsutil cp sample_data/valid_batch.csv "${GCS_LANDING_PREFIX}/valid_batch.csv"
gsutil cp sample_data/corrupt_batch.csv "${GCS_LANDING_PREFIX}/corrupt_batch.csv"
gsutil cp sample_data/mixed_batch.csv "${GCS_LANDING_PREFIX}/mixed_batch.csv"

echo "Uploaded test files to ${GCS_LANDING_PREFIX}/"

echo "=== 3. Current Row Counts in BigQuery (Before Job Execution) ==="
bq query --use_legacy_sql=false "
    SELECT 'Target Records' AS table_type, COUNT(1) AS row_count FROM \`${PROJECT_ID}.${TARGET_DATASET}.${TARGET_TABLE}\`
    UNION ALL
    SELECT 'DLQ Records' AS table_type, COUNT(1) AS row_count FROM \`${PROJECT_ID}.${TARGET_DATASET}.${TARGET_TABLE}_dlq\`;
"

echo "=== 4. Trigger Instructions ==="
echo "The test CSV files are staged in GCS!"
echo "To trigger execution via Cloud Composer:"
echo "  gcloud composer environments run <COMPOSER_ENV_NAME> --location <REGION> dags trigger -- gcs_to_bq_dataflow_etl"
echo ""
echo "After DAG execution completes, verify row counts using:"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(1) FROM \`${PROJECT_ID}.${TARGET_DATASET}.${TARGET_TABLE}\`'"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(1) FROM \`${PROJECT_ID}.${TARGET_DATASET}.${TARGET_TABLE}_dlq\`'"
