#!/usr/bin/env bash
# =============================================================================
# GCP Resource Setup Script: Provision Buckets, BigQuery Datasets & Tables
# =============================================================================
set -euo pipefail

# Configuration Variables
PROJECT_ID="${GCP_PROJECT_ID:-pradeep-demo-1}"
REGION="${GCP_REGION:-US}"
RAW_BUCKET="${RAW_DATA_BUCKET:-pradeep-demo-1-raw-data}"
TEMP_BUCKET="${TEMP_BUCKET:-pradeep-demo-1-temp}"

echo "=== 1. Setting Active GCP Project ==="
gcloud config set project "${PROJECT_ID}"

echo "=== 2. Creating Cloud Storage Landing & Temp Buckets ==="
gcloud storage buckets create "gs://${RAW_BUCKET}" --project="${PROJECT_ID}" --location="${REGION}" || echo "Raw bucket already exists."
gcloud storage buckets create "gs://${TEMP_BUCKET}" --project="${PROJECT_ID}" --location="${REGION}" || echo "Temp bucket already exists."

echo "=== 3. Initializing BigQuery Datasets & Tables ==="
bq query \
    --use_legacy_sql=false \
    --location="${REGION}" \
    --project_id="${PROJECT_ID}" \
    < sql/create_tables.sql

echo "=== GCP Resource Setup Complete! ==="
echo "Raw Data Bucket: gs://${RAW_BUCKET}"
echo "Temp Bucket: gs://${TEMP_BUCKET}"
