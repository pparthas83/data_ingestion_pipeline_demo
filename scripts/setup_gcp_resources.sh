#!/bin/bash
set -e

# Load environment variables or use generic defaults
PROJECT_ID="${GCP_PROJECT_ID:-YOUR_GCP_PROJECT_ID}"
REGION="${GCP_REGION:-us-central1}"
RAW_BUCKET="${RAW_DATA_BUCKET:-YOUR_RAW_DATA_BUCKET}"
TEMP_BUCKET="${TEMP_BUCKET:-YOUR_TEMP_BUCKET}"

echo "========================================="
echo "Setting up GCP Resources for Data Pipeline"
echo "Project: $PROJECT_ID"
echo "Region:  $REGION"
echo "========================================="

# 1. Create Cloud Storage Buckets
echo "[1/3] Creating GCS Buckets..."
gcloud storage buckets create "gs://${RAW_BUCKET}" --project="${PROJECT_ID}" --location="${REGION}" 2>/dev/null || echo "Bucket gs://${RAW_BUCKET} already exists."
gcloud storage buckets create "gs://${TEMP_BUCKET}" --project="${PROJECT_ID}" --location="${REGION}" 2>/dev/null || echo "Bucket gs://${TEMP_BUCKET} already exists."

# 2. Create BigQuery Dataset & Tables
echo "[2/3] Provisioning BigQuery Dataset & Schemas..."
bq mk --project_id="${PROJECT_ID}" --location="${REGION}" --dataset analytics_ds 2>/dev/null || echo "Dataset analytics_ds already exists."
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false < sql/create_tables.sql

# 3. Create Artifact Registry Repository for Dataflow Templates
echo "[3/3] Creating Artifact Registry Repository..."
gcloud artifacts repositories create dataflow-templates \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" 2>/dev/null || echo "Artifact Registry repository already exists."

echo "GCP Resource Setup Complete!"
