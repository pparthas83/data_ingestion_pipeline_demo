#!/usr/bin/env bash
set -euo pipefail

# Deployment script for standalone BigQuery Dataform & Cloud Composer pipeline

GCP_PROJECT_ID="${1:-${GCP_PROJECT_ID:-}}"
COMPOSER_DAG_BUCKET="${2:-${COMPOSER_DAG_BUCKET:-}}"
GCP_REGION="${3:-${GCP_REGION:-us-central1}}"
DATAFORM_REPO_NAME="${4:-coned-dataform-repo}"

if [[ -z "$GCP_PROJECT_ID" || -z "$COMPOSER_DAG_BUCKET" ]]; then
  echo "Usage: $0 <GCP_PROJECT_ID> <COMPOSER_DAG_BUCKET> [GCP_REGION] [DATAFORM_REPO_NAME]"
  exit 1
fi

echo "========================================================================="
echo " Deploying Dataform Pipeline to GCP"
echo " Project ID : ${GCP_PROJECT_ID}"
echo " Region     : ${GCP_REGION}"
echo " Repo Name  : ${DATAFORM_REPO_NAME}"
echo " DAG Bucket : ${COMPOSER_DAG_BUCKET}"
echo "========================================================================="

# 1. Enable Dataform API
echo "[1/3] Enabling Dataform API..."
gcloud services enable dataform.googleapis.com --project="${GCP_PROJECT_ID}"

# 2. Check or Create Dataform Repository
echo "[2/3] Checking Dataform Repository '${DATAFORM_REPO_NAME}'..."
if ! gcloud dataform repositories describe "${DATAFORM_REPO_NAME}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  echo "Creating Dataform Repository..."
  gcloud dataform repositories create "${DATAFORM_REPO_NAME}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}"
else
  echo "Dataform Repository already exists."
fi

# 3. Deploy Composer DAG to GCS
echo "[3/3] Syncing DAG to Composer Bucket ${COMPOSER_DAG_BUCKET}..."
gsutil cp dataform_pipeline/dags/dataform_bq_transform_dag.py "${COMPOSER_DAG_BUCKET}/"

echo "========================================================================="
echo " Deployment Complete!"
echo "========================================================================="
