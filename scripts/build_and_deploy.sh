#!/usr/bin/env bash
# =============================================================================
# Build & Deploy Script: Dataflow Flex Template, Airflow DAG & Monitoring
# =============================================================================
set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration Variables (Customize for your GCP environment)
# -----------------------------------------------------------------------------
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
REGION="${GCP_REGION:-us-central1}"
ARTIFACT_REPO="${ARTIFACT_REPO:-dataflow-templates}"
IMAGE_NAME="gcs-to-bq-etl"
IMAGE_TAG="v1.0.0"

BUCKET_NAME="${TEMP_BUCKET:-your-temp-bucket}"
COMPOSER_DAG_BUCKET="${COMPOSER_DAG_BUCKET:-your-composer-dag-bucket}"

IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"
TEMPLATE_SPEC_GCS="gs://${BUCKET_NAME}/templates/gcs_to_bq_spec.json"

echo "=== 1. Ensuring Artifact Registry Repository Exists ==="
gcloud artifacts repositories create "${ARTIFACT_REPO}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" || true

echo "=== 2. Building Dataflow Flex Template Container Image ==="
gcloud builds submit pipeline/ \
    --tag="${IMAGE_URI}" \
    --project="${PROJECT_ID}"

echo "=== 3. Creating Dataflow Flex Template JSON Specification ==="
gcloud dataflow flex-template build "${TEMPLATE_SPEC_GCS}" \
    --image="${IMAGE_URI}" \
    --sdk-language="PYTHON" \
    --metadata-file="pipeline/metadata.json" \
    --project="${PROJECT_ID}"

echo "=== 4. Deploying Airflow DAG to Cloud Composer ==="
gsutil cp dags/gcs_to_bq_dag.py "gs://${COMPOSER_DAG_BUCKET}/dags/"

echo "=== 5. Provisioning Cloud Monitoring Dashboard ==="
gcloud monitoring dashboards create \
    --config-from-file="monitoring/dashboard.json" \
    --project="${PROJECT_ID}" || echo "Dashboard creation skipped or already exists."

echo "=== 6. Creating BigQuery Cloud Logging Sink for Pipeline Auditing ==="
gcloud logging sinks create pipeline_audit_sink \
    bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/logs_ds \
    --log-filter='resource.type="dataflow_step" OR resource.type="cloud_composer_environment"' \
    --project="${PROJECT_ID}" || echo "Log sink creation skipped or already exists."

echo "=== Deployment Complete! ==="
echo "Flex Template Spec GCS Path: ${TEMPLATE_SPEC_GCS}"
echo "Airflow DAG deployed to: gs://${COMPOSER_DAG_BUCKET}/dags/gcs_to_bq_dag.py"
echo "Cloud Monitoring Dashboard deployed."
