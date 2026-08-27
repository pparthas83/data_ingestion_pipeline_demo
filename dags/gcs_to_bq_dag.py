"""
Airflow DAG: gcs_to_bq_dataflow_etl
Description: Cloud Composer orchestration DAG featuring:
             - Non-blocking deferrable GCS sensors
             - Dataflow Flex Template execution
             - Post-ETL BigQuery DLQ threshold quality checks
             - Failure and SLA miss callback alerts
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor


def on_failure_callback(context):
    """
    Callback function executed automatically when any DAG task fails.
    Routes incident details to alerting system (Slack/PubSub/Email).
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    alert_message = {
        "severity": "CRITICAL",
        "event": "airflow_task_failure",
        "dag_id": dag_id,
        "task_id": task_instance.task_id if task_instance else "unknown",
        "execution_date": str(execution_date),
        "exception": str(exception),
    }
    logging.error(f"[ALERT CALLBACK] Task Failure Incident: {alert_message}")
    # In production, publish alert_message to Cloud Pub/Sub or Slack Webhook


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback executed when execution exceeds defined SLA window.
    """
    logging.warning(f"[SLA MISS CALLBACK] Pipeline Execution SLA breached for DAG: {dag.dag_id}")


# Default DAG Arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,
    'sla': timedelta(minutes=45),  # SLA Alert if DAG run takes > 45 minutes
}

# Environment & Variable Configuration
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="YOUR_GCP_PROJECT_ID")
GCP_REGION = Variable.get("gcp_region", default_var="us-central1")
RAW_DATA_BUCKET = Variable.get("raw_data_bucket", default_var="YOUR_RAW_DATA_BUCKET")
TEMP_BUCKET = Variable.get("temp_bucket", default_var="YOUR_TEMP_BUCKET")
FLEX_TEMPLATE_SPEC_GCS = Variable.get(
    "flex_template_spec_gcs",
    default_var="gs://YOUR_TEMP_BUCKET/templates/gcs_to_bq_spec.json"
)
DATAFLOW_SERVICE_ACCOUNT = Variable.get(
    "dataflow_service_account",
    default_var="dataflow-worker-sa@YOUR_GCP_PROJECT_ID.iam.gserviceaccount.com"
)

TARGET_DATASET = Variable.get("target_dataset", default_var="analytics_ds")
TARGET_TABLE_NAME = Variable.get("target_table_name", default_var="target_records")
MAX_DLQ_THRESHOLD = int(Variable.get("max_dlq_threshold", default_var="50"))

with DAG(
    dag_id='gcs_to_bq_dataflow_etl',
    default_args=default_args,
    description='Orchestrate GCS to BQ ETL via Dataflow Flex Template with Deferrable Sensor & Quality Auditing',
    schedule_interval='0 2 * * *',  # Daily at 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    tags=['gcs', 'dataflow', 'bigquery', 'etl', 'monitoring', 'quality'],
) as dag:

    # 1. Non-blocking Deferrable GCS Sensor
    wait_for_gcs_files = GCSObjectsWithPrefixExistenceSensor(
        task_id='wait_for_gcs_landing_files',
        bucket=RAW_DATA_BUCKET,
        prefix='landing/{{ ds_nodash }}/',
        deferrable=True,   # Non-blocking sensor mode
        timeout=3600,      # Wait up to 1 hour
        poke_interval=120, # Poll every 2 mins
    )

    # 2. Trigger Dataflow Flex Template Pipeline Job
    launch_dataflow_flex_template = DataflowStartFlexTemplateOperator(
        task_id='launch_dataflow_flex_template',
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        wait_for_pipeline=True,
        body={
            "launchParameter": {
                "jobName": "gcs-to-bq-etl-{{ ds_nodash }}",
                "containerSpecGcsPath": FLEX_TEMPLATE_SPEC_GCS,
                "parameters": {
                    "input_pattern": f"gs://{RAW_DATA_BUCKET}/landing/{{{{ ds_nodash }}}}/*.csv",
                    "output_table": f"{GCP_PROJECT_ID}:{TARGET_DATASET}.{TARGET_TABLE_NAME}",
                    "dlq_table": f"{GCP_PROJECT_ID}:{TARGET_DATASET}.{TARGET_TABLE_NAME}_dlq",
                },
                "environment": {
                    "tempLocation": f"gs://{TEMP_BUCKET}/dataflow/temp",
                    "stagingLocation": f"gs://{TEMP_BUCKET}/dataflow/staging",
                    "serviceAccountEmail": DATAFLOW_SERVICE_ACCOUNT,
                    "ipConfiguration": "WORKER_IP_PRIVATE",
                    "network": Variable.get("vpc_network", default_var="pradeep-demo-vpc"),
                },
            }
        },
    )

    # 3. Post-ETL Quality Task: Audit DLQ Record Count against MAX_DLQ_THRESHOLD
    audit_dlq_quality_threshold = BigQueryInsertJobOperator(
        task_id='audit_dlq_quality_threshold',
        configuration={
            "query": {
                "query": f"""
                    DECLARE dlq_count INT64;
                    SET dlq_count = (
                        SELECT COUNT(1)
                        FROM `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE_NAME}_dlq`
                        WHERE DATE(failed_at) = CURRENT_DATE()
                          AND source_file LIKE '%{{{{ ds_nodash }}}}%'
                    );

                    IF dlq_count > {MAX_DLQ_THRESHOLD} THEN
                        ERROR(CONCAT('Data Quality Threshold Breach: DLQ record count (', CAST(dlq_count AS STRING), ') exceeds allowed threshold of {MAX_DLQ_THRESHOLD}'));
                    END IF;
                """,
                "useLegacySql": False,
            }
        },
    )

    # DAG Task Dependency Order
    wait_for_gcs_files >> launch_dataflow_flex_template >> audit_dlq_quality_threshold
