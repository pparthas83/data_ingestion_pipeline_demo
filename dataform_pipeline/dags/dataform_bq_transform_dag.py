"""
Airflow DAG: dataform_bq_transform_migration
Description: Standalone Cloud Composer DAG orchestrating BigQuery table transformations & migrations via Google Cloud Dataform Core.
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import (
    DataformWorkflowInvocationStateSensor,
)
from google.cloud.dataform_v1beta1 import WorkflowInvocation

GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="YOUR_GCP_PROJECT_ID")
GCP_REGION = Variable.get("gcp_region", default_var="us-central1")
DATAFORM_REPOSITORY_ID = Variable.get("dataform_repository_id", default_var="coned-dataform-repo")

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=30),
}

with DAG(
    dag_id='dataform_bq_transform_migration',
    default_args=default_args,
    description='Orchestrate BigQuery Dataform transformations & migrations via Cloud Composer',
    schedule='0 3 * * *',  # Daily at 03:00 UTC
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=['dataform', 'bigquery', 'composer', 'silver2', 'transformation'],
) as dag:

    # 1. Create Dataform Compilation Result
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id='create_compilation_result',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "code_compilation_config": {
                "default_database": GCP_PROJECT_ID,
                "vars": {
                    "execution_date": "{{ ds }}"
                }
            }
        },
    )

    # 2. Invoke Dataform Workflow Execution (asynchronous dispatch)
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        asynchronous=True,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull(task_ids='create_compilation_result')['name'] }}",
            "invocation_config": {
                "included_tags": ["silver2_transform"],
                "transitive_dependencies_included": True
            }
        },
    )

    # 3. Sensor: Monitor Dataform Workflow Invocation until SUCCEEDED
    wait_for_workflow_invocation = DataformWorkflowInvocationStateSensor(
        task_id='wait_for_workflow_invocation',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation_id="{{ task_instance.xcom_pull(task_ids='create_workflow_invocation')['name'].split('/')[-1] }}",
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
        poke_interval=30,
        timeout=1800,
    )

    # DAG Task Graph
    create_compilation_result >> create_workflow_invocation >> wait_for_workflow_invocation
