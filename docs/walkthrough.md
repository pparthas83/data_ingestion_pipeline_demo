# Verification Walkthrough & Empirical Proof

This walkthrough provides complete empirical proof of offline unit testing and live GCP Cloud Dataflow execution for the **GCS to BigQuery ETL Dataflow Flex Template Pipeline** and **Cloud Composer Airflow DAG**.

---

## 1. Automated Test Suite (Offline Verification)

We validated both the **Apache Beam pipeline transforms** and the **Airflow DAG orchestration structure** locally via Python `unittest`.

### Executed Command
```bash
.venv/bin/python3 -m unittest discover -s tests -v
```

### Test Results
```text
test_dag_loaded (test_dag.TestGCSNotificationDAG.test_dag_loaded) ... ok
test_dag_structure_and_task_count (test_dag.TestGCSNotificationDAG.test_dag_structure_and_task_count) ... ok
test_task_dependencies (test_dag.TestGCSNotificationDAG.test_task_dependencies) ... ok
test_failure_scenario_all_corrupt_records (test_pipeline.TestGCSIngestionPipeline) ... ok
test_mixed_records_routing (test_pipeline.TestGCSIngestionPipeline) ... ok
test_success_scenario_all_valid_records (test_pipeline.TestGCSIngestionPipeline) ... ok

----------------------------------------------------------------------
Ran 6 tests in 6.350s

OK
```

---

## 2. Live Cloud Execution Verification

### Dataflow Job Details
- **Job Name**: `gcs-to-bq-etl-1787941124`
- **Job ID**: `2026-08-28_11_18_46-13579482598464932234`
- **Region**: `us-central1`
- **Project**: `pradeep-demo-1`
- **Execution Status**: **`JOB_STATE_DONE`**

---

## 3. BigQuery Ingestion Results

```sql
SELECT "Target Records" AS table_name, COUNT(1) AS count FROM `pradeep-demo-1.analytics_ds.target_records`
UNION ALL
SELECT "DLQ Records" AS table_name, COUNT(1) AS count FROM `pradeep-demo-1.analytics_ds.target_records_dlq`;
```

### Table Row Counts
| Table Name | Record Count | Description |
| :--- | :---: | :--- |
| **Target Records** (`analytics_ds.target_records`) | **180** | Valid parsed CSV records |
| **DLQ Records** (`analytics_ds.target_records_dlq`) | **70** | Invalid/corrupt CSV records captured |

---

## 4. Sample Query Output (BigQuery Target Records)

| `id` | `timestamp` | `category` | `amount` | `ingested_at` |
| :--- | :--- | :--- | :--- | :--- |
| `TXN-100002` | `2026-08-27 00:03:40` | `HOME` | `342.5` | `2026-08-28 18:24:34` |
| `TXN-100039` | `2026-08-27 00:57:40` | `AUTOMOTIVE` | `1384.28` | `2026-08-28 18:24:34` |
| `TXN-100005` | `2026-08-27 00:44:40` | `AUTOMOTIVE` | `745.74` | `2026-08-28 18:24:34` |
| `TXN-100079` | `2026-08-27 00:16:40` | `AUTOMOTIVE` | `1161.09` | `2026-08-28 18:24:34` |
| `TXN-100038` | `2026-08-27 00:05:40` | `ELECTRONICS` | `913.86` | `2026-08-28 18:24:34` |

---

## 5. Sample Query Output (BigQuery DLQ Records)

| `raw_record` | `error_message` | `source_file` | `failed_at` |
| :--- | :--- | :--- | :--- |
| `,2026-08-27 00:59:40,ELECTRONICS,100.50` | `Field 'id' cannot be empty` | `gs://pradeep-demo-1-raw-data/landing/20260827/*.csv` | `2026-08-28 18:24:33` |
| `,2026-08-27 00:59:40,AUTOMOTIVE,100.50` | `Field 'id' cannot be empty` | `gs://pradeep-demo-1-raw-data/landing/20260827/*.csv` | `2026-08-28 18:24:33` |

---

## 6. Root Causes Addressed

1. **Python Module Distribution on Dataflow Worker Nodes**:
   - Packaged custom transforms into package `gcs_etl` and configured `setup_options.setup_file = "/template/setup.py"` in `pipeline/main.py`.
   - Dataflow worker VMs automatically install `gcs_etl` upon startup from GCS staging, preventing `ModuleNotFoundError`.

2. **BigQuery IAM Permission**:
   - Granted IAM role `roles/bigquery.jobUser` (`bigquery.jobs.create`) to `832497031659-compute@developer.gserviceaccount.com`, allowing worker batch file load jobs into BigQuery.

3. **Airflow DAG Compatibility**:
   - Updated `dags/gcs_to_bq_dag.py` parameter `schedule_interval` -> `schedule` and `wait_for_pipeline` -> `wait_until_finished` for modern Airflow / Composer compatibility.
