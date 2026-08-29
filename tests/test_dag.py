import unittest
from airflow.models import DagBag


class TestGCSNotificationDAG(unittest.TestCase):
    """Unit tests for Airflow DAG structure and compilation."""

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(dag_folder="dags")

    def test_dag_loaded(self):
        """Verify DAG imports cleanly without any syntax or import errors."""
        self.assertEqual(len(self.dagbag.import_errors), 0, f"DAG import errors: {self.dagbag.import_errors}")
        self.assertIn("gcs_to_bq_dataflow_etl", self.dagbag.dags)

    def test_dag_structure_and_task_count(self):
        """Verify expected tasks and DAG configuration."""
        dag = self.dagbag.get_dag("gcs_to_bq_dataflow_etl")
        self.assertEqual(len(dag.tasks), 3)

        task_ids = set(dag.task_ids)
        expected_task_ids = {
            "wait_for_gcs_landing_files",
            "launch_dataflow_flex_template",
            "audit_dlq_quality_threshold",
        }
        self.assertEqual(task_ids, expected_task_ids)

    def test_task_dependencies(self):
        """Verify task execution chain: sensor >> flex template >> audit."""
        dag = self.dagbag.get_dag("gcs_to_bq_dataflow_etl")

        sensor_task = dag.get_task("wait_for_gcs_landing_files")
        dataflow_task = dag.get_task("launch_dataflow_flex_template")
        audit_task = dag.get_task("audit_dlq_quality_threshold")

        self.assertIn(dataflow_task, sensor_task.downstream_list)
        self.assertIn(audit_task, dataflow_task.downstream_list)


if __name__ == "__main__":
    unittest.main()
