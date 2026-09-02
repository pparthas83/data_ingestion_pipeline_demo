import unittest
from airflow.models import DagBag


class TestDataformDAG(unittest.TestCase):
    """
    Automated unit test suite verifying Cloud Composer DAG loading, syntax, and task dependency structure.
    """

    def setUp(self):
        self.dagbag = DagBag(dag_folder="dataform_pipeline/dags")

    def test_dag_loaded(self):
        """Ensure DAG parses cleanly without import errors or syntax exceptions."""
        self.assertFalse(
            bool(self.dagbag.import_errors),
            f"Airflow DAG Import Errors: {self.dagbag.import_errors}"
        )
        dag = self.dagbag.get_dag("dataform_bq_transform_migration")
        self.assertIsNotNone(dag, "DAG 'dataform_bq_transform_migration' failed to load.")
        self.assertEqual(len(dag.tasks), 3, f"Expected 3 tasks in DAG, found {len(dag.tasks)}.")

    def test_task_dependencies(self):
        """Verify downstream task execution sequence: Compilation -> Invocation -> Sensor."""
        dag = self.dagbag.get_dag("dataform_bq_transform_migration")

        compilation_task = dag.get_task("create_compilation_result")
        invocation_task = dag.get_task("create_workflow_invocation")
        sensor_task = dag.get_task("wait_for_workflow_invocation")

        self.assertIn(invocation_task, compilation_task.downstream_list)
        self.assertIn(sensor_task, invocation_task.downstream_list)


if __name__ == "__main__":
    unittest.main()
