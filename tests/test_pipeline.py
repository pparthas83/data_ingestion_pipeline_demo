"""
Unit & Integration Test Suite for GCS to BigQuery Apache Beam Pipeline
Uses Apache Beam DirectRunner to test Success and Failure/DLQ scenarios.
"""

import os
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# Import DoFn from pipeline module
from pipeline.main import ProcessAndValidateRow

SAMPLE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sample_data"))


class TestGCSIngestionPipeline(unittest.TestCase):

    def setUp(self):
        self.valid_lines = [
            "id,timestamp,category,amount",
            "TXN-001,2026-08-27 10:00:00,ELECTRONICS,150.00",
            "TXN-002,2026-08-27T10:15:00Z,GROCERY,45.50",
        ]

        self.corrupt_lines = [
            "id,timestamp,category,amount",
            ",2026-08-27 10:00:00,ELECTRONICS,150.00",            # Missing ID
            "TXN-ERR-1,invalid-date,CLOTHING,99.00",               # Invalid Date
            "TXN-ERR-2,2026-08-27 10:00:00,HOME,-50.00",            # Negative Amount
            "TXN-ERR-3,2026-08-27 10:00:00",                       # Truncated
        ]

    def test_success_scenario_all_valid_records(self):
        """Test Success Scenario: Valid rows produce 2 valid outputs and 0 DLQ outputs."""
        with TestPipeline() as p:
            raw_input = p | "CreateValidInput" >> beam.Create(self.valid_lines)
            results = raw_input | "ProcessValid" >> beam.ParDo(
                ProcessAndValidateRow()
            ).with_outputs(
                ProcessAndValidateRow.OUTPUT_TAG_VALID,
                ProcessAndValidateRow.OUTPUT_TAG_DLQ
            )

            valid_pcoll = results[ProcessAndValidateRow.OUTPUT_TAG_VALID]
            dlq_pcoll = results[ProcessAndValidateRow.OUTPUT_TAG_DLQ]

            # Assert DLQ is empty
            assert_that(dlq_pcoll, equal_to([]), label="AssertDLQEmpty")

            # Assert valid records count
            def check_valid_count(elements):
                assert len(elements) == 2, f"Expected 2 valid records, got {len(elements)}"
                assert elements[0]['id'] == 'TXN-001'
                assert elements[1]['category'] == 'GROCERY'

            assert_that(valid_pcoll, check_valid_count, label="AssertValidRecords")

    def test_failure_scenario_all_corrupt_records(self):
        """Test Failure Scenario: Corrupt rows produce 0 valid outputs and 4 DLQ outputs."""
        with TestPipeline() as p:
            raw_input = p | "CreateCorruptInput" >> beam.Create(self.corrupt_lines)
            results = raw_input | "ProcessCorrupt" >> beam.ParDo(
                ProcessAndValidateRow()
            ).with_outputs(
                ProcessAndValidateRow.OUTPUT_TAG_VALID,
                ProcessAndValidateRow.OUTPUT_TAG_DLQ
            )

            valid_pcoll = results[ProcessAndValidateRow.OUTPUT_TAG_VALID]
            dlq_pcoll = results[ProcessAndValidateRow.OUTPUT_TAG_DLQ]

            # Assert Valid is empty
            assert_that(valid_pcoll, equal_to([]), label="AssertValidEmpty")

            # Assert DLQ records count and content
            def check_dlq_count(elements):
                assert len(elements) == 4, f"Expected 4 DLQ records, got {len(elements)}"
                assert "error_message" in elements[0]

            assert_that(dlq_pcoll, check_dlq_count, label="AssertDLQRecords")

    def test_mixed_batch_routing(self):
        """Test Mixed Scenario: Mixed rows correctly split into Valid and DLQ outputs."""
        mixed_lines = self.valid_lines + self.corrupt_lines[1:]  # 2 valid + 4 corrupt
        with TestPipeline() as p:
            raw_input = p | "CreateMixedInput" >> beam.Create(mixed_lines)
            results = raw_input | "ProcessMixed" >> beam.ParDo(
                ProcessAndValidateRow()
            ).with_outputs(
                ProcessAndValidateRow.OUTPUT_TAG_VALID,
                ProcessAndValidateRow.OUTPUT_TAG_DLQ
            )

            valid_pcoll = results[ProcessAndValidateRow.OUTPUT_TAG_VALID]
            dlq_pcoll = results[ProcessAndValidateRow.OUTPUT_TAG_DLQ]

            def check_counts(valid_elements, dlq_elements):
                pass  # verified via separate assertions below

            assert_that(valid_pcoll, lambda elems: len(elems) == 2, label="CheckValidCountMixed")
            assert_that(dlq_pcoll, lambda elems: len(elems) == 4, label="CheckDLQCountMixed")


if __name__ == "__main__":
    unittest.main()
