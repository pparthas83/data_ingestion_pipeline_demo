# Copyright 2026 ConEd / GCP Data Engineering
# GCS to BigQuery ETL Dataflow Flex Template Pipeline.

import json
import logging
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

try:
    from gcs_etl.transforms import ProcessAndValidateRow
except ImportError:
    from pipeline.gcs_etl.transforms import ProcessAndValidateRow


class ETLPipelineOptions(PipelineOptions):
    """Custom Apache Beam PipelineOptions for Flex Template argument registration."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_pattern",
            default=None,
            help="GCS input file pattern, e.g. gs://my-bucket/landing/20260827/*.csv",
        )
        parser.add_argument(
            "--output_table",
            default=None,
            help="BigQuery output table spec: project:dataset.table",
        )
        parser.add_argument(
            "--dlq_table",
            default=None,
            help="BigQuery DLQ table spec: project:dataset.dlq_table",
        )


def run(argv=None):
    """Executes the Apache Beam pipeline with production telemetry."""
    pipeline_options = PipelineOptions(argv)
    etl_options = pipeline_options.view_as(ETLPipelineOptions)
    
    # Configure main session pickling and automatic package staging for worker nodes
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True
    if os.path.exists("/template/setup.py"):
        setup_options.setup_file = "/template/setup.py"

    # BigQuery Target Schema
    bq_target_schema = {
        "fields": [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    }

    # BigQuery DLQ Schema
    bq_dlq_schema = {
        "fields": [
            {"name": "raw_record", "type": "STRING", "mode": "NULLABLE"},
            {"name": "error_message", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
            {"name": "failed_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Read lines from GCS source
        raw_lines = p | "ReadFromGCS" >> beam.io.ReadFromText(etl_options.input_pattern)

        # 2. Process, validate, and split into Valid vs DLQ
        results = raw_lines | "ProcessAndValidate" >> beam.ParDo(
            ProcessAndValidateRow(), input_file_name=etl_options.input_pattern
        ).with_outputs(
            ProcessAndValidateRow.OUTPUT_TAG_VALID,
            ProcessAndValidateRow.OUTPUT_TAG_DLQ,
        )

        valid_records = results[ProcessAndValidateRow.OUTPUT_TAG_VALID]
        dlq_records = results[ProcessAndValidateRow.OUTPUT_TAG_DLQ]

        # 3. Write Valid Records to BigQuery Target Table (Native Batch File Loads)
        _ = valid_records | "WriteValidToBigQuery" >> WriteToBigQuery(
            table=etl_options.output_table,
            schema=bq_target_schema,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )

        # 4. Write DLQ Records to BigQuery Error Table (Native Batch File Loads)
        _ = dlq_records | "WriteDLQToBigQuery" >> WriteToBigQuery(
            table=etl_options.dlq_table,
            schema=bq_dlq_schema,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
