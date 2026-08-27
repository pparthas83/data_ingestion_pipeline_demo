import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class ProcessAndValidateRow(beam.DoFn):
    """
    Parses incoming raw lines (CSV or JSON format) and validates required schema fields.
    Routes valid records to OUTPUT_TAG_VALID and malformed records to OUTPUT_TAG_DLQ.
    Tracks production metrics via Beam Counters.
    """

    OUTPUT_TAG_VALID = "valid_records"
    OUTPUT_TAG_DLQ = "dlq_records"

    def __init__(self):
        super().__init__()
        # Custom Beam Metric Counters for Cloud Monitoring
        self.processed_counter = Metrics.counter("pipeline_telemetry", "processed_records")
        self.valid_counter = Metrics.counter("pipeline_telemetry", "valid_records")
        self.dlq_counter = Metrics.counter("pipeline_telemetry", "dlq_records")

    def process(self, element, input_file_name=None):
        self.processed_counter.inc()
        line = element.strip()
        if not line:
            return

        try:
            # Example: CSV Parsing (id, timestamp, category, amount)
            fields = [f.strip() for f in line.split(",")]

            # Skip header if present
            if fields[0].lower() in ("id", "transaction_id") or fields[0].startswith("#"):
                return

            if len(fields) < 4:
                raise ValueError(
                    f"Invalid record length: Expected at least 4 comma-separated values, got {len(fields)}"
                )

            record_id, raw_ts, category, raw_amount = fields[0], fields[1], fields[2], fields[3]

            if not record_id:
                raise ValueError("Field 'id' cannot be empty")

            # Validate Timestamp
            try:
                if "T" in raw_ts:
                    parsed_ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                else:
                    parsed_ts = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S")
                timestamp_str = parsed_ts.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                raise ValueError(f"Invalid timestamp format '{raw_ts}': {str(e)}")

            # Validate Amount
            try:
                amount = float(raw_amount)
                if amount < 0:
                    raise ValueError(f"Amount cannot be negative: {amount}")
            except ValueError as e:
                raise ValueError(f"Invalid numeric amount '{raw_amount}': {str(e)}")

            # Construct Valid Record dictionary
            valid_record = {
                "id": record_id,
                "timestamp": timestamp_str,
                "category": category.upper() if category else "UNKNOWN",
                "amount": amount,
                "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }

            self.valid_counter.inc()
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_VALID, valid_record)

        except Exception as err:
            self.dlq_counter.inc()
            # Structured Logging for Cloud Logging ingestion
            log_payload = {
                "severity": "WARNING",
                "event": "dlq_record_captured",
                "error_message": str(err),
                "raw_record": str(element),
                "source_file": input_file_name or "unknown",
            }
            logging.warning(json.dumps(log_payload))

            dlq_record = {
                "raw_record": str(element),
                "error_message": str(err),
                "source_file": input_file_name or "unknown",
                "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_DLQ, dlq_record)


def run(argv=None):
    """Executes the Apache Beam pipeline with production telemetry."""
    parser = argparse.ArgumentParser(description="GCS to BigQuery Ingestion Beam Pipeline")
    parser.add_argument(
        "--input_pattern",
        required=True,
        help="GCS input file pattern, e.g. gs://my-bucket/landing/20260827/*.csv",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery output table spec: project:dataset.table",
    )
    parser.add_argument(
        "--dlq_table",
        required=True,
        help="BigQuery DLQ table spec: project:dataset.dlq_table",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

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
        raw_lines = p | "ReadFromGCS" >> beam.io.ReadFromText(known_args.input_pattern)

        # 2. Process, validate, and split into Valid vs DLQ
        results = raw_lines | "ProcessAndValidate" >> beam.ParDo(
            ProcessAndValidateRow(), input_file_name=known_args.input_pattern
        ).with_outputs(
            ProcessAndValidateRow.OUTPUT_TAG_VALID,
            ProcessAndValidateRow.OUTPUT_TAG_DLQ,
        )

        valid_records = results[ProcessAndValidateRow.OUTPUT_TAG_VALID]
        dlq_records = results[ProcessAndValidateRow.OUTPUT_TAG_DLQ]

        # 3. Write Valid Records to BigQuery Target Table (Storage Write API)
        _ = valid_records | "WriteValidToBigQuery" >> WriteToBigQuery(
            table=known_args.output_table,
            schema=bq_target_schema,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        )

        # 4. Write DLQ Records to BigQuery Error Table
        _ = dlq_records | "WriteDLQToBigQuery" >> WriteToBigQuery(
            table=known_args.dlq_table,
            schema=bq_dlq_schema,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
