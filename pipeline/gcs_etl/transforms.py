# Copyright 2026 ConEd / GCP Data Engineering
# Transforms module for Apache Beam GCS-to-BigQuery ETL pipeline.

import json
import logging
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.metrics import Metrics


class ProcessAndValidateRow(beam.DoFn):
    """
    Parses incoming raw lines (CSV format) and validates required schema fields.
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
            # CSV Parsing (id, timestamp, category, amount)
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
