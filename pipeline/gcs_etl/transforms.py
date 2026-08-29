"""
Transforms module for Apache Beam GCS-to-BigQuery ETL pipeline.
"""

import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.metrics import Metrics

logger = logging.getLogger(__name__)


class ParseAndValidateCSVDoFn(beam.DoFn):
    """
    Custom Apache Beam DoFn to parse raw CSV lines, validate schemas,
    and route records to Output Tags:
      - OUTPUT_TAG_VALID: Structured dictionary for BigQuery Target table.
      - OUTPUT_TAG_DLQ: Error details dictionary for BigQuery Dead Letter Queue (DLQ).
    """

    OUTPUT_TAG_VALID = "valid_records"
    OUTPUT_TAG_DLQ = "dlq_records"

    def __init__(self):
        super().__init__()
        # Apache Beam Telemetry Metric Counters
        self.processed_counter = Metrics.counter(self.__class__, "processed_records")
        self.valid_counter = Metrics.counter(self.__class__, "valid_records")
        self.dlq_counter = Metrics.counter(self.__class__, "dlq_records")

    def process(self, element, window=beam.DoFn.WindowParam, input_file_name=None):
        """
        Process a single CSV line element.
        Format expected: id,timestamp,category,amount
        """
        self.processed_counter.inc()
        raw_line = element.strip() if isinstance(element, str) else str(element)

        if not raw_line:
            return  # Skip empty lines

        try:
            fields = [f.strip() for f in raw_line.split(",")]
            if len(fields) < 4:
                raise ValueError(
                    f"Invalid record length: Expected at least 4 comma-separated values, got {len(fields)}"
                )

            raw_id, raw_ts, category, raw_amount = fields[0], fields[1], fields[2], fields[3]

            # Validate ID
            if not raw_id:
                raise ValueError("Field 'id' cannot be empty")

            # Validate Timestamp
            try:
                if "T" in raw_ts:
                    parsed_ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                else:
                    parsed_ts = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                timestamp_str = parsed_ts.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                raise ValueError(f"Invalid timestamp format '{raw_ts}': {e!s}") from e

            # Validate Amount
            try:
                amount = float(raw_amount)
                if amount < 0:
                    raise ValueError(f"Amount cannot be negative: {amount}")
            except ValueError as e:
                raise ValueError(f"Invalid numeric amount '{raw_amount}': {e!s}") from e

            # Construct Valid Record dictionary
            valid_record = {
                "id": raw_id,
                "timestamp": timestamp_str,
                "category": category if category else "UNSPECIFIED",
                "amount": amount,
                "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }

            self.valid_counter.inc()
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_VALID, valid_record)

        except Exception as err:  # noqa: BLE001
            self.dlq_counter.inc()
            # Structured Logging for Cloud Logging ingestion
            log_payload = {
                "severity": "WARNING",
                "event": "dlq_record_captured",
                "error_message": str(err),
                "raw_record": raw_line,
                "source_file": input_file_name or "unknown",
            }
            logger.warning(json.dumps(log_payload))

            dlq_record = {
                "raw_record": raw_line,
                "error_message": str(err),
                "source_file": input_file_name or "unknown",
                "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_DLQ, dlq_record)
