"""
Kinesis → S3 Raw Layer Consumer (AWS Lambda)
=============================================
Triggered by a Kinesis Data Stream event source mapping.
Parses incoming clickstream/transaction events, enriches with
ingestion metadata, and writes raw JSON to S3 partitioned by
year/month/day/hour.

Environment Variables:
    RAW_BUCKET  — S3 bucket for the Raw layer
    RAW_PREFIX  — S3 key prefix (default: "raw/")
"""

import json
import os
import uuid
import logging
import base64
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

RAW_BUCKET = os.environ.get("RAW_BUCKET", "c360-raw-dev")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")


def _build_s3_key(event_type: str, ts: datetime) -> str:
    """
    Build a partition-aware S3 key.

    Pattern:
        raw/events/{event_type}/year=YYYY/month=MM/day=DD/hour=HH/{uuid}.json

    Hive-style partitioning enables efficient Athena / Glue queries.
    """
    return (
        f"{RAW_PREFIX}events/{event_type}/"
        f"year={ts.strftime('%Y')}/"
        f"month={ts.strftime('%m')}/"
        f"day={ts.strftime('%d')}/"
        f"hour={ts.strftime('%H')}/"
        f"{uuid.uuid4().hex}.json"
    )


def _enrich_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Add ingestion metadata to the raw record."""
    record["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    record["_source"] = "kinesis"
    record["_pipeline_version"] = "1.0.0"
    return record


def _write_batch_to_s3(records: List[Dict[str, Any]], event_type: str) -> int:
    """
    Write a batch of records as a single newline-delimited JSON file to S3.
    Returns the number of records written.
    """
    if not records:
        return 0

    now = datetime.now(timezone.utc)
    key = _build_s3_key(event_type, now)
    body = "\n".join(json.dumps(r) for r in records)

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="aws:kms",
    )
    logger.info("Wrote %d records → s3://%s/%s", len(records), RAW_BUCKET, key)
    return len(records)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda entry point — processes a batch of Kinesis records.

    Parameters
    ----------
    event : dict
        Kinesis event with Records[].kinesis.data (base64-encoded JSON).
    context : LambdaContext
        AWS Lambda runtime context.

    Returns
    -------
    dict
        Processing summary with counts per event type.
    """
    logger.info(
        "Received %d records from Kinesis (function=%s)",
        len(event.get("Records", [])),
        context.function_name if context else "local"
    )

    # Group records by event_type for efficient batched writes
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    parse_errors = 0

    for kinesis_record in event.get("Records", []):
        try:
            payload = base64.b64decode(
                kinesis_record["kinesis"]["data"]
            ).decode("utf-8")
            record = json.loads(payload)
            record = _enrich_record(record)

            event_type = record.get("event_type", "unknown")
            buckets.setdefault(event_type, []).append(record)

        except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as exc:
            parse_errors += 1
            logger.error(
                "Failed to parse record seq=%s: %s",
                kinesis_record.get("kinesis", {}).get("sequenceNumber", "?"),
                exc
            )

    # Write each event-type batch to S3
    summary: Dict[str, int] = {}
    for event_type, records in buckets.items():
        count = _write_batch_to_s3(records, event_type)
        summary[event_type] = count

    result = {
        "statusCode": 200,
        "processed": sum(summary.values()),
        "parse_errors": parse_errors,
        "by_event_type": summary
    }
    logger.info("Processing complete: %s", json.dumps(result))
    return result


# ── Local Testing ────────────────────────────────────────────
if __name__ == "__main__":
    # Simulate a Kinesis event for local testing
    sample_event = {
        "event_id": "test-001",
        "event_type": "page_view",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": "CUST-000001",
        "session_id": "sess-abc-123",
        "page_url": "/products/electronics",
        "channel": "web"
    }
    test_kinesis_event = {
        "Records": [
            {
                "kinesis": {
                    "data": base64.b64encode(
                        json.dumps(sample_event).encode()
                    ).decode(),
                    "sequenceNumber": "0001"
                }
            }
        ]
    }
    print(json.dumps(
        lambda_handler(test_kinesis_event, None),
        indent=2
    ))
