"""
CloudWatch Dashboard
=====================
Creates a CloudWatch dashboard for pipeline health monitoring.
"""

import json
import argparse
import logging

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_dashboard(env: str, region: str) -> None:
    """Create a CloudWatch dashboard for the C360 pipeline."""
    cw = boto3.client("cloudwatch", region_name=region)

    dashboard_body = {
        "widgets": [
            # ── Header ───────────────────────────────────────
            {
                "type": "text",
                "x": 0, "y": 0, "width": 24, "height": 1,
                "properties": {
                    "markdown": "# 🚀 Customer 360 Pipeline Dashboard"
                }
            },
            # ── Pipeline Execution Status ─────────────────────
            {
                "type": "metric",
                "x": 0, "y": 1, "width": 8, "height": 6,
                "properties": {
                    "title": "Pipeline Executions",
                    "metrics": [
                        ["AWS/States", "ExecutionsSucceeded",
                         "StateMachineArn", f"arn:aws:states:{region}:123456789012:stateMachine:c360-pipeline-orchestrator-{env}",
                         {"label": "Succeeded", "color": "#2ca02c"}],
                        ["AWS/States", "ExecutionsFailed",
                         "StateMachineArn", f"arn:aws:states:{region}:123456789012:stateMachine:c360-pipeline-orchestrator-{env}",
                         {"label": "Failed", "color": "#d62728"}],
                    ],
                    "period": 3600,
                    "stat": "Sum",
                    "view": "timeSeries",
                    "region": region
                }
            },
            # ── Glue Job Duration ────────────────────────────
            {
                "type": "metric",
                "x": 8, "y": 1, "width": 8, "height": 6,
                "properties": {
                    "title": "Glue Job Duration (minutes)",
                    "metrics": [
                        ["AWS/Glue", "glue.driver.aggregate.elapsedTime",
                         "JobName", "c360-raw-to-clean",
                         {"label": "Raw→Clean"}],
                        ["AWS/Glue", "glue.driver.aggregate.elapsedTime",
                         "JobName", "c360-clean-to-curated",
                         {"label": "Clean→Curated"}],
                        ["AWS/Glue", "glue.driver.aggregate.elapsedTime",
                         "JobName", "c360-curated-to-redshift",
                         {"label": "Curated→Redshift"}],
                    ],
                    "period": 3600,
                    "stat": "Average",
                    "view": "timeSeries",
                    "region": region
                }
            },
            # ── Kinesis Metrics ──────────────────────────────
            {
                "type": "metric",
                "x": 16, "y": 1, "width": 8, "height": 6,
                "properties": {
                    "title": "Kinesis Stream Health",
                    "metrics": [
                        ["AWS/Kinesis", "IncomingRecords",
                         "StreamName", "c360-clickstream-events",
                         {"label": "Incoming Records", "stat": "Sum"}],
                        ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds",
                         "StreamName", "c360-clickstream-events",
                         {"label": "Iterator Age (ms)", "stat": "Maximum", "yAxis": "right"}],
                    ],
                    "period": 300,
                    "view": "timeSeries",
                    "region": region
                }
            },
            # ── Lambda Consumer ──────────────────────────────
            {
                "type": "metric",
                "x": 0, "y": 7, "width": 8, "height": 6,
                "properties": {
                    "title": "Lambda Consumer",
                    "metrics": [
                        ["AWS/Lambda", "Invocations",
                         "FunctionName", "c360-kinesis-consumer",
                         {"label": "Invocations", "color": "#1f77b4"}],
                        ["AWS/Lambda", "Errors",
                         "FunctionName", "c360-kinesis-consumer",
                         {"label": "Errors", "color": "#d62728"}],
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "view": "timeSeries",
                    "region": region
                }
            },
            # ── Redshift Performance ─────────────────────────
            {
                "type": "metric",
                "x": 8, "y": 7, "width": 8, "height": 6,
                "properties": {
                    "title": "Redshift Performance",
                    "metrics": [
                        ["AWS/Redshift", "CPUUtilization",
                         "ClusterIdentifier", f"c360-warehouse-{env}",
                         {"label": "CPU %", "color": "#ff7f0e"}],
                        ["AWS/Redshift", "PercentageDiskSpaceUsed",
                         "ClusterIdentifier", f"c360-warehouse-{env}",
                         {"label": "Disk %", "color": "#9467bd"}],
                    ],
                    "period": 600,
                    "stat": "Average",
                    "view": "timeSeries",
                    "region": region
                }
            },
            # ── S3 Bucket Size ───────────────────────────────
            {
                "type": "metric",
                "x": 16, "y": 7, "width": 8, "height": 6,
                "properties": {
                    "title": "S3 Data Lake Size (GB)",
                    "metrics": [
                        ["AWS/S3", "BucketSizeBytes",
                         "BucketName", f"c360-raw-{env}", "StorageType", "StandardStorage",
                         {"label": "Raw Layer"}],
                        ["AWS/S3", "BucketSizeBytes",
                         "BucketName", f"c360-clean-{env}", "StorageType", "StandardStorage",
                         {"label": "Clean Layer"}],
                        ["AWS/S3", "BucketSizeBytes",
                         "BucketName", f"c360-curated-{env}", "StorageType", "StandardStorage",
                         {"label": "Curated Layer"}],
                    ],
                    "period": 86400,
                    "stat": "Average",
                    "view": "timeSeries",
                    "region": region
                }
            },
        ]
    }

    cw.put_dashboard(
        DashboardName=f"C360-Pipeline-{env}",
        DashboardBody=json.dumps(dashboard_body)
    )
    logger.info("Dashboard created: C360-Pipeline-%s", env)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create CloudWatch dashboard")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()
    create_dashboard(args.env, args.region)
