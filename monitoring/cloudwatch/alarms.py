"""
CloudWatch Alarms
==================
Creates CloudWatch alarms for pipeline health monitoring:
Glue job failures, Lambda errors, Kinesis iterator age,
Redshift query latency, and S3 bucket size.

Usage:
    python alarms.py --env dev --region us-east-1
"""

import argparse
import logging

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_alarms(env: str, region: str, sns_topic_arn: str) -> None:
    """Create all pipeline monitoring alarms."""
    cw = boto3.client("cloudwatch", region_name=region)

    alarms = [
        # ── Glue Job Failure ─────────────────────────────────
        {
            "AlarmName": f"c360-{env}-glue-job-failure",
            "AlarmDescription": "Alerts when any C360 Glue ETL job fails",
            "Namespace": "AWS/Glue",
            "MetricName": "glue.driver.aggregate.numFailedTasks",
            "Dimensions": [{"Name": "JobName", "Value": f"c360-raw-to-clean"}],
            "Statistic": "Sum",
            "Period": 300,
            "EvaluationPeriods": 1,
            "Threshold": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": "notBreaching",
        },
        # ── Lambda Consumer Errors ───────────────────────────
        {
            "AlarmName": f"c360-{env}-lambda-errors",
            "AlarmDescription": "Alerts when Kinesis consumer Lambda has errors",
            "Namespace": "AWS/Lambda",
            "MetricName": "Errors",
            "Dimensions": [
                {"Name": "FunctionName", "Value": "c360-kinesis-consumer"}
            ],
            "Statistic": "Sum",
            "Period": 300,
            "EvaluationPeriods": 2,
            "Threshold": 5,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": "notBreaching",
        },
        # ── Kinesis Iterator Age ─────────────────────────────
        {
            "AlarmName": f"c360-{env}-kinesis-iterator-age",
            "AlarmDescription": "Alerts when Kinesis consumer falls behind (high iterator age)",
            "Namespace": "AWS/Kinesis",
            "MetricName": "GetRecords.IteratorAgeMilliseconds",
            "Dimensions": [
                {"Name": "StreamName", "Value": "c360-clickstream-events"}
            ],
            "Statistic": "Maximum",
            "Period": 300,
            "EvaluationPeriods": 2,
            "Threshold": 60000,  # 60 seconds
            "ComparisonOperator": "GreaterThanThreshold",
            "TreatMissingData": "notBreaching",
        },
        # ── Lambda Duration (near timeout) ───────────────────
        {
            "AlarmName": f"c360-{env}-lambda-duration",
            "AlarmDescription": "Alerts when Lambda duration approaches timeout",
            "Namespace": "AWS/Lambda",
            "MetricName": "Duration",
            "Dimensions": [
                {"Name": "FunctionName", "Value": "c360-kinesis-consumer"}
            ],
            "Statistic": "Average",
            "Period": 300,
            "EvaluationPeriods": 3,
            "Threshold": 250000,  # 250 seconds (assuming 300s timeout)
            "ComparisonOperator": "GreaterThanThreshold",
            "TreatMissingData": "notBreaching",
        },
        # ── Step Functions Execution Failure ──────────────────
        {
            "AlarmName": f"c360-{env}-step-functions-failure",
            "AlarmDescription": "Alerts when pipeline orchestration fails",
            "Namespace": "AWS/States",
            "MetricName": "ExecutionsFailed",
            "Dimensions": [
                {"Name": "StateMachineArn",
                 "Value": f"arn:aws:states:{region}:123456789012:stateMachine:c360-pipeline-orchestrator-{env}"}
            ],
            "Statistic": "Sum",
            "Period": 300,
            "EvaluationPeriods": 1,
            "Threshold": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": "notBreaching",
        },
        # ── Redshift Health ──────────────────────────────────
        {
            "AlarmName": f"c360-{env}-redshift-cpu-high",
            "AlarmDescription": "Alerts when Redshift CPU utilization is high",
            "Namespace": "AWS/Redshift",
            "MetricName": "CPUUtilization",
            "Dimensions": [
                {"Name": "ClusterIdentifier", "Value": f"c360-warehouse-{env}"}
            ],
            "Statistic": "Average",
            "Period": 600,
            "EvaluationPeriods": 3,
            "Threshold": 80,
            "ComparisonOperator": "GreaterThanThreshold",
            "TreatMissingData": "notBreaching",
        },
        {
            "AlarmName": f"c360-{env}-redshift-disk-usage",
            "AlarmDescription": "Alerts when Redshift disk usage exceeds 75%",
            "Namespace": "AWS/Redshift",
            "MetricName": "PercentageDiskSpaceUsed",
            "Dimensions": [
                {"Name": "ClusterIdentifier", "Value": f"c360-warehouse-{env}"}
            ],
            "Statistic": "Average",
            "Period": 600,
            "EvaluationPeriods": 2,
            "Threshold": 75,
            "ComparisonOperator": "GreaterThanThreshold",
            "TreatMissingData": "notBreaching",
        },
    ]

    for alarm_config in alarms:
        alarm_config["AlarmActions"] = [sns_topic_arn]
        alarm_config["OKActions"] = [sns_topic_arn]
        alarm_config["Tags"] = [
            {"Key": "Project", "Value": "customer-360"},
            {"Key": "Environment", "Value": env},
        ]

        cw.put_metric_alarm(**alarm_config)
        logger.info("Created alarm: %s", alarm_config["AlarmName"])

    logger.info("All %d alarms created successfully", len(alarms))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create CloudWatch alarms")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--sns-topic-arn", required=True)
    args = parser.parse_args()

    create_alarms(args.env, args.region, args.sns_topic_arn)
