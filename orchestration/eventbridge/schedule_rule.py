"""
EventBridge Schedule Rule
==========================
Creates an EventBridge rule to trigger the pipeline state machine
on a recurring schedule (hourly or daily).

Usage:
    python schedule_rule.py --env dev --schedule "rate(1 hour)"
"""

import argparse
import json
import logging

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_schedule_rule(
    env: str,
    region: str,
    account_id: str,
    schedule_expression: str = "rate(1 hour)"
) -> None:
    """
    Create an EventBridge rule that triggers the pipeline state machine.
    """
    events = boto3.client("events", region_name=region)

    rule_name = f"c360-pipeline-schedule-{env}"
    sm_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:c360-pipeline-orchestrator-{env}"
    role_arn = f"arn:aws:iam::{account_id}:role/c360-eventbridge-role"

    # Create or update the rule
    events.put_rule(
        Name=rule_name,
        ScheduleExpression=schedule_expression,
        State="ENABLED",
        Description=f"Triggers C360 pipeline - {env} environment",
        Tags=[
            {"Key": "Project", "Value": "customer-360"},
            {"Key": "Environment", "Value": env},
        ]
    )
    logger.info("Created/updated rule: %s with schedule: %s", rule_name, schedule_expression)

    # Add the state machine as target
    from datetime import datetime, timezone
    process_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    events.put_targets(
        Rule=rule_name,
        Targets=[{
            "Id": f"c360-pipeline-target-{env}",
            "Arn": sm_arn,
            "RoleArn": role_arn,
            "Input": json.dumps({
                "processDate": process_date,
                "environment": env,
                "triggeredBy": "eventbridge-schedule"
            })
        }]
    )
    logger.info("Added target: %s", sm_arn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create EventBridge schedule rule")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--schedule", default="rate(1 hour)",
                        help="EventBridge schedule expression (e.g., 'rate(1 hour)', 'cron(0 */6 * * ? *)')")
    args = parser.parse_args()

    create_schedule_rule(args.env, args.region, args.account_id, args.schedule)
