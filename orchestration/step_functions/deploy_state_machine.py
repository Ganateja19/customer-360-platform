"""
Step Functions State Machine Deployer
======================================
Deploys or updates the pipeline orchestration state machine.

Usage:
    python deploy_state_machine.py --env dev --region us-east-1
"""

import json
import argparse
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STATE_MACHINE_FILE = Path(__file__).parent / "pipeline_state_machine.json"


def deploy_state_machine(env: str, region: str, account_id: str) -> str:
    """
    Deploy or update the Step Functions state machine.

    Returns the state machine ARN.
    """
    sfn = boto3.client("stepfunctions", region_name=region)
    sm_name = f"c360-pipeline-orchestrator-{env}"
    role_arn = f"arn:aws:iam::{account_id}:role/c360-step-functions-role"

    # Load and parameterize the state machine definition
    with open(STATE_MACHINE_FILE) as f:
        definition = f.read()

    # Replace placeholder account ID
    definition = definition.replace("123456789012", account_id)
    definition = definition.replace("c360-raw-dev", f"c360-raw-{env}")
    definition = definition.replace("c360-clean-dev", f"c360-clean-{env}")
    definition = definition.replace("c360-curated-dev", f"c360-curated-{env}")
    definition = definition.replace("c360-quality-logs-dev", f"c360-quality-logs-{env}")

    # Check if state machine exists
    sm_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{sm_name}"

    try:
        sfn.describe_state_machine(stateMachineArn=sm_arn)
        # Update existing
        sfn.update_state_machine(
            stateMachineArn=sm_arn,
            definition=definition,
            roleArn=role_arn
        )
        logger.info("Updated state machine: %s", sm_arn)

    except ClientError as e:
        if e.response["Error"]["Code"] == "StateMachineDoesNotExist":
            # Create new
            response = sfn.create_state_machine(
                name=sm_name,
                definition=definition,
                roleArn=role_arn,
                type="STANDARD",
                loggingConfiguration={
                    "level": "ALL",
                    "includeExecutionData": True,
                    "destinations": [{
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": f"arn:aws:logs:{region}:{account_id}:log-group:/aws/stepfunctions/{sm_name}:*"
                        }
                    }]
                },
                tags=[
                    {"key": "Project", "value": "customer-360"},
                    {"key": "Environment", "value": env},
                ]
            )
            sm_arn = response["stateMachineArn"]
            logger.info("Created state machine: %s", sm_arn)
        else:
            raise

    return sm_arn


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy Step Functions state machine")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--account-id", required=True)
    args = parser.parse_args()

    arn = deploy_state_machine(args.env, args.region, args.account_id)
    print(f"State machine ARN: {arn}")
