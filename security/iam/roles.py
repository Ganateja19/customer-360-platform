"""
IAM Roles Setup
================
Creates IAM roles with the defined least-privilege policies
for each pipeline component.

Usage:
    python roles.py --env dev --account-id 123456789012
"""

import json
import argparse
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

POLICIES_FILE = Path(__file__).parent / "policies.json"


ROLES = [
    {
        "role_name": "c360-lambda-consumer-role",
        "service": "lambda.amazonaws.com",
        "policy_key": "LambdaKinesisConsumerPolicy",
        "managed_policies": [
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        ]
    },
    {
        "role_name": "c360-glue-etl-role",
        "service": "glue.amazonaws.com",
        "policy_key": "GlueETLJobPolicy",
        "managed_policies": [
            "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        ]
    },
    {
        "role_name": "c360-redshift-role",
        "service": "redshift.amazonaws.com",
        "policy_key": "RedshiftLoadPolicy",
        "managed_policies": []
    },
    {
        "role_name": "c360-step-functions-role",
        "service": "states.amazonaws.com",
        "policy_key": "StepFunctionsOrchestratorPolicy",
        "managed_policies": []
    },
]


def create_role(
    iam_client,
    role_name: str,
    service: str,
    inline_policy: dict,
    managed_policies: list,
    env: str
) -> str:
    """Create an IAM role with trust policy and attach policies."""

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": service},
            "Action": "sts:AssumeRole"
        }]
    }

    full_role_name = f"{role_name}-{env}"

    try:
        response = iam_client.create_role(
            RoleName=full_role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=f"C360 pipeline role - {env}",
            Tags=[
                {"Key": "Project", "Value": "customer-360"},
                {"Key": "Environment", "Value": env},
            ]
        )
        role_arn = response["Role"]["Arn"]
        logger.info("Created role: %s", full_role_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            role_arn = iam_client.get_role(RoleName=full_role_name)["Role"]["Arn"]
            logger.info("Role already exists: %s", full_role_name)
        else:
            raise

    # Attach inline policy
    iam_client.put_role_policy(
        RoleName=full_role_name,
        PolicyName=f"{role_name}-inline-policy",
        PolicyDocument=json.dumps(inline_policy)
    )

    # Attach managed policies
    for policy_arn in managed_policies:
        try:
            iam_client.attach_role_policy(RoleName=full_role_name, PolicyArn=policy_arn)
        except ClientError:
            logger.warning("Could not attach managed policy: %s", policy_arn)

    return role_arn


def setup_roles(env: str, region: str) -> dict:
    """Create all pipeline IAM roles."""
    iam = boto3.client("iam", region_name=region)

    with open(POLICIES_FILE) as f:
        all_policies = json.load(f)["Policies"]

    results = {}
    for role_config in ROLES:
        policy = all_policies[role_config["policy_key"]]["PolicyDocument"]
        arn = create_role(
            iam,
            role_config["role_name"],
            role_config["service"],
            policy,
            role_config["managed_policies"],
            env
        )
        results[role_config["role_name"]] = arn

    logger.info("All roles created: %s", list(results.keys()))
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create IAM roles")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()
    setup_roles(args.env, args.region)
