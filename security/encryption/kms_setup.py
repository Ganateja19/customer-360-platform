"""
KMS Key Setup
==============
Creates KMS customer-managed key for encrypting data at rest
across S3 buckets and Redshift.
"""

import argparse
import json
import logging

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_kms_key(env: str, region: str, account_id: str) -> str:
    """Create a KMS key for pipeline data encryption."""
    kms = boto3.client("kms", region_name=region)

    key_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "EnableRootAccountAccess",
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
                "Action": "kms:*",
                "Resource": "*"
            },
            {
                "Sid": "AllowS3ServiceEncryption",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": [
                    "kms:GenerateDataKey",
                    "kms:Decrypt"
                ],
                "Resource": "*"
            },
            {
                "Sid": "AllowGlueAccess",
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{account_id}:role/c360-glue-etl-role-{env}"},
                "Action": [
                    "kms:Decrypt",
                    "kms:GenerateDataKey"
                ],
                "Resource": "*"
            },
            {
                "Sid": "AllowRedshiftAccess",
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{account_id}:role/c360-redshift-role-{env}"},
                "Action": "kms:Decrypt",
                "Resource": "*"
            }
        ]
    }

    # Create key
    response = kms.create_key(
        Description=f"Customer 360 data encryption key - {env}",
        KeyUsage="ENCRYPT_DECRYPT",
        KeySpec="SYMMETRIC_DEFAULT",
        Policy=json.dumps(key_policy),
        Tags=[
            {"TagKey": "Project", "TagValue": "customer-360"},
            {"TagKey": "Environment", "TagValue": env},
        ]
    )
    key_id = response["KeyMetadata"]["KeyId"]
    key_arn = response["KeyMetadata"]["Arn"]
    logger.info("Created KMS key: %s", key_id)

    # Create alias
    alias_name = f"alias/c360-data-key-{env}"
    try:
        kms.create_alias(AliasName=alias_name, TargetKeyId=key_id)
        logger.info("Created alias: %s", alias_name)
    except kms.exceptions.AlreadyExistsException:
        logger.info("Alias already exists: %s", alias_name)

    # Enable automatic key rotation
    kms.enable_key_rotation(KeyId=key_id)
    logger.info("Enabled automatic key rotation")

    return key_arn


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create KMS encryption key")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--account-id", required=True)
    args = parser.parse_args()

    arn = create_kms_key(args.env, args.region, args.account_id)
    print(f"KMS Key ARN: {arn}")
