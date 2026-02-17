"""
SNS Notifications Setup
=========================
Creates SNS topic and subscriptions for pipeline alerting.
"""

import argparse
import logging
import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_sns_notifications(env: str, region: str, email: str) -> str:
    """Create SNS topic and email subscription for pipeline alerts."""
    sns = boto3.client("sns", region_name=region)

    topic_name = f"c360-pipeline-alerts-{env}"

    # Create topic
    response = sns.create_topic(
        Name=topic_name,
        Tags=[
            {"Key": "Project", "Value": "customer-360"},
            {"Key": "Environment", "Value": env},
        ],
        Attributes={
            "KmsMasterKeyId": f"alias/c360-data-key",
        }
    )
    topic_arn = response["TopicArn"]
    logger.info("Created SNS topic: %s", topic_arn)

    # Add email subscription
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=email,
    )
    logger.info("Subscription pending confirmation for: %s", email)

    # Set topic policy for Step Functions and CloudWatch
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowStepFunctionsPublish",
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sns:Publish",
                "Resource": topic_arn
            },
            {
                "Sid": "AllowCloudWatchPublish",
                "Effect": "Allow",
                "Principal": {"Service": "cloudwatch.amazonaws.com"},
                "Action": "sns:Publish",
                "Resource": topic_arn
            }
        ]
    }
    import json
    sns.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(policy)
    )

    return topic_arn


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup SNS notifications")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--email", required=True, help="Email for alerts")
    args = parser.parse_args()

    arn = create_sns_notifications(args.env, args.region, args.email)
    print(f"SNS Topic ARN: {arn}")
