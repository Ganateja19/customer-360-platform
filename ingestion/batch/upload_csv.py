"""
Batch CSV Uploader — S3 Raw Layer
==================================
Uploads batch CSV files (customer master, product catalog, historical
transactions) to the S3 Raw Layer with proper partitioning.

Usage:
    python upload_csv.py --file customers.csv --entity customers
    python upload_csv.py --file products.csv --entity products --date 2024-01-15
"""

import os
import argparse
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def upload_to_raw_layer(
    file_path: str,
    entity: str,
    bucket: str,
    prefix: str = "raw/",
    date_override: str = None,
    region: str = "us-east-1"
) -> str:
    """
    Upload a CSV file to the S3 Raw Layer with date partitioning.

    Parameters
    ----------
    file_path : str
        Local path to the CSV file.
    entity : str
        Entity name (customers, products, transactions).
    bucket : str
        S3 bucket name.
    prefix : str
        S3 key prefix.
    date_override : str
        Optional date string (YYYY-MM-DD) to use as partition date.
    region : str
        AWS region.

    Returns
    -------
    str
        The S3 URI of the uploaded file.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    s3 = boto3.client("s3", region_name=region)

    # Determine partition date
    if date_override:
        partition_date = datetime.strptime(date_override, "%Y-%m-%d")
    else:
        partition_date = datetime.now(timezone.utc)

    # Build partitioned key
    filename = os.path.basename(file_path)
    s3_key = (
        f"{prefix}batch/{entity}/"
        f"year={partition_date.strftime('%Y')}/"
        f"month={partition_date.strftime('%m')}/"
        f"day={partition_date.strftime('%d')}/"
        f"{filename}"
    )

    file_size = os.path.getsize(file_path)
    logger.info(
        "Uploading %s (%.2f MB) → s3://%s/%s",
        file_path, file_size / (1024 * 1024), bucket, s3_key
    )

    try:
        # Use multipart for files > 100 MB
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=100 * 1024 * 1024,
            multipart_chunksize=50 * 1024 * 1024,
            max_concurrency=10
        )
        s3.upload_file(
            Filename=file_path,
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={
                "ServerSideEncryption": "aws:kms",
                "Metadata": {
                    "entity": entity,
                    "upload_timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "batch_upload",
                    "original_filename": filename
                }
            },
            Config=transfer_config
        )
        s3_uri = f"s3://{bucket}/{s3_key}"
        logger.info("Upload complete → %s", s3_uri)
        return s3_uri

    except ClientError as e:
        logger.error("Upload failed: %s", e)
        raise


def upload_multiple_files(
    directory: str,
    entity: str,
    bucket: str,
    prefix: str = "raw/",
    region: str = "us-east-1"
) -> list:
    """Upload all CSV files in a directory to S3."""
    results = []
    for filename in sorted(os.listdir(directory)):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            uri = upload_to_raw_layer(file_path, entity, bucket, prefix, region=region)
            results.append(uri)
    logger.info("Uploaded %d files from %s", len(results), directory)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload batch CSV to S3 Raw Layer")
    parser.add_argument("--file", required=True, help="Path to CSV file")
    parser.add_argument("--entity", required=True, choices=["customers", "products", "transactions"],
                        help="Entity type")
    parser.add_argument("--bucket", default="c360-raw-dev", help="S3 bucket")
    parser.add_argument("--prefix", default="raw/", help="S3 key prefix")
    parser.add_argument("--date", default=None, help="Partition date (YYYY-MM-DD)")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()

    upload_to_raw_layer(
        args.file, args.entity, args.bucket,
        args.prefix, args.date, args.region
    )
