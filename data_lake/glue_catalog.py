"""
AWS Glue Data Catalog Setup
=============================
Creates and manages Glue Data Catalog databases and tables for each
layer of the data lake (Raw, Clean, Curated).

Usage:
    python glue_catalog.py --env dev --region us-east-1
"""

import argparse
import logging
from typing import Dict, List, Any

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Table Schemas ────────────────────────────────────────────

CLICKSTREAM_COLUMNS = [
    {"Name": "event_id", "Type": "string", "Comment": "Unique event identifier"},
    {"Name": "event_type", "Type": "string", "Comment": "Event type: page_view, purchase, etc."},
    {"Name": "event_timestamp", "Type": "timestamp", "Comment": "UTC timestamp of event"},
    {"Name": "customer_id", "Type": "string", "Comment": "Customer identifier"},
    {"Name": "session_id", "Type": "string", "Comment": "Browser session identifier"},
    {"Name": "channel", "Type": "string", "Comment": "Channel: web, mobile_app, tablet"},
    {"Name": "device", "Type": "string", "Comment": "Device type"},
    {"Name": "browser", "Type": "string", "Comment": "Browser name"},
    {"Name": "region", "Type": "string", "Comment": "Geographic region"},
    {"Name": "page_url", "Type": "string", "Comment": "Page URL for page_view events"},
    {"Name": "product_id", "Type": "string", "Comment": "Product ID for cart/purchase events"},
    {"Name": "search_term", "Type": "string", "Comment": "Search query for search events"},
    {"Name": "order_total", "Type": "double", "Comment": "Order total for purchase events"},
]

CUSTOMER_COLUMNS = [
    {"Name": "customer_id", "Type": "string", "Comment": "Primary key"},
    {"Name": "first_name", "Type": "string", "Comment": "First name"},
    {"Name": "last_name", "Type": "string", "Comment": "Last name"},
    {"Name": "email", "Type": "string", "Comment": "Email address"},
    {"Name": "phone", "Type": "string", "Comment": "Phone number"},
    {"Name": "date_of_birth", "Type": "date", "Comment": "Date of birth"},
    {"Name": "gender", "Type": "string", "Comment": "Gender"},
    {"Name": "address_street", "Type": "string", "Comment": "Street address"},
    {"Name": "address_city", "Type": "string", "Comment": "City"},
    {"Name": "address_state", "Type": "string", "Comment": "State/Province"},
    {"Name": "address_zip", "Type": "string", "Comment": "Postal code"},
    {"Name": "address_country", "Type": "string", "Comment": "Country"},
    {"Name": "registration_date", "Type": "date", "Comment": "Account creation date"},
    {"Name": "customer_segment", "Type": "string", "Comment": "Segment: premium, standard, basic"},
    {"Name": "lifetime_value", "Type": "double", "Comment": "Calculated LTV"},
]

PRODUCT_COLUMNS = [
    {"Name": "product_id", "Type": "string", "Comment": "Primary key"},
    {"Name": "product_name", "Type": "string", "Comment": "Product display name"},
    {"Name": "category", "Type": "string", "Comment": "Top-level category"},
    {"Name": "subcategory", "Type": "string", "Comment": "Sub-category"},
    {"Name": "brand", "Type": "string", "Comment": "Brand name"},
    {"Name": "price", "Type": "double", "Comment": "Current retail price"},
    {"Name": "cost", "Type": "double", "Comment": "Wholesale cost"},
    {"Name": "weight_kg", "Type": "double", "Comment": "Weight in kg"},
    {"Name": "is_active", "Type": "boolean", "Comment": "Whether product is currently sold"},
    {"Name": "created_date", "Type": "date", "Comment": "Date added to catalog"},
]

TRANSACTION_COLUMNS = [
    {"Name": "transaction_id", "Type": "string", "Comment": "Primary key"},
    {"Name": "customer_id", "Type": "string", "Comment": "Foreign key to customer"},
    {"Name": "product_id", "Type": "string", "Comment": "Foreign key to product"},
    {"Name": "transaction_date", "Type": "timestamp", "Comment": "Transaction timestamp"},
    {"Name": "quantity", "Type": "int", "Comment": "Units purchased"},
    {"Name": "unit_price", "Type": "double", "Comment": "Price per unit at purchase"},
    {"Name": "discount_amount", "Type": "double", "Comment": "Discount applied"},
    {"Name": "total_amount", "Type": "double", "Comment": "Final amount charged"},
    {"Name": "payment_method", "Type": "string", "Comment": "Payment method used"},
    {"Name": "channel", "Type": "string", "Comment": "Purchase channel"},
    {"Name": "store_id", "Type": "string", "Comment": "Physical store ID (if applicable)"},
]


def create_database(glue_client, db_name: str, description: str) -> None:
    """Create a Glue database if it doesn't exist."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": db_name,
                "Description": description
            }
        )
        logger.info("Created database: %s", db_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            logger.info("Database already exists: %s", db_name)
        else:
            raise


def create_table(
    glue_client,
    db_name: str,
    table_name: str,
    columns: List[Dict[str, str]],
    s3_location: str,
    input_format: str = "parquet",
    partition_keys: List[Dict[str, str]] = None
) -> None:
    """Create or update a Glue Catalog table."""

    serde_info = {
        "parquet": {
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"}
            }
        },
        "json": {
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                "Parameters": {"serialization.format": "1"}
            }
        },
        "csv": {
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                "Parameters": {"separatorChar": ",", "quoteChar": "\""}
            }
        }
    }

    fmt = serde_info[input_format]
    table_input: Dict[str, Any] = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_location,
            "InputFormat": fmt["InputFormat"],
            "OutputFormat": fmt["OutputFormat"],
            "SerdeInfo": fmt["SerdeInfo"],
            "Compressed": input_format == "parquet",
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": input_format,
            "has_encrypted_data": "true",
        }
    }
    if partition_keys:
        table_input["PartitionKeys"] = partition_keys

    try:
        glue_client.create_table(DatabaseName=db_name, TableInput=table_input)
        logger.info("Created table: %s.%s", db_name, table_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            glue_client.update_table(DatabaseName=db_name, TableInput=table_input)
            logger.info("Updated table: %s.%s", db_name, table_name)
        else:
            raise


DATE_PARTITION_KEYS = [
    {"Name": "year", "Type": "string"},
    {"Name": "month", "Type": "string"},
    {"Name": "day", "Type": "string"},
]

MONTH_PARTITION_KEYS = [
    {"Name": "year", "Type": "string"},
    {"Name": "month", "Type": "string"},
]


def setup_catalog(env: str, region: str) -> None:
    """Set up the complete Glue Data Catalog for all layers."""
    glue = boto3.client("glue", region_name=region)

    # ── Databases ────────────────────────────────────────
    databases = {
        f"c360_raw_{env}": "Customer 360 — Raw layer (Bronze)",
        f"c360_clean_{env}": "Customer 360 — Clean layer (Silver)",
        f"c360_curated_{env}": "Customer 360 — Curated layer (Gold)",
    }
    for db_name, desc in databases.items():
        create_database(glue, db_name, desc)

    # ── Raw Layer Tables ─────────────────────────────────
    raw_db = f"c360_raw_{env}"
    raw_bucket = f"c360-raw-{env}"
    hour_partitions = DATE_PARTITION_KEYS + [{"Name": "hour", "Type": "string"}]

    create_table(glue, raw_db, "clickstream_raw",
                 CLICKSTREAM_COLUMNS, f"s3://{raw_bucket}/events/",
                 "json", hour_partitions)
    create_table(glue, raw_db, "customers_raw",
                 CUSTOMER_COLUMNS, f"s3://{raw_bucket}/batch/customers/",
                 "csv", DATE_PARTITION_KEYS)
    create_table(glue, raw_db, "products_raw",
                 PRODUCT_COLUMNS, f"s3://{raw_bucket}/batch/products/",
                 "csv", DATE_PARTITION_KEYS)
    create_table(glue, raw_db, "transactions_raw",
                 TRANSACTION_COLUMNS, f"s3://{raw_bucket}/batch/transactions/",
                 "csv", DATE_PARTITION_KEYS)

    # ── Clean Layer Tables ───────────────────────────────
    clean_db = f"c360_clean_{env}"
    clean_bucket = f"c360-clean-{env}"

    create_table(glue, clean_db, "clickstream_clean",
                 CLICKSTREAM_COLUMNS, f"s3://{clean_bucket}/clickstream/",
                 "parquet", DATE_PARTITION_KEYS)
    create_table(glue, clean_db, "customers_clean",
                 CUSTOMER_COLUMNS, f"s3://{clean_bucket}/customers/",
                 "parquet", DATE_PARTITION_KEYS)
    create_table(glue, clean_db, "products_clean",
                 PRODUCT_COLUMNS, f"s3://{clean_bucket}/products/",
                 "parquet", DATE_PARTITION_KEYS)
    create_table(glue, clean_db, "transactions_clean",
                 TRANSACTION_COLUMNS, f"s3://{clean_bucket}/transactions/",
                 "parquet", DATE_PARTITION_KEYS)

    # ── Curated Layer Tables ──────────────────────────────
    curated_db = f"c360_curated_{env}"
    curated_bucket = f"c360-curated-{env}"

    create_table(glue, curated_db, "fact_sales",
                 TRANSACTION_COLUMNS, f"s3://{curated_bucket}/fact_sales/",
                 "parquet", MONTH_PARTITION_KEYS)
    create_table(glue, curated_db, "fact_clickstream",
                 CLICKSTREAM_COLUMNS, f"s3://{curated_bucket}/fact_clickstream/",
                 "parquet", MONTH_PARTITION_KEYS)
    create_table(glue, curated_db, "dim_customer",
                 CUSTOMER_COLUMNS, f"s3://{curated_bucket}/dim_customer/",
                 "parquet")
    create_table(glue, curated_db, "dim_product",
                 PRODUCT_COLUMNS, f"s3://{curated_bucket}/dim_product/",
                 "parquet")

    logger.info("Glue Data Catalog setup complete for environment: %s", env)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup Glue Data Catalog")
    parser.add_argument("--env", default="dev", help="Environment (dev/staging/prod)")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()
    setup_catalog(args.env, args.region)
