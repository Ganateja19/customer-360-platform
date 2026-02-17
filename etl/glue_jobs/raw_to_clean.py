"""
AWS Glue ETL Job: Raw → Clean Layer
=====================================
PySpark-based distributed ETL that reads raw JSON/CSV data from the
Raw S3 layer, applies schema enforcement, deduplication, null handling,
type casting, and timestamp normalization, then writes clean Parquet
to the Clean S3 layer.

Glue Job Parameters:
    --RAW_BUCKET        Source S3 bucket (Raw layer)
    --CLEAN_BUCKET      Target S3 bucket (Clean layer)
    --DATABASE          Glue catalog database name
    --PROCESS_DATE      Date to process (YYYY-MM-DD)
"""

import sys
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType, BooleanType
)

# ── Initialize Glue Context ─────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "RAW_BUCKET", "CLEAN_BUCKET", "DATABASE", "PROCESS_DATE"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

RAW_BUCKET = args["RAW_BUCKET"]
CLEAN_BUCKET = args["CLEAN_BUCKET"]
DATABASE = args["DATABASE"]
PROCESS_DATE = args["PROCESS_DATE"]

process_dt = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
year, month, day = process_dt.strftime("%Y"), process_dt.strftime("%m"), process_dt.strftime("%d")


# ── Schema Definitions ──────────────────────────────────────

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", StringType(), True),
    StructField("customer_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("region", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("search_term", StringType(), True),
    StructField("results_count", IntegerType(), True),
    StructField("order_total", DoubleType(), True),
    StructField("payment_method", StringType(), True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address_street", StringType(), True),
    StructField("address_city", StringType(), True),
    StructField("address_state", StringType(), True),
    StructField("address_zip", StringType(), True),
    StructField("address_country", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("lifetime_value", DoubleType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("cost", DoubleType(), True),
    StructField("weight_kg", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_date", StringType(), True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("transaction_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("store_id", StringType(), True),
])


# ── Transform Functions ─────────────────────────────────────

def clean_clickstream(raw_path: str, clean_path: str) -> int:
    """Clean clickstream events: schema enforcement, dedup, null handling."""
    logger.info(f"Processing clickstream from {raw_path}")

    df = spark.read.json(raw_path, schema=CLICKSTREAM_SCHEMA)
    initial_count = df.count()
    logger.info(f"Raw clickstream records: {initial_count}")

    df_clean = (
        df
        # Remove records missing required fields
        .filter(F.col("event_id").isNotNull() & F.col("customer_id").isNotNull())
        # Deduplicate on event_id (keep first occurrence)
        .dropDuplicates(["event_id"])
        # Normalize timestamp
        .withColumn("event_timestamp",
                     F.to_timestamp(F.col("event_timestamp")))
        # Filter future timestamps
        .filter(F.col("event_timestamp") <= F.current_timestamp())
        # Standardize string columns to lowercase
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("device", F.lower(F.trim(F.col("device"))))
        .withColumn("browser", F.lower(F.trim(F.col("browser"))))
        # Validate event_type
        .filter(F.col("event_type").isin(
            "page_view", "add_to_cart", "remove_from_cart",
            "purchase", "search", "wishlist_add"
        ))
        # Add processing metadata
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_process_date", F.lit(PROCESS_DATE))
        # Add partition columns
        .withColumn("year", F.year(F.col("event_timestamp")).cast("string"))
        .withColumn("month", F.format_string("%02d", F.month(F.col("event_timestamp"))))
        .withColumn("day", F.format_string("%02d", F.dayofmonth(F.col("event_timestamp"))))
    )

    final_count = df_clean.count()
    logger.info(f"Clean clickstream records: {final_count} (dropped {initial_count - final_count})")

    # Write partitioned Parquet
    df_clean.write.mode("overwrite").partitionBy("year", "month", "day").parquet(clean_path)
    return final_count


def clean_customers(raw_path: str, clean_path: str) -> int:
    """Clean customer master data."""
    logger.info(f"Processing customers from {raw_path}")

    df = spark.read.option("header", True).csv(raw_path, schema=CUSTOMER_SCHEMA)
    initial_count = df.count()

    df_clean = (
        df
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        # Normalize names
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        # Lowercase email
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        # Parse dates
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
        .withColumn("registration_date", F.to_date(F.col("registration_date"), "yyyy-MM-dd"))
        # Validate segment
        .withColumn("customer_segment",
                     F.when(F.col("customer_segment").isin("premium", "standard", "basic"),
                            F.col("customer_segment"))
                     .otherwise("standard"))
        # Default lifetime_value
        .withColumn("lifetime_value",
                     F.coalesce(F.col("lifetime_value"), F.lit(0.0)))
        # Metadata
        .withColumn("_processed_at", F.current_timestamp())
        # Partition columns
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
    )

    final_count = df_clean.count()
    logger.info(f"Clean customer records: {final_count}")

    df_clean.write.mode("overwrite").partitionBy("year", "month", "day").parquet(clean_path)
    return final_count


def clean_products(raw_path: str, clean_path: str) -> int:
    """Clean product catalog data."""
    logger.info(f"Processing products from {raw_path}")

    df = spark.read.option("header", True).csv(raw_path, schema=PRODUCT_SCHEMA)
    initial_count = df.count()

    df_clean = (
        df
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("subcategory", F.lower(F.trim(F.col("subcategory"))))
        .withColumn("brand", F.trim(F.col("brand")))
        # Validate price > 0
        .filter(F.col("price") > 0)
        .withColumn("cost", F.coalesce(F.col("cost"), F.col("price") * 0.6))
        .withColumn("created_date", F.to_date(F.col("created_date"), "yyyy-MM-dd"))
        .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(True)))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
    )

    final_count = df_clean.count()
    logger.info(f"Clean product records: {final_count}")
    df_clean.write.mode("overwrite").partitionBy("year", "month", "day").parquet(clean_path)
    return final_count


def clean_transactions(raw_path: str, clean_path: str) -> int:
    """Clean transaction data."""
    logger.info(f"Processing transactions from {raw_path}")

    df = spark.read.option("header", True).csv(raw_path, schema=TRANSACTION_SCHEMA)
    initial_count = df.count()

    df_clean = (
        df
        .filter(
            F.col("transaction_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("product_id").isNotNull()
        )
        .dropDuplicates(["transaction_id"])
        .withColumn("transaction_date", F.to_timestamp(F.col("transaction_date")))
        .filter(F.col("transaction_date").isNotNull())
        # Validate amounts
        .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
        .withColumn("discount_amount", F.coalesce(F.col("discount_amount"), F.lit(0.0)))
        # Recalculate total
        .withColumn("total_amount",
                     F.round(F.col("quantity") * F.col("unit_price") - F.col("discount_amount"), 2))
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("year", F.year(F.col("transaction_date")).cast("string"))
        .withColumn("month", F.format_string("%02d", F.month(F.col("transaction_date"))))
        .withColumn("day", F.format_string("%02d", F.dayofmonth(F.col("transaction_date"))))
    )

    final_count = df_clean.count()
    logger.info(f"Clean transaction records: {final_count}")
    df_clean.write.mode("overwrite").partitionBy("year", "month", "day").parquet(clean_path)
    return final_count


# ── Main Execution ───────────────────────────────────────────

def main():
    logger.info(f"Starting Raw → Clean ETL for date: {PROCESS_DATE}")

    partition_path = f"year={year}/month={month}/day={day}"

    results = {}

    # Clickstream (all event types merged)
    results["clickstream"] = clean_clickstream(
        f"s3://{RAW_BUCKET}/events/*/{partition_path}/",
        f"s3://{CLEAN_BUCKET}/clickstream/"
    )

    # Customers
    results["customers"] = clean_customers(
        f"s3://{RAW_BUCKET}/batch/customers/{partition_path}/",
        f"s3://{CLEAN_BUCKET}/customers/"
    )

    # Products
    results["products"] = clean_products(
        f"s3://{RAW_BUCKET}/batch/products/{partition_path}/",
        f"s3://{CLEAN_BUCKET}/products/"
    )

    # Transactions
    results["transactions"] = clean_transactions(
        f"s3://{RAW_BUCKET}/batch/transactions/{partition_path}/",
        f"s3://{CLEAN_BUCKET}/transactions/"
    )

    logger.info(f"Raw → Clean ETL complete. Results: {results}")


main()
job.commit()
