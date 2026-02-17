"""
AWS Glue ETL Job: Clean → Curated Layer
=========================================
Reads cleaned Parquet data from the Clean layer, applies business rules,
joins customer + transaction + clickstream data, performs aggregations,
and writes star-schema-aligned curated Parquet to the Curated layer.

Glue Job Parameters:
    --CLEAN_BUCKET      Source S3 bucket (Clean layer)
    --CURATED_BUCKET    Target S3 bucket (Curated layer)
    --DATABASE          Glue catalog database name
    --PROCESS_DATE      Date to process (YYYY-MM-DD)
"""

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Initialize ───────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "CLEAN_BUCKET", "CURATED_BUCKET", "DATABASE", "PROCESS_DATE"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

CLEAN_BUCKET = args["CLEAN_BUCKET"]
CURATED_BUCKET = args["CURATED_BUCKET"]
PROCESS_DATE = args["PROCESS_DATE"]

process_dt = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
year = process_dt.strftime("%Y")
month = process_dt.strftime("%m")


# ── Dimension: Customer ──────────────────────────────────────

def build_dim_customer():
    """
    Build customer dimension with SCD Type 1 (overwrite).
    Enriches with calculated fields.
    """
    logger.info("Building dim_customer")

    customers = spark.read.parquet(f"s3://{CLEAN_BUCKET}/customers/")
    transactions = spark.read.parquet(f"s3://{CLEAN_BUCKET}/transactions/")

    # Calculate customer-level metrics
    customer_metrics = (
        transactions
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("total_amount").alias("total_spend"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("transaction_date").alias("first_purchase_date"),
            F.max("transaction_date").alias("last_purchase_date"),
            F.countDistinct("product_id").alias("unique_products_purchased"),
        )
    )

    dim_customer = (
        customers
        .join(customer_metrics, "customer_id", "left")
        # Fill nulls for customers with no transactions
        .withColumn("total_transactions", F.coalesce(F.col("total_transactions"), F.lit(0)))
        .withColumn("total_spend", F.coalesce(F.col("total_spend"), F.lit(0.0)))
        .withColumn("avg_order_value", F.coalesce(F.col("avg_order_value"), F.lit(0.0)))
        # Calculate days since last purchase
        .withColumn("days_since_last_purchase",
                     F.datediff(F.current_date(), F.col("last_purchase_date")))
        # Calculate customer tenure
        .withColumn("tenure_days",
                     F.datediff(F.current_date(), F.col("registration_date")))
        # RFM-based customer tier
        .withColumn("customer_tier",
                     F.when(F.col("total_spend") >= 5000, "platinum")
                     .when(F.col("total_spend") >= 2000, "gold")
                     .when(F.col("total_spend") >= 500, "silver")
                     .otherwise("bronze"))
        # Churn risk flag
        .withColumn("churn_risk",
                     F.when(F.col("days_since_last_purchase") > 90, "high")
                     .when(F.col("days_since_last_purchase") > 30, "medium")
                     .otherwise("low"))
        # Surrogate key
        .withColumn("customer_key", F.monotonically_increasing_id())
        .withColumn("_curated_at", F.current_timestamp())
    )

    count = dim_customer.count()
    logger.info(f"dim_customer: {count} records")

    dim_customer.write.mode("overwrite").parquet(
        f"s3://{CURATED_BUCKET}/dim_customer/"
    )
    return count


# ── Dimension: Product ───────────────────────────────────────

def build_dim_product():
    """Build product dimension with margin calculation."""
    logger.info("Building dim_product")

    products = spark.read.parquet(f"s3://{CLEAN_BUCKET}/products/")
    transactions = spark.read.parquet(f"s3://{CLEAN_BUCKET}/transactions/")

    product_metrics = (
        transactions
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_buyers"),
            F.avg("unit_price").alias("avg_selling_price"),
        )
    )

    dim_product = (
        products
        .join(product_metrics, "product_id", "left")
        .withColumn("total_units_sold", F.coalesce(F.col("total_units_sold"), F.lit(0)))
        .withColumn("total_revenue", F.coalesce(F.col("total_revenue"), F.lit(0.0)))
        .withColumn("profit_margin",
                     F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2))
        .withColumn("product_key", F.monotonically_increasing_id())
        .withColumn("_curated_at", F.current_timestamp())
    )

    count = dim_product.count()
    logger.info(f"dim_product: {count} records")

    dim_product.write.mode("overwrite").parquet(
        f"s3://{CURATED_BUCKET}/dim_product/"
    )
    return count


# ── Dimension: Date ──────────────────────────────────────────

def build_dim_date():
    """Build a calendar dimension table."""
    logger.info("Building dim_date")

    dim_date = (
        spark.sql("""
            SELECT
                explode(
                    sequence(
                        to_date('2023-01-01'),
                        to_date('2026-12-31'),
                        interval 1 day
                    )
                ) AS date_key
        """)
        .withColumn("year", F.year("date_key"))
        .withColumn("quarter", F.quarter("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("month_name", F.date_format("date_key", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("date_key"))
        .withColumn("day_of_month", F.dayofmonth("date_key"))
        .withColumn("day_of_week", F.dayofweek("date_key"))
        .withColumn("day_name", F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend",
                     F.when(F.dayofweek("date_key").isin(1, 7), True)
                     .otherwise(False))
        .withColumn("is_month_end", F.col("date_key") == F.last_day("date_key"))
        .withColumn("fiscal_year", F.year(F.add_months("date_key", 3)))
        .withColumn("fiscal_quarter",
                     F.quarter(F.add_months("date_key", 3)))
    )

    count = dim_date.count()
    logger.info(f"dim_date: {count} records")

    dim_date.write.mode("overwrite").parquet(
        f"s3://{CURATED_BUCKET}/dim_date/"
    )
    return count


# ── Fact: Sales ──────────────────────────────────────────────

def build_fact_sales():
    """
    Build the sales fact table by joining transactions with dimension keys.
    Partitioned by year/month for efficient querying.
    """
    logger.info("Building fact_sales")

    transactions = spark.read.parquet(f"s3://{CLEAN_BUCKET}/transactions/")
    dim_customer = spark.read.parquet(f"s3://{CURATED_BUCKET}/dim_customer/")
    dim_product = spark.read.parquet(f"s3://{CURATED_BUCKET}/dim_product/")

    # Get surrogate keys
    customer_keys = dim_customer.select("customer_id", "customer_key")
    product_keys = dim_product.select("product_id", "product_key")

    fact_sales = (
        transactions
        .join(customer_keys, "customer_id", "left")
        .join(product_keys, "product_id", "left")
        .withColumn("date_key", F.to_date("transaction_date"))
        .withColumn("gross_amount",
                     F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("net_amount",
                     F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
        .withColumn("discount_percentage",
                     F.round(F.col("discount_amount") / F.col("gross_amount") * 100, 2))
        .select(
            "transaction_id",
            "customer_key",
            "product_key",
            "date_key",
            "quantity",
            "unit_price",
            "gross_amount",
            "discount_amount",
            "discount_percentage",
            "net_amount",
            "payment_method",
            "channel",
            "store_id",
        )
        .withColumn("year", F.year("date_key").cast("string"))
        .withColumn("month", F.format_string("%02d", F.month(F.col("date_key"))))
        .withColumn("_curated_at", F.current_timestamp())
    )

    count = fact_sales.count()
    logger.info(f"fact_sales: {count} records")

    fact_sales.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{CURATED_BUCKET}/fact_sales/"
    )
    return count


# ── Fact: Clickstream ────────────────────────────────────────

def build_fact_clickstream():
    """Build clickstream fact table with session-level aggregations."""
    logger.info("Building fact_clickstream")

    clickstream = spark.read.parquet(f"s3://{CLEAN_BUCKET}/clickstream/")
    dim_customer = spark.read.parquet(f"s3://{CURATED_BUCKET}/dim_customer/")

    customer_keys = dim_customer.select("customer_id", "customer_key")

    # Session-level aggregations
    session_window = Window.partitionBy("session_id").orderBy("event_timestamp")

    fact_clickstream = (
        clickstream
        .join(customer_keys, "customer_id", "left")
        .withColumn("event_sequence", F.row_number().over(session_window))
        .withColumn("date_key", F.to_date("event_timestamp"))
        .select(
            "event_id",
            "event_type",
            "event_timestamp",
            "customer_key",
            "session_id",
            "date_key",
            "channel",
            "device",
            "browser",
            "region",
            "page_url",
            "product_id",
            "search_term",
            "order_total",
            "event_sequence",
        )
        .withColumn("year", F.year("date_key").cast("string"))
        .withColumn("month", F.format_string("%02d", F.month(F.col("date_key"))))
        .withColumn("_curated_at", F.current_timestamp())
    )

    count = fact_clickstream.count()
    logger.info(f"fact_clickstream: {count} records")

    fact_clickstream.write.mode("overwrite").partitionBy("year", "month").parquet(
        f"s3://{CURATED_BUCKET}/fact_clickstream/"
    )
    return count


# ── Main ─────────────────────────────────────────────────────

def main():
    logger.info(f"Starting Clean → Curated ETL for date: {PROCESS_DATE}")

    results = {
        "dim_customer": build_dim_customer(),
        "dim_product": build_dim_product(),
        "dim_date": build_dim_date(),
        "fact_sales": build_fact_sales(),
        "fact_clickstream": build_fact_clickstream(),
    }

    logger.info(f"Clean → Curated ETL complete. Results: {results}")


main()
job.commit()
