"""
AWS Glue ETL Job: Curated → Redshift
======================================
Reads curated Parquet from S3, loads into Redshift staging tables,
then performs MERGE/upsert into the dimensional model.

Glue Job Parameters:
    --CURATED_BUCKET          Source S3 bucket (Curated layer)
    --REDSHIFT_CONNECTION     Glue connection name for Redshift
    --REDSHIFT_DATABASE       Redshift database name
    --REDSHIFT_SCHEMA         Redshift schema name
    --REDSHIFT_IAM_ROLE       IAM role ARN for Redshift COPY
    --PROCESS_DATE            Date to process (YYYY-MM-DD)
"""

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ── Initialize ───────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "CURATED_BUCKET", "REDSHIFT_CONNECTION",
    "REDSHIFT_DATABASE", "REDSHIFT_SCHEMA", "REDSHIFT_IAM_ROLE",
    "PROCESS_DATE"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

CURATED_BUCKET = args["CURATED_BUCKET"]
REDSHIFT_CONN = args["REDSHIFT_CONNECTION"]
REDSHIFT_DB = args["REDSHIFT_DATABASE"]
REDSHIFT_SCHEMA = args["REDSHIFT_SCHEMA"]
REDSHIFT_IAM_ROLE = args["REDSHIFT_IAM_ROLE"]
PROCESS_DATE = args["PROCESS_DATE"]

# JDBC URL from Glue connection
REDSHIFT_URL = f"jdbc:redshift://c360-warehouse.xxxx.{args.get('region', 'us-east-1')}.redshift.amazonaws.com:5439/{REDSHIFT_DB}"

# Temp S3 location for Redshift COPY
TEMP_S3 = f"s3://{CURATED_BUCKET}/_redshift_temp/"


# ── Helper Functions ─────────────────────────────────────────

def load_to_redshift(
    df,
    table_name: str,
    mode: str = "append",
    pre_actions: str = "",
    post_actions: str = ""
) -> int:
    """
    Load a Spark DataFrame into a Redshift table using the Redshift COPY command.

    Parameters
    ----------
    df : DataFrame
        Source DataFrame to load.
    table_name : str
        Target Redshift table (schema.table).
    mode : str
        Write mode: 'append', 'overwrite', or 'error'.
    pre_actions : str
        SQL to execute before the COPY (e.g., TRUNCATE staging table).
    post_actions : str
        SQL to execute after the COPY (e.g., MERGE into target).
    """
    full_table = f"{REDSHIFT_SCHEMA}.{table_name}"
    count = df.count()
    logger.info(f"Loading {count} records → {full_table}")

    options = {
        "url": REDSHIFT_URL,
        "dbtable": full_table,
        "tempdir": TEMP_S3,
        "aws_iam_role": REDSHIFT_IAM_ROLE,
    }

    if pre_actions:
        options["preactions"] = pre_actions
    if post_actions:
        options["postactions"] = post_actions

    df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .options(**options) \
        .mode(mode) \
        .save()

    logger.info(f"Successfully loaded {count} records into {full_table}")
    return count


def upsert_dimension(entity_name: str, primary_key: str) -> int:
    """
    Load a curated dimension into Redshift using staging + MERGE pattern.

    Steps:
    1. COPY data into staging table
    2. DELETE matching rows from target
    3. INSERT from staging into target
    4. DROP staging table
    """
    staging_table = f"stg_{entity_name}"
    target_table = entity_name

    logger.info(f"Upserting {entity_name} via staging table")

    df = spark.read.parquet(f"s3://{CURATED_BUCKET}/{entity_name}/")

    # Drop partition/metadata columns not needed in Redshift
    drop_cols = ["year", "month", "day", "_curated_at", "_processed_at", "_process_date"]
    for col in drop_cols:
        if col in df.columns:
            df = df.drop(col)

    pre_sql = f"""
        DROP TABLE IF EXISTS {REDSHIFT_SCHEMA}.{staging_table};
        CREATE TABLE {REDSHIFT_SCHEMA}.{staging_table} (LIKE {REDSHIFT_SCHEMA}.{target_table});
    """

    post_sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM {REDSHIFT_SCHEMA}.{target_table}
        USING {REDSHIFT_SCHEMA}.{staging_table}
        WHERE {REDSHIFT_SCHEMA}.{target_table}.{primary_key}
            = {REDSHIFT_SCHEMA}.{staging_table}.{primary_key};

        INSERT INTO {REDSHIFT_SCHEMA}.{target_table}
        SELECT * FROM {REDSHIFT_SCHEMA}.{staging_table};

        DROP TABLE {REDSHIFT_SCHEMA}.{staging_table};
        END TRANSACTION;
    """

    return load_to_redshift(df, staging_table, "overwrite", pre_sql, post_sql)


def load_fact_table(entity_name: str, incremental: bool = True) -> int:
    """
    Load a fact table into Redshift.
    Incremental mode appends only the current process date partition.
    Full mode overwrites the entire table.
    """
    logger.info(f"Loading fact table: {entity_name} (incremental={incremental})")

    process_dt = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
    year_val = process_dt.strftime("%Y")
    month_val = process_dt.strftime("%m")

    if incremental:
        df = spark.read.parquet(
            f"s3://{CURATED_BUCKET}/{entity_name}/year={year_val}/month={month_val}/"
        )
    else:
        df = spark.read.parquet(f"s3://{CURATED_BUCKET}/{entity_name}/")

    # Drop partition columns
    drop_cols = ["year", "month", "_curated_at"]
    for col in drop_cols:
        if col in df.columns:
            df = df.drop(col)

    if incremental:
        # Delete existing data for this month, then append
        pre_sql = f"""
            DELETE FROM {REDSHIFT_SCHEMA}.{entity_name}
            WHERE date_key >= '{year_val}-{month_val}-01'
              AND date_key <  '{year_val}-{month_val}-01'::date + interval '1 month';
        """
        return load_to_redshift(df, entity_name, "append", pre_sql)
    else:
        return load_to_redshift(df, entity_name, "overwrite")


# ── Main ─────────────────────────────────────────────────────

def main():
    logger.info(f"Starting Curated → Redshift load for date: {PROCESS_DATE}")

    results = {}

    # Dimensions (upsert)
    results["dim_customer"] = upsert_dimension("dim_customer", "customer_id")
    results["dim_product"] = upsert_dimension("dim_product", "product_id")
    results["dim_date"] = upsert_dimension("dim_date", "date_key")

    # Facts (incremental)
    results["fact_sales"] = load_fact_table("fact_sales", incremental=True)
    results["fact_clickstream"] = load_fact_table("fact_clickstream", incremental=True)

    logger.info(f"Curated → Redshift load complete. Results: {results}")

    # Run ANALYZE on loaded tables
    logger.info("Running ANALYZE on loaded tables...")
    for table in results.keys():
        try:
            spark.read \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", REDSHIFT_URL) \
                .option("query", f"ANALYZE {REDSHIFT_SCHEMA}.{table}") \
                .option("tempdir", TEMP_S3) \
                .option("aws_iam_role", REDSHIFT_IAM_ROLE) \
                .load()
        except Exception as e:
            logger.warn(f"ANALYZE failed for {table}: {e}")


main()
job.commit()
