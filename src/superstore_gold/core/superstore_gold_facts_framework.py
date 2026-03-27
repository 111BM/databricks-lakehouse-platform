# -----------------------------
# ETL Pipeline: Silver to Gold Table Transformation
# -----------------------------
# This pipeline processes data from a Silver Delta table to a Gold Delta table, including:
# 1. Reading and validating the Silver table for required columns.
# 2. Preparing data with necessary transformations (e.g., adding audit timestamps).
# 3. Ensuring the Gold table exists before performing an idempotent merge.
# 4. Merging data from Silver to Gold using Delta Lake's merge feature, ensuring no duplication.
# 5. Collecting metrics for pipeline observability (e.g., rows inserted, updated).
# 6. Optimizing Gold table performance using Z-Ordering and cleaning up old data with VACUUM.
#
# This solution is designed for production-scale data processing with logging, error handling,
# and performance optimization in mind.
# -----------------------------

import logging  # Standard Python logging module for structured logging
from uuid import uuid4  # Generates a unique identifier for tracking pipeline runs
from pyspark.sql import SparkSession  # Core SparkSession for interacting with Spark
from pyspark.sql.functions import col, current_timestamp, max as spark_max, lit, spark_partition_id
  # Helper functions
from delta.tables import DeltaTable  # Delta Lake API for interacting with Delta tables
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    IntegerType,
    DoubleType
)
import sys

sys.path.append(
    "/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/src/superstore_shared_utilities"
)
from superstore_logger import get_superstore_logger, log_event
from superstore_platform_constants import GOLD_LAYER


# -----------------------------
# log_event Setup for Bronze Ingestion
# -----------------------------
# Initialize log_event to capture events in the Bronze ingestion pipeline
logger_gold_facts = get_superstore_logger("superstore_gold_facts_framework")

def get_incremental_silver_for_facts(
    spark, 
    silver_table: str, 
    gold_table: str, 
    master_run_id: str, 
    layer_run_id: str, 
    layer: str, 
    ingestion_col: str = "ingestion_ts"
):
    """
    Returns only new Bronze rows not yet ingested into Silver.
    Optimized for serverless / partitioned Bronze tables.
    """

    # -----------------------------
    # Validate Silver table exists
    # -----------------------------
    if not spark.catalog.tableExists(silver_table):

        log_event(
            logger_gold_facts,
            "WARNING",
            f"Silver table '{silver_table}' does not exist. Skipping fact processing.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

        return spark.createDataFrame([], StructType([]))


    # -----------------------------
    # Start logging
    # -----------------------------
    log_event(
        logger_gold_facts,
        "INFO",
        f"Fetching incremental rows from silver table '{silver_table}' for Silver table '{silver_table}'",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
        ingestion_col=ingestion_col
    )

    # -----------------------------
    # Get max ingestion from Gold
    # -----------------------------
    max_ingestion_ts = None
    if spark.catalog.tableExists(gold_table):
        max_ingestion_ts_row = (
            spark.table(gold_table)
            .agg(spark_max(ingestion_col).alias("max_ingest_ts"))
            .first()
        )
        max_ingestion_ts = max_ingestion_ts_row["max_ingest_ts"]
        log_event(
            logger_gold_facts,
            "INFO",
            f"Max ingestion timestamp found in gold table '{gold_table}': {max_ingestion_ts}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

    # Push filter down to silver partitions
    silver_df = spark.table(silver_table)
    if max_ingestion_ts:
        incremental_df = silver_df.filter(col(ingestion_col) > max_ingestion_ts)
    else:
        incremental_df = silver_df
        log_event(
            logger_gold_facts,
            "INFO",
            f"Gold table '{gold_table}' does not exist. Returning full silver table.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
    row_count = incremental_df.count()
    log_event(
        logger_gold_facts,
        "INFO",
        f"Incremental silver rows to process: {row_count}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )   
    
    return incremental_df

# -----------------------------
# 1. Read Silver Table
# -----------------------------
def read_silver_table(
    spark, 
    silver_table: str,
    gold_table: str,
    required_columns, 
    master_run_id: str, 
    layer_run_id: str,
    layer: str
):
    """
    Reads data from the Silver fact table, validates the required columns are present,
    and logs success or failure for transparency in pipeline execution.

    Args:
        spark (SparkSession): Active Spark session.
        table_name (str): Name of the Silver table to read from.
        log_event (logger_gold_facts): log_event instance to log pipeline progress.
        required_columns (list): List of columns that must exist in the Silver table.

    Returns:
        DataFrame: DataFrame loaded from the Silver table.

    Raises:
        ValueError: If any required column is missing from the Silver table.
    """
    log_event(
        logger_gold_facts,
        "INFO",
        f"Reading Silver fact table: {silver_table}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )

    try:
        # Read the Delta table into a DataFrame incrementally
        df = get_incremental_silver_for_facts(spark, silver_table, gold_table, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=GOLD_LAYER, ingestion_col="ingestion_ts")

        # -----------------------------
        # Check if dataframe has rows FIRST
        # -----------------------------
        has_rows = bool(df.head(1))

        if not has_rows:
            log_event(
                logger_gold_facts,
                "INFO",
                f"No incremental rows found in {silver_table}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )

            return df   # Skip column validation completely
        
        # Check if dataframe is empty (table missing scenario)
        if len(df.columns) == 0:
            log_event(
                logger_gold_facts,
                "WARNING",
                f"Silver table '{silver_table}' does not exist or returned empty schema. Skipping processing.",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )
            return df


        # -----------------------------
        # Optional columns that may not exist yet in Silver
        # -----------------------------
        optional_columns_with_defaults = {
            "silver_has_id": False,  # only optional, add more if needed
        }

        # Validate strictly required columns (from config)
        missing_required = [c for c in required_columns if c not in df.columns]
        if missing_required:
            raise ValueError(
                f"Missing required columns in Silver table '{silver_table}': {missing_required}"
            )

        # Add optional columns if they are missing
        for col_name, default_value in optional_columns_with_defaults.items():
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(default_value))

        log_event(
            logger_gold_facts,
            "INFO",
            f"Successfully read Silver table: {silver_table}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return df
    except Exception as e:
        log_event(
            logger_gold_facts,
            "ERROR",
            f"Failed to read Silver table {silver_table}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )  # Log error if loading fails
        raise


# -----------------------------
# 2. Prepare Fact Table Columns
# -----------------------------
def prepare_fact_columns(df, master_run_id: str, layer_run_id: str, layer: str):
    """
    Prepares the fact table by adding a 'gold_load_ts' timestamp column for auditing and idempotency.

    Args:
        df (DataFrame): Silver fact table DataFrame to be transformed.
        log_event (logger_gold_facts): log_event instance to track the process.

    Returns:
        DataFrame: Transformed DataFrame with added 'gold_load_ts' column.
    """
    log_event(
        logger_gold_facts,
        "INFO",
        "Preparing fact table columns for Gold",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    df_prepared = df.withColumn(
        "gold_load_ts", current_timestamp()
    )  # Add timestamp for auditing
    log_event(
        logger_gold_facts,
        "INFO",
        "Fact table columns prepared successfully",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    return df_prepared


# -----------------------------
# 3. Create Gold Table if Not Exists
# -----------------------------
def create_gold_table_if_not_exists(
    df, gold_tbl, master_run_id: str, layer_run_id: str, layer: str
):
    """
    Checks if the Gold fact table exists. If not, creates an empty Delta Gold table to ensure
    the merge operation works smoothly later in the pipeline.

    Args:
        df (DataFrame): DataFrame that will be merged into Gold.
        gold_tbl (str): Name of the Gold table.
        log_event (logger_gold_facts): log_event instance to track the process.
    """
    log_event(logger_gold_facts, "INFO", f"Checking if Gold table {gold_tbl} exists", master_run_id=master_run_id, layer_run_id=layer_run_id, layer=GOLD_LAYER)
    if not df.sparkSession.catalog.tableExists(
        gold_tbl
    ):  # Check if the Gold table exists in the catalog
        df.limit(0).write.format("delta").mode("ignore").saveAsTable(
            gold_tbl
        )  # Create an empty table if not exists
        log_event(
            logger_gold_facts,
            "INFO",
            f"Gold table {gold_tbl} created",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id, 
            layer=GOLD_LAYER
        )
    else:
        log_event(
            logger_gold_facts,
            "INFO",
            f"Gold table {gold_tbl} already exists",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )


# -----------------------------
# 4. Merge Fact Table into Gold
# -----------------------------
def merge_fact_into_gold(
    df, gold_tbl, natural_keys, hash_column, master_run_id: str, layer_run_id: str, layer: str
):
    """
    Performs an idempotent merge from the Silver fact table into the Gold fact table.
    - Updates only rows where the hash has changed.
    - Inserts new records if they do not exist in the Gold table.

    Args:
        df (DataFrame): Prepared fact DataFrame to merge into the Gold table.
        gold_tbl (str): Name of the Gold table.
        natural_keys (list): List of business keys for matching records between tables.
        hash_column (str): Column name used for hashing records to track changes.
        log_event (logger_gold_facts): log_event instance to track progress.
    """
    log_event(
        logger_gold_facts,
        "INFO",
        f"Starting merge into Gold table {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    gold_delta = DeltaTable.forName(
        df.sparkSession, gold_tbl
    )  # Get a DeltaTable object for the Gold table
    merge_condition = " AND ".join(
        [f"tgt.{k} = src.{k}" for k in natural_keys]
    )  # Matching condition based on business keys

    # Perform the MERGE operation
    gold_delta.alias("tgt").merge(df.alias("src"), merge_condition).whenMatchedUpdate(
        condition=f"tgt.{hash_column} <> src.{hash_column}",  # Only update if hash values differ
        set={
            c: f"src.{c}" for c in df.columns
        },  # Set updated values from the source DataFrame
    ).whenNotMatchedInsertAll().execute()  # Insert new records if they don't match
    log_event(
        logger_gold_facts,
        "INFO",
        f"Merge completed for Gold table {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )


# -----------------------------
# 5. Collect Fact Metrics
# -----------------------------
def collect_fact_metrics(
    spark,
    df,
    metrics_table,
    layer_name,
    table_name,
    silver_table: str,
    table_type: str,
    start_ts,
    end_ts,
    duration_secs,
    run_status,
    notes,
    master_run_id: str,
    layer_run_id: str,
    layer: str
):
    """
    Collects operational observability metrics for fact pipelines, ensuring transparency into
    the performance of each pipeline run.

    Captures the following metrics:
    - rows_read (from Silver table)
    - rows_inserted (from Delta MERGE operation)
    - rows_updated (from Delta MERGE operation)
    - rows_unchanged (calculated as remaining rows)

    Args:
        spark (SparkSession): Active Spark session.
        df (DataFrame): Silver fact DataFrame.
        gold_tbl (str): Gold table to track changes against.
        layer_name (str): Name of the layer for logging.
        run_id (str): Unique identifier for the pipeline run.
        log_event (logger_gold_facts): log_event instance for tracking pipeline progress.

    Returns:
        None: The metrics are stored in a separate Delta table for visualization and monitoring.
    """
    log_event(
        logger_gold_facts,
        "INFO",
        f"Collecting fact metrics for {table_name}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    # -----------------------------
    # Detect missing source table
    # -----------------------------
    if len(df.columns) == 0:
        log_event(
            logger_gold_facts,
            "WARN",
            f"No table found in source {silver_table}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

        rows_read = 0
        rows_inserted = 0
        rows_updated = 0
        rows_unchanged = 0
        rows_soft_deleted = 0
        rows_current = 0
        throughput_rows_per_sec = 0.0
        skew_ratio = 0.0

        notes = f"Source silver table '{silver_table}' does not exist"

        has_rows = False

    else:
        # -----------------------------
        # Safe check if df has any rows
        # -----------------------------
        has_rows = df.limit(1).count() > 0

        # -----------------------------
        # Initialize defaults
        # -----------------------------
        rows_read = 0
        rows_inserted = 0
        rows_updated = 0
        rows_unchanged = rows_read
        rows_soft_deleted = 0
        rows_current = 0  # Fact tables usually append, no soft deletes unless modeled
        throughput_rows_per_sec = 0.0
        skew_ratio = 0.0

        # -----------------------------
        # If rows exist, get Delta MERGE metrics
        # -----------------------------
        if has_rows:
            try:
                rows_read = df.count()  # Only count if there are rows
                history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
                latest_merge = (
                    history_df.filter(col("operation") == "MERGE")
                    .orderBy(col("timestamp").desc())
                    .limit(1)
                    .collect()
                )

                if latest_merge:
                    op_metrics = latest_merge[0]["operationMetrics"]
                    rows_inserted = int(op_metrics.get("numInsertedRows", 0))
                    rows_updated = int(op_metrics.get("numUpdatedRows", 0))

                rows_unchanged = rows_read - rows_inserted - rows_updated

            except Exception as e:
                log_event(
                    logger_gold_facts,
                    "WARN",
                    f"Unable to fetch Delta MERGE metrics for {table_name}: {e}",
                    master_run_id=master_run_id,
                    layer_run_id=layer_run_id,
                    layer=GOLD_LAYER
                )

    # -----------------------------
    # Ensure defaults for timestamps & notes
    # -----------------------------
    start_ts = start_ts or spark.sql("SELECT current_timestamp() as ts").first()["ts"]
    end_ts = end_ts or spark.sql("SELECT current_timestamp() as ts").first()["ts"]
    duration_secs = duration_secs or 0
    notes = notes or ("No rows/files processed" if rows_read == 0 else "")

    # -----------------------------
    # Compute throughput and skew
    # -----------------------------
    throughput_rows_per_sec = round(rows_read / duration_secs, 2) if duration_secs > 0 else 0.0

    if has_rows:
        partition_counts = (
            df.withColumn("partition_id", spark_partition_id())
            .groupBy("partition_id")
            .count()
            .collect()
        )

        counts = [r["count"] for r in partition_counts]

        max_partition_rows = max(counts) if counts else 0
        avg_partition_rows = sum(counts) / len(counts) if counts else 1

        skew_ratio = round(max_partition_rows / avg_partition_rows, 2) if avg_partition_rows > 0 else 0.0
    else:
        skew_ratio = 0.0



    # ------------------------------------------------------------------
    # 1. Create table if not exists (enterprise-grade template pattern)
    # ------------------------------------------------------------------
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {metrics_table} (
        master_run_id STRING,
        layer_run_id STRING,
        layer_name STRING,
        source_table STRING,
        target_table STRING,
        table_type STRING,        

        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        duration_secs BIGINT,

        rows_read BIGINT,
        rows_inserted BIGINT,
        rows_updated BIGINT,
        rows_unchanged BIGINT,
        rows_soft_deleted BIGINT,
        rows_current BIGINT,

        throughput_rows_per_sec DOUBLE,
        skew_ratio DOUBLE,

        run_status STRING,
        load_timestamp TIMESTAMP, -- Timestamp in Australia/Sydney
        notes STRING
    )
    USING DELTA
    TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true
    )
    """)

    # ------------------------------------------------------------------
    # 2. Explicit schema definition for DataFrame (avoids inference issues)
    # ------------------------------------------------------------------
    schema = StructType([
        StructField("master_run_id", StringType(), False),
        StructField("layer_run_id", StringType(), False),
        StructField("layer_name", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("table_type", StringType(), True),

        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("duration_secs", LongType(), True),

        StructField("rows_read", LongType(), True),
        StructField("rows_inserted", LongType(), True),
        StructField("rows_updated", LongType(), True),
        StructField("rows_unchanged", LongType(), True),
        StructField("rows_soft_deleted", LongType(), True),
        StructField("rows_current", LongType(), True),

        StructField("throughput_rows_per_sec", DoubleType(), True),
        StructField("skew_ratio", DoubleType(), True),

        StructField("run_status", StringType(), True),
        StructField("notes", StringType(), True),
    ])

    # ------------------------------------------------------------------
    # 3. Create a single-row DataFrame representing this execution event
    # ------------------------------------------------------------------
    metrics_df = spark.createDataFrame(
        [(
            master_run_id,
            layer_run_id,
            layer_name,
            silver_table,
            table_name,
            table_type,

            start_ts,
            end_ts,
            duration_secs,

            rows_read,
            rows_inserted,
            rows_updated,
            rows_unchanged,
            None,                  # rows_soft_deleted → NULL for fact
            None,                  # rows_current→ NULL for fact

            throughput_rows_per_sec,
            skew_ratio,

            run_status,
            notes
        )],
        schema=schema
    ).withColumn(
        "load_timestamp", current_timestamp()
    )

    # Append metrics to dedicated dashboard table
    metrics_df.write.format("delta").mode("append").saveAsTable(metrics_table)

    log_event(
        logger_gold_facts,
        "INFO",
        f"Fact metrics recorded | "
        f"read={rows_read}, inserted={rows_inserted}, updated={rows_updated}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )


# -----------------------------
# 6. Optimize Gold Table
# -----------------------------
def optimize_gold_table(spark, gold_tbl, z_order_cols, master_run_id: str, layer_run_id: str, layer: str):
    """
    Optimizes the Delta Gold table using Z-Ordering on key columns to enhance query performance.
    Z-Ordering improves partition pruning during query execution.

    Args:
        spark (SparkSession): Active Spark session.
        gold_tbl (str): Name of the Gold table to optimize.
        z_order_cols (list): List of columns to apply Z-Ordering on.
        log_event (logger_gold_facts): log_event instance to track progress.
    """
    log_event(
        logger_gold_facts,
        "INFO",
        f"Optimizing {gold_tbl} using ZORDER BY {z_order_cols}", master_run_id=master_run_id, layer_run_id=layer_run_id, layer=GOLD_LAYER)
    
    if not spark.catalog.tableExists(gold_tbl):
        log_event(
            logger_gold_facts,
            "WARN",
            f"Gold table {gold_tbl} does not exist. Skipping z-odering operation.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return
    spark.sql(
        f"OPTIMIZE {gold_tbl} ZORDER BY ({','.join(z_order_cols)})"
    )  # Perform Z-Ordering for optimization
    log_event(logger_gold_facts, "INFO", f"Optimization completed for {gold_tbl}", master_run_id=master_run_id, layer_run_id=layer_run_id, layer=GOLD_LAYER)


# -----------------------------
# 7. Vacuum Gold Table
# -----------------------------
def vacuum_gold_table(
    spark, gold_tbl, master_run_id: str, layer_run_id: str,layer: str, retention_hours=168
):
    """
    Performs a Delta VACUUM operation to remove stale files after a specified retention period.
    This step helps in cleaning up files and improving storage efficiency.

    Args:
        spark (SparkSession): Active Spark session.
        gold_tbl (str): Name of the Gold table to vacuum.
        retention_hours (int): Number of hours to retain old data (default is 168 hours).
        log_event (logger_gold_facts): log_event instance to track the operation.

    Returns:
        None
    """
    log_event(
        logger_gold_facts,
        "INFO",
        f"Vacuuming {gold_tbl}, retention={retention_hours} hours",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )

    if not spark.catalog.tableExists(gold_tbl):
        log_event(
            logger_gold_facts,
            "WARN",
            f"Gold table {gold_tbl} does not exist. Skipping vacuuming operation.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return

    spark.sql(
        f"VACUUM {gold_tbl} RETAIN {retention_hours} HOURS"
    )  # Clean up old files from Delta table

    log_event(
        logger_gold_facts,
        "INFO",
        f"Vacuum completed for {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
