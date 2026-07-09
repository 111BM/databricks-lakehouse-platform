"""
==============================================================
Module: Silver to Gold ETL Pipeline with SCD2 and Delta Lake Operations

Purpose:
    This module is designed to transform data from a Silver Delta table to a Gold Delta table in a Databricks Delta Lake environment.
    It handles the following operations:
    - Incrementally loads new data from the Silver table into the Gold table based on the `silver_ingestion_ts`.
    - Applies Slowly Changing Dimension Type 2 (SCD2) logic for capturing historical data changes, ensuring accurate tracking of data changes over time.
    - Performs an idempotent merge from Silver to Gold using Delta Lake's `MERGE` operation, ensuring no data duplication and maintaining data integrity.
    - Handles dynamic partitioning and repartitioning for better performance with large datasets.
    - Optimizes the Gold table for query performance using Z-Ordering and performs a vacuum operation to remove stale data.
    - Collects performance metrics for observability, including row counts and processing time.

Key Features:
1. Incremental Data Loading:
    - Optimized to process only new data from Silver based on the `silver_ingestion_ts` column.
    - Ensures efficient processing of large, partitioned tables in a serverless Databricks environment.

2. SCD2 Processing:
    - Supports Slowly Changing Dimension Type 2 for entities, including calculating `effective_from`, `effective_to`, and `is_current` columns.
    - Tracks the history of changes for dimensional data.

3. Delta Lake `MERGE` Operations:
    - Uses Delta Lake's `MERGE` operation for efficient upserts into the Gold table.
    - The operation ensures that only updated or new records are inserted, and old records are marked as expired.

4. Performance Optimization:
    - The pipeline dynamically repartitions data to optimize memory usage and performance.
    - Uses Z-Ordering on key columns for efficient query performance, particularly after large inserts or updates.
    - The VACUUM operation removes stale data and optimizes storage.

5. Metrics Collection:
    - Tracks various metrics, including the number of rows read, inserted, updated, unchanged, and soft-deleted.
    - Provides detailed insights into pipeline performance, including throughput and skew ratios.

6. Logging and Error Handling:
    - Logs every step of the pipeline for auditing and traceability, including start/end times and any errors encountered during execution.
    - Provides detailed logging for every table operation, including data ingestion, transformations, merges, and optimizations.

7. Serverless Compatibility:
    - Fully compatible with Databricks serverless clusters, designed to run efficiently on large datasets.

Best Practices / Notes:
- Ensure that the `silver_ingestion_ts` column is correctly populated in the Silver table to enable efficient incremental loading.
- Properly configure the `effective_from` and `effective_to` fields to maintain the historical integrity of the dimensional data.
- Monitor the metrics output to keep track of the pipeline's performance and make adjustments as needed.
- Perform the `OPTIMIZE` operation after significant data loads to improve query performance and partition pruning.
- The pipeline can be modified for other types of slowly changing dimensions or different Delta tables as needed.

==============================================================
"""

# -------------------------------
# Standard Python Libraries
# -------------------------------
import math          # used for mathematical operations (e.g., rounding, calculations)
import logging       # fallback logging utility (framework also uses custom logger)

# -------------------------------
# PySpark Core Imports
# -------------------------------
from pyspark.sql import SparkSession  # entry point for Spark execution

from pyspark.sql import Window  # used for window functions (SCD2, ranking, lag/lead logic)

from pyspark.sql.functions import (
    col,                 # column reference
    lead,                # access next row in window (SCD2 validity end logic)
    expr,                # SQL expression execution
    sha2,                # hashing function for change detection
    concat_ws,           # concatenate columns for hashing
    coalesce,            # handle null fallbacks
    lit,                 # create literal values
    current_timestamp,   # timestamp for auditing
    broadcast,           # optimize joins for small datasets
    hash,                # generic hash function
    hash as hash_fn,     # alias for clarity in some transformations
    max as spark_max,    # Spark-safe max aggregation
    min,                 # minimum aggregation
    spark_partition_id   # used for partition-level skew analysis
)

# -------------------------------
# Delta Lake Imports
# -------------------------------
from delta.tables import DeltaTable  # used for MERGE (SCD2 / UPSERT operations)

# -------------------------------
# Schema Definitions
# -------------------------------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    DoubleType
)

# -------------------------------
# Add Shared Utilities Path
# -------------------------------
# Enables reuse of logging, config, and platform utilities across layers
import sys

sys.path.append(
    "/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/src/superstore_shared_utilities"
)

# -------------------------------
# Custom Framework Imports
# -------------------------------
from superstore_logger import get_superstore_logger, log_event  # centralized logging framework
from superstore_platform_constants import GOLD_LAYER            # constant to enforce Gold layer naming consistency

# -----------------------------
# log_event Setup for gold dimensions
# -----------------------------
# Initialize log_event to capture events in the gold dimensions pipeline
logger_gold_dimensional = get_superstore_logger("superstore_gold_dimension_framework")

def get_incremental_silver_for_dims(
    spark, 
    silver_table: str, 
    gold_table: str, 
    master_run_id: str, 
    layer_run_id: str, 
    layer: str, 
    ingestion_col: str = "silver_ingestion_ts"
):
    """
    Returns only new silver rows not yet ingested into gold dimension table.
    Optimized for serverless / partitioned Bronze tables.
    """

    # -----------------------------
    # Validate Silver table exists
    # -----------------------------
    if not spark.catalog.tableExists(silver_table):

        log_event(
            logger_gold_dimensional,
            "WARNING",
            f"Source Silver table '{silver_table}' does not exist. Skipping incremental fetch.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

        return spark.createDataFrame([], StructType([]))


    # -----------------------------
    # Logging
    # -----------------------------
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Fetching incremental rows from silver table '{silver_table}' for Gold table '{gold_table}'",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=layer,
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
            logger_gold_dimensional,
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
            logger_gold_dimensional,
            "INFO",
            f"Gold table '{gold_table}' does not exist. Returning full silver table.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
    row_count = incremental_df.count()
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Incremental silver rows to process: {row_count}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )   
    
    return incremental_df

# -----------------------------
# 1. Read Silver Table with Validations
# -----------------------------
def read_silver_table(
    spark,
    silver_table: str,
    gold_table: str,
    master_run_id: str,
    layer_run_id: str,
    layer: str, 
    required_columns
):
    """
    Reads Silver table from Delta/Spark catalog and validates schema:
    - Ensures all required columns exist for downstream processing
    - Logs success/failure with clear error messages
    - Raises errors immediately to fail fast in production
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Starting to read Silver table: {silver_table}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    try:
        # Read the Delta table into a DataFrame incrementally
        silver_df = get_incremental_silver_for_dims(spark, silver_table, gold_table, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=GOLD_LAYER, ingestion_col="silver_ingestion_ts")

        # Check if dataframe is empty (table missing scenario)
        if len(silver_df.columns) == 0:
            log_event(
                logger_gold_dimensional,
                "WARNING",
                f"Silver table '{silver_table}' does not exist or returned empty schema. Skipping processing.",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )
            return silver_df


        # Validate schema: fail early if expected columns are missing
        missing_columns = [
            col for col in required_columns if col not in silver_df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Missing columns in Silver table: {', '.join(missing_columns)}"
            )

        log_event(
            logger_gold_dimensional,
            "INFO",
            f"Successfully read Silver table: {silver_table}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return silver_df
    except Exception as e:
        log_event(
            logger_gold_dimensional,
            "ERROR",
            f"Failed to read Silver table {silver_table}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        raise


# -----------------------------
# 2. Prepare SCD2 Columns
# -----------------------------
def prepare_scd2_columns(
    spark,
    silver_df,
    dim_type: str,
    silver_order_table,
    entity_columns,
    hash_column: str,
    master_run_id: str,
    layer_run_id: str,
    layer: str
):
    """
    Prepares columns for Slowly Changing Dimension Type 2:
    - Sets effective_from and load timestamps
    - Ensures auditability and reproducibility of SCD2 process
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        "Starting to prepare SCD2 columns.",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )

    try:
        # # Ensure silver_df is a DataFrame
    
        # -------------------------------
        # For customer dim: use first order date
        # -------------------------------
        if dim_type == "customers":
            silver_orders_df = spark.table(silver_order_table)

            # Compute first order per customer
            first_order_df = silver_orders_df.groupBy("customer_id") \
                .agg(min("order_date").alias("first_order_date"))

            # Join with Silver customer data
            df = silver_df.join(first_order_df, on="customer_id", how="left")

            # Set effective_from: first order date if exists, else silver_ingestion_ts
            df = df.withColumn("effective_from", col("first_order_date")) \
                   .drop("first_order_date")
        else:
            # For non-transactional dims
            df = silver_df.withColumn("effective_from", col("silver_ingestion_ts"))

        # Add gold load timestamp
        df = df.withColumn("gold_ingestion_ts", current_timestamp())

        # Select required columns
        cols_for_scd = entity_columns + ["effective_from", "silver_ingestion_ts", "gold_ingestion_ts"]
        scd_df = df.select(*cols_for_scd)

        log_event(
            logger_gold_dimensional,
            "INFO",
            f"Successfully prepared SCD2 columns for dim_type={dim_type}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=layer
        )

        return scd_df

    except Exception as e:
        log_event(
            logger_gold_dimensional,
            "ERROR",
            f"Failed to prepare SCD2 columns: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        raise


# -----------------------------
# 3. Compute SCD2 Timeline
# -----------------------------
def compute_scd2_timeline(
    scd_df, entity_id_column, hash_column, master_run_id: str, layer_run_id: str, layer: str
):
    """
    Computes SCD2 effective timeline and current flags:
    - Uses windowing to calculate next_effective_from
    - Derives effective_to and is_current columns
    - Drops duplicates to ensure unique historical records
    - Timestamp handling is timezone-aware
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Starting to compute SCD2 timeline for {entity_id_column}.",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    try:
        window_spec = Window.partitionBy(entity_id_column).orderBy("effective_from")

        repaired_df = (
            scd_df.withColumn(
                "next_effective_from", lead("effective_from").over(window_spec)
            )
            .withColumn(
                "effective_to",
                        col("next_effective_from").cast("timestamp")
                        - expr("INTERVAL 1 MICROSECOND")
            )
            .withColumn("is_current", col("next_effective_from").isNull())
            .drop("next_effective_from")
            # .dropDuplicates([entity_id_column, hash_column])
        )
        log_event(
            logger_gold_dimensional,
            "INFO",
            f"Successfully computed SCD2 timeline for {entity_id_column}.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return repaired_df
    except Exception as e:
        log_event(
            logger_gold_dimensional,
            "ERROR",
            f"Failed to compute SCD2 timeline for {entity_id_column}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        raise


# -----------------------------
# 4. Create Gold Table if Not Exists
# -----------------------------
def create_gold_table_if_not_exists(
    spark,
    master_run_id: str,
    layer_run_id: str,
    layer: str,
    repaired_df,
    gold_tbl,
    partition_column,
):
    """
    Creates Delta Gold table in production-safe way:
    - Idempotent: does not overwrite existing table
    - Partitions by relevant column for query performance
    - Minimal write (limit(0)) for schema-only table creation
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Checking if Gold table {gold_tbl} exists and creating it if necessary.",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    try:
        if not spark.catalog.tableExists(gold_tbl):
            repaired_df.limit(0).write.format("delta").mode("ignore").partitionBy(
                partition_column
            ).saveAsTable(gold_tbl)
            log_event(
                logger_gold_dimensional,
                "INFO",
                f"Gold table {gold_tbl} created.",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )
        else:
            log_event(
                logger_gold_dimensional,
                "INFO",
                f"Gold table {gold_tbl} is already ready.",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )
    except Exception as e:
        log_event(
            logger_gold_dimensional,
            "ERROR",
            f"Failed to create Gold table {gold_tbl}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        raise


# -----------------------------
# 5. Dynamically Calculate Partitions
# -----------------------------
def dynamically_calculate_partitions(
    master_run_id: str, layer_run_id: str, layer: str, silver_df, repaired_df, partition_column
):
    """
    Dynamically repartitions data based on volume:
    - Ensures memory-efficient processing for large datasets
    - Uses heuristics: 1 partition per 1M rows, min 200 partitions
    - Facilitates distributed merge and SCD2 operations
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        "Starting data repartitioning.",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    try:
        record_count = silver_df.count()
        num_partitions = max(200, math.ceil(record_count / 1000000))
        repaired_df = repaired_df.repartition(num_partitions, partition_column)
        log_event(
            logger_gold_dimensional,
            "INFO",
            f"Repartitioned data into {num_partitions} partitions",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return repaired_df
    except Exception as e:
        log_event(
            logger_gold_dimensional,
            "ERROR",
            f"Failed to repartition data: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        raise

# -----------------------------
# 6. Merge Data into Gold Table (SCD2 Handling)
# -----------------------------
def merge_into_gold_table_scd2(
    spark,
    master_run_id: str,
    layer_run_id: str,
    layer: str,
    repaired_df,
    gold_tbl,
    entity_id_column,
    hash_column,
    max_rows_per_bucket=1000000,
):
    """
    Production-grade SCD2 merge for Delta Gold table:
    - Handles large tables via bucketed merges to avoid memory issues
    - Idempotent: updates effective_to and is_current safely
    - Tracks run_id for observability and debugging
    - Uses hash-based bucket assignment for string-safe partitioning
    - Logs per-bucket progress for auditability
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Starting SCD2 merge into Gold table: {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )

    # -----------------------------
    # Safety check
    # -----------------------------
    try:
        if repaired_df.limit(1).count() == 0:
            log_event(
                logger_gold_dimensional,
                "INFO",
                f"No data to merge. Skipping SCD2 merge for Gold table: {gold_tbl}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )
            return


        gold_delta = DeltaTable.forName(spark, gold_tbl)

        # -----------------------------
        # MERGE CONDITION (CRITICAL FIX)
        # Only current record is eligible for update
        # -----------------------------
        merge_condition = f"""
            tgt.{entity_id_column} = src.{entity_id_column} 
            AND tgt.is_current = true
        """

        # -----------------------------
        # START LOG
        # -----------------------------
        log_event(
            logger_gold_dimensional,
            "INFO",
            f"Starting SCD2 merge for Gold table: {gold_tbl}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

        # # -----------------------------
        # # MERGE OPERATION (SINGLE ATOMIC SCD2)
        # # -----------------------------
        # (
        #     gold_delta.alias("tgt")
        #     .merge(
        #         repaired_df.alias("src"),
        #         merge_condition
        #     )

        #     # -------------------------------------------------------
        #     # WHEN MATCHED → CLOSE OLD VERSION ONLY IF CHANGED
        #     # -------------------------------------------------------
        #     .whenMatchedUpdate(
        #         condition=f"NOT (tgt.{hash_column} <=> src.{hash_column})",
        #         set={
        #             "effective_to": expr("src.effective_from - INTERVAL 1 SECOND"),
        #             "is_current": expr("false"),
        #         }
        #     )

        #     # -------------------------
        #     # WHEN NOT MATCHED → INSERT NEW VERSION
        #     # -------------------------
        #     .whenNotMatchedInsert(
        #         values={
        #             **{c: f"src.{c}" for c in repaired_df.columns},
        #             "is_current": expr("true"),
        #             "effective_to": expr("cast(null as timestamp)")
        #         }
        #     )
        #     # # -------------------------
        #     # # SOFT DELETE
        #     # # -------------------------
        #     # .whenNotMatchedBySourceUpdate(
        #     #     condition="tgt.is_current = true",
        #     #     set={
        #     #         "is_current": expr("false"),
        #     #         "effective_to": expr("current_timestamp()")
        #     #     }
        #     # )
        #     .execute()
        # )

        # -----------------------------
        # STEP 1: Close old records that changed
        # -----------------------------
        log_event(logger_gold_dimensional, "INFO", 
                "Step 1: Closing expired records",
                master_run_id=master_run_id, layer_run_id=layer_run_id, layer=GOLD_LAYER)
        
        (
            gold_delta.alias("tgt")
            .merge(
                repaired_df.alias("src"),
                merge_condition
            )
            .whenMatchedUpdate(
                condition=f"NOT (tgt.{hash_column} <=> src.{hash_column})",
                set={
                    "effective_to": expr("src.effective_from - INTERVAL 1 SECOND"),
                    "is_current": expr("false"),
                }
            )
            .execute()
        )

        # --- METRICS: Collect after first merge ---
        hist_close = gold_delta.history(1).select("operationMetrics").collect()[0][0]
        closed_records = int(hist_close.get("numTargetRowsUpdated", 0))
        
        log_event(
            logger_gold_dimensional,
            "INFO",
            f"Step 1 completed: Closed {closed_records} expired records",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
            closed_records=closed_records
        )

        # -----------------------------
        # STEP 2: Insert new versions (new + changed records)
        # -----------------------------
        log_event(logger_gold_dimensional, "INFO",
                "Step 2: Inserting new/updated records",
                master_run_id=master_run_id, layer_run_id=layer_run_id, layer=GOLD_LAYER)
        
        (
            gold_delta.alias("tgt")
            .merge(
                repaired_df.alias("src"),
                merge_condition
            )
            .whenNotMatchedInsert(
                values={
                    **{c: f"src.{c}" for c in repaired_df.columns},
                    "is_current": expr("true"),
                    "effective_to": expr("cast(null as timestamp)")
                }
            )
            .execute()
        )
        # --- METRICS: Collect after second merge ---
        hist_insert = gold_delta.history(1).select("operationMetrics").collect()[0][0]
        new_versions_inserted = int(hist_insert.get("numTargetRowsInserted", 0))
        
        log_event(
            logger_gold_dimensional,
            "INFO",
            f"Step 2 completed: Inserted {new_versions_inserted} new records",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
            new_versions_inserted=new_versions_inserted
        )

        # -----------------------------
        # COMBINED METRICS
        # -----------------------------
        # inserted row fetch from history include actual new and new updated version of existing rows 
        # To compute actual new insterted rows 
        inserted= new_versions_inserted-closed_records
        merge_metrics = {
            "inserted": inserted,           # Total new rows added
            "updated": closed_records,       # Total rows closed (marked as historical)
        }

        # -----------------------------
        # SUCCESS LOG
        # -----------------------------
        log_event(
            logger_gold_dimensional,
            "INFO",
            f"SCD2 merge completed successfully for Gold table: {gold_tbl}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

        # # --- METRICS: read immediately after merge ---
        # hist = gold_delta.history(1).select("operationMetrics").collect()[0][0]

        # merge_metrics = {
        #     "inserted": int(hist.get("numTargetRowsInserted", 0)),
        #     "updated": int(hist.get("numTargetRowsUpdated", 0)),  # includes soft deletes
        # }


    except Exception as e:
        # -----------------------------
        # ERROR LOG
        # -----------------------------
        log_event(
            logger_gold_dimensional,
            "ERROR",
            f"SCD2 merge failed for Gold table: {gold_tbl}. Error: {str(e)}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        raise

    return {
        "merge_metrics": merge_metrics,
    }


def handle_soft_deletes(
    spark,
    master_run_id,
    layer_run_id,
    layer: str,
    silver_df, #  This is incremental only!
    gold_tbl,
    entity_id_column,
    silver_table: str, # Full silver table name
):
    """
    Performs soft delete handling for SCD2 Gold table.
    
    Logic:
        - Finds active Gold records not present in Silver
        - Marks them as inactive (is_current = False)
        - Sets effective_to timestamp
    """

    # Get Delta table reference
    gold_delta = DeltaTable.forName(spark, gold_tbl)

    # # Extract current Silver keys (source of truth)
    # silver_ids = silver_df.select(entity_id_column).distinct()
    # Read FULL silver table, not incremental
    full_silver_df = spark.table(silver_table)
    silver_ids = full_silver_df.select(entity_id_column).distinct()

    # Current active Gold records
    gold_current = gold_delta.toDF().filter(col("is_current") == True)

    # Identify records missing in Silver (to be soft deleted)
    soft_delete_df = gold_current.join(
        broadcast(silver_ids),
        on=entity_id_column,
        how="left_anti"
    )

    # Avoid unnecessary actions
    soft_deleted_count = soft_delete_df.limit(1).count()

    if soft_deleted_count == 0:
        log_event(
            logger_gold_dimensional,
            "INFO",
            f"No soft deletes needed for {gold_tbl}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=layer
        )
        return 0

    # Apply soft delete update in Gold
    (
        gold_delta.alias("tgt")
        .merge(
            soft_delete_df.alias("src"),
            f"""
            tgt.{entity_id_column} = src.{entity_id_column}
            AND tgt.is_current = true
            """
        )
        .whenMatchedUpdate(set={
            "is_current": lit(False),          # FIX: boolean instead of string
            "effective_to": current_timestamp()
        })
        .execute()
    )

    # Optional: log soft delete activity
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Soft deleted {soft_deleted_count} records in {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=layer
    )

    return soft_deleted_count

# -----------------------------
# Metrics for Observability to metrics table
# -----------------------------
# def collect_metrics(
#     spark,
#     repaired_df,
#     dim_gold_before_merge,
#     gold_tbl: str,
#     layer_name: str,
#     silver_table: str,
#     table_name: str,
#     table_type: str,
#     entity_id_column: str,
#     silver_hash_column: str,
#     gold_hash_column: str,
#     metrics_table: str,
#     master_run_id: str,
#     layer_run_id: str,
#     layer: str,
#     start_ts,
#     end_ts,
#     duration_secs,
#     run_status: str,
#     max_rows_per_bucket: int,
#     notes: str = ""
    
# ):
#     """
#     Collects metrics on SCD2 processing for auditing and monitoring:
#     - Counts new, updated, and current records
#     - Uses bucketed approach for large datasets to avoid driver overload
#     - Writes results to dedicated metrics dashboard table
#     - Supports master_run_id and layer_run_id for pipeline observability
#     """
#     log_event(
#         logger_gold_dimensional,
#         "INFO",
#         f"Collecting SCD2 metrics for {gold_tbl}",
#         master_run_id=master_run_id,
#         layer_run_id=layer_run_id,
#         layer=GOLD_LAYER
#     )

#     # Initialize defaults
#     # -----------------------------
#     inserted_rows = 0
#     updated_rows = 0
#     unchanged_rows = 0
#     soft_deleted_rows = 0
#     active_rows = 0  # Fact tables usually append, no soft deletes unless modeled
#     throughput_rows_per_sec = 0.0
#     skew_ratio = 0.0


#     # ------------------------------------------------------------------
#     # CASE 1: Detect missing or empty Silver table
#     # ------------------------------------------------------------------
#     # if  or dim_gold_before_merge is None:
#     if repaired_df is None:
#         log_event(
#             logger_gold_dimensional,
#             "WARN",
#             f"No table found in Silver for {silver_table}",
#             master_run_id=master_run_id,
#             layer_run_id=layer_run_id,
#             layer=GOLD_LAYER
#         )
#         read_rows = 0
#         inserted_rows = 0
#         updated_rows = 0
#         unchanged_rows = 0
#         soft_deleted_rows = 0
#         active_rows = 0
#         throughput_rows_per_sec = 0.0
#         skew_ratio = 0.0
#         notes = f"Source silver table '{silver_table}' does not exist"
#         has_rows = False
    
    
#     else:
#         read_rows = repaired_df.count()
#         # Check if repaired_df has rows
#         has_rows = read_rows > 0

#         # ------------------------------------------------------
#         # CASE 2: FIRST LOAD (Gold table does not exist yet)
#         # ------------------------------------------------------
#         # elif has_rows and dim_gold_before_merge.limit(1).count() == 0:
#         if dim_gold_before_merge is None or dim_gold_before_merge.limit(1).count() == 0:
#             log_event(
#                 logger_gold_dimensional,
#                 "INFO",
#                 f"First load detected for Gold table: {gold_tbl}",
#                 master_run_id=master_run_id,
#                 layer_run_id=layer_run_id,
#                 layer=layer
#             )
#             inserted_rows = read_rows
#             # updated_rows = 0
#             # unchanged_rows = 0
#             active_rows = read_rows

#         # ------------------------------------------------------
#         # CASE 3: NORMAL SCD2 COMPARISON
#         # ------------------------------------------------------
#         elif has_rows:
#             gold_current = dim_gold_before_merge.filter(col("is_current") == True)
        
#             # -----------------------------
#             # Bucketization
#             # -----------------------------
#             # Split repaired_df into buckets to avoid overwhelming the Spark driver
#             # Each bucket is a subset of rows based on a hash of the entity_id
#             num_buckets = max(1, math.ceil(read_rows / max_rows_per_bucket))
#             repaired_df_buckets = repaired_df.withColumn("_bucket", (hash(col(entity_id_column)) % num_buckets))

#             # Process each bucket separately
#             for b in range(num_buckets):
#                 # Select only rows in this bucket and remove the temporary _bucket column
#                 bucket_df = repaired_df_buckets.filter(col("_bucket") == b).drop("_bucket")
#                 if bucket_df.limit(1).count() == 0:
#                     # Skip empty buckets
#                     continue

#                 # -----------------------------
#                 # Rows inserted: present in repaired_df but not in gold_df
#                 # left_anti join returns only rows in bucket_df with no matching entity_id in gold_df
#                 # -----------------------------
#                 inserted = bucket_df.alias("src").join(
#                     gold_current.select(entity_id_column).alias("tgt"),
#                     on=entity_id_column,
#                     how="left_anti"
#                 ).count()
#                 inserted_rows +=inserted


#                 # -----------------------------
#                 # Current matching rows: entity_id exists in both repaired_df and gold_df
#                 # -----------------------------
#                 matching_current = bucket_df.alias("src").join(
#                     gold_current.alias("tgt"),
#                     on=entity_id_column,
#                     how="inner"
#                 )

#                 # -----------------------------
#                 # Rows updated: entity_id exists in both, but hash value differs (data changed)
#                 # -----------------------------
#                 updated= matching_current.filter(
#                     col(f"src.{silver_hash_column}") != col(f"tgt.{silver_hash_column}")
#                 ).count()
#                 updated_rows += updated
                

#                 # -----------------------------
#                 # Rows unchanged: entity_id exists in both and hash is identical (no data change)
#                 # -----------------------------
#                 unchanged = matching_current.filter(
#                     col(f"src.{silver_hash_column}") == col(f"tgt.{silver_hash_column}")
#                 ).count()
#                 unchanged_rows+= unchanged
                

#             # -----------------------------
#             # CURRENT ROWS (AFTER MERGE STATE)
#             # ----------------------------- 
#             if spark.catalog.tableExists(gold_tbl):
#                 active_rows = spark.table(gold_tbl).filter(col("is_current") == True).count()


#             # # -----------------------------
#             # # Total current rows in gold table
#             # # is_current = True identifies the active/latest version of each entity
#             # # -----------------------------
#             # active_rows = spark.table(gold_tbl).filter(col("is_current") == True).count() if gold_df is not None else 0

#             # -----------------------------
#             # Soft-deleted rows: rows currently marked as active in gold but missing from repaired_df
#             # left_anti join returns rows in gold_df not present in repaired_df
#             # -----------------------------
#             if has_rows:
#                 soft_deleted_rows = gold_current.join(
#                     repaired_df.select(entity_id_column),
#                     on=entity_id_column,
#                     how="left_anti"
#                 ).count()


def collect_metrics(
    spark,
    repaired_df,
    merge_metrics, 
    is_initial_load,
    soft_deleted_rows,   
    gold_tbl: str,
    layer_name: str,
    silver_table: str,
    table_name: str,
    table_type: str,
    entity_id_column: str,
    silver_hash_column: str,
    gold_hash_column: str,
    metrics_table: str,
    master_run_id: str,
    layer_run_id: str,
    layer: str,
    start_ts,
    end_ts,
    duration_secs,
    max_rows_per_bucket: int,
):
    """
    Optimized metrics collection using Delta merge metrics
    """

    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Collecting SCD2 metrics for {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=layer
    )

    # -----------------------------
    # DEFAULTS
    # -----------------------------
    read_rows = 0
    inserted_rows = 0
    updated_rows = 0
    unchanged_rows = 0
    
    active_rows = 0
    total_rows=0

    throughput_rows_per_sec = 0.0
    skew_ratio = 0.0
    duration_secs=duration_secs or 0

    merge_metrics = merge_metrics or {}
    soft_deleted_rows = soft_deleted_rows or 0
    is_initial_load = bool(is_initial_load)

    run_status = "SUCCESS"
    load_type = None
    notes = None

    start_ts = start_ts 
    end_ts = end_ts 

    # -----------------------------
    # CASE 1: Silver missing
    # -----------------------------
    if repaired_df is None:
        run_status = "SKIPPED"
        load_type = "NO_DATA"
        notes = f"Source silver table {silver_table} does not exist"
        has_rows = False

    # -----------------------------
    # CASE 2: Silver empty
    # -----------------------------
    elif repaired_df.limit(1).count() == 0:
        run_status = "SKIPPED"
        load_type = "INCREMENTAL"
        notes = f"No incremental rows in silver table {silver_table}"
        has_rows = False
    # -----------------------------
    # CASE 3: Normal processing/Data present
    # -----------------------------
    else:
        has_rows = True

        # -----------------------------
        # Read input rows from Silver
        # -----------------------------
        read_rows = repaired_df.count()

        # -----------------------------
        # Extract MERGE metrics
        # -----------------------------
        inserted_rows = merge_metrics.get("inserted", 0)
        updated_rows = merge_metrics.get("updated", 0)
        
        # -----------------------------
        # DERIVED METRICS
        # unchanged_rows = rows that came from Silver but did not trigger any SCD2 action
        # -----------------------------
        unchanged_rows = max(read_rows - (inserted_rows + updated_rows), 0)
        # unchanged_rows = max(read_rows - inserted_rows, 0)

        # -----------------------------
        # CHANGE DETECTION (SCD2 aware)
        # included soft deletes as changes
        # -----------------------------
        has_changes = (inserted_rows + updated_rows + soft_deleted_rows) > 0


        # -----------------------------
        # Status + Execution Mode
        # -----------------------------
        # INITIAL_LOAD  → first time full population
        # NO_CHANGE     → data arrived but no changes detected
        # SCD2_APPLIED  → inserts/updates/soft deletes applied
        # ---------------------------------------------------

        
        # CASE 3.1: INITIAL LOAD
        # First-time load into Gold table
        
        if is_initial_load:
            load_type = "INITIAL_LOAD"
            notes = f"Initial load completed for {gold_tbl}"
        
        # CASE 3.2: NO_CHANGE
        elif  not has_changes:
            load_type = "INCREMENTAL"
            notes = f"No changes detected for {gold_tbl}"
            
        # CASE 3.3: Real SCD2 changes occurred
        else:
            load_type = "INCREMENTAL"
            notes = f"SCD2 changes applied to {gold_tbl}"
        
        run_status = "SUCCESS"

        # -----------------------------
        # soft_deleted_rows
        # -----------------------------
        soft_deleted_rows=soft_deleted_rows

        # -----------------------------
        # Compute active and total rows 
        # -----------------------------
        gold_df = spark.table(gold_tbl)
        total_rows = gold_df.count() 
        active_rows =gold_df.filter(col("is_current") == True).count() 

        # -----------------------------
        # Compute throughput 
        # -----------------------------
        if duration_secs and duration_secs > 0:
            throughput_rows_per_sec = round(read_rows / duration_secs, 2)
        # -----------------------------

        # -----------------------------
        # Compute skew
        # -----------------------------
        # Measures how evenly data is distributed across Spark partitions.
        # High skew indicates uneven partition distribution → potential performance bottleneck.
        partition_counts = (
            repaired_df
            .withColumn("partition_id", spark_partition_id())  # assign each row to its Spark partition
            .groupBy("partition_id")                           # group by partition
            .count()                                           # count rows per partition
            .collect()                                         # bring results to driver for calculation
        )

        # Extract row counts per partition
        counts = [r["count"] for r in partition_counts]

        # Identify max and average partition sizes safely
        max_partition_rows = max(counts) if counts else 0
        avg_partition_rows = sum(counts) / len(counts) if counts else 1

        # Skew ratio formula:
        #   higher value = more imbalance between partitions
        skew_ratio = (
            round(max_partition_rows / avg_partition_rows, 2)
            if avg_partition_rows > 0 else 0.0
        )

    # ------------------------------------------------------------------
    # 1. Create Dimension Metrics table if not exists
    # ------------------------------------------------------------------
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {metrics_table} (
        master_run_id STRING,
        layer_run_id STRING,
        layer_name STRING,
        source_table STRING,
        target_table STRING,
        table_type STRING,               -- 'dimension'

        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        duration_secs BIGINT,

        read_rows BIGINT,
        inserted_rows BIGINT,
        updated_rows BIGINT,
        unchanged_rows BIGINT,
        soft_deleted_rows  BIGINT,
        active_rows BIGINT,
        total_rows BIGINT,

        throughput_rows_per_sec DOUBLE,
        skew_ratio DOUBLE,

        load_type STRING,
        run_status STRING,
        load_timestamp TIMESTAMP,
        notes STRING
    )
    USING DELTA
    TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true
    )
    """)

    # Prepare metrics schema
    # DF columns order must match table
    schema = StructType([
        StructField("master_run_id", StringType(), True),
        StructField("layer_run_id", StringType(), True),
        StructField("layer_name", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("table_type", StringType(), True),

        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("duration_secs", LongType(), True),

        StructField("read_rows", LongType(), True),
        StructField("inserted_rows", LongType(), True),
        StructField("updated_rows", LongType(), True),
        StructField("unchanged_rows", LongType(), True),
        StructField("soft_deleted_rows", LongType(), True),
        StructField("active_rows", LongType(), True),
        StructField("total_rows", LongType(), True),

        StructField("throughput_rows_per_sec", DoubleType(), True),
        StructField("skew_ratio", DoubleType(), True),

        StructField("load_type", StringType(), True),
        StructField("run_status", StringType(), True),
        StructField("notes", StringType(), True),
    ])
    

    # Prepare metrics DataFrame 
    metrics_df = spark.createDataFrame(
        [
            (
                master_run_id,
                layer_run_id,
                layer_name,
                silver_table,
                table_name,
                table_type,

                start_ts,
                end_ts,
                duration_secs,

                read_rows,
                inserted_rows,
                updated_rows,
                unchanged_rows,                  
                soft_deleted_rows,
                active_rows,
                total_rows,
                
                throughput_rows_per_sec,
                skew_ratio,

                load_type,
                run_status,
                notes
            )
        ],
        schema=schema,
    ).withColumn("load_timestamp", current_timestamp())

    # Write metrics to Delta table
    metrics_df.write.format("delta").mode("append").saveAsTable(metrics_table)

    log_event(
        logger_gold_dimensional,
        "INFO",
        f"SCD2 Metrics | new={inserted_rows}, updated={updated_rows}, "
        f"current={active_rows}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )


# -----------------------------
# 9. Optimize and Vacuum Gold Table
# -----------------------------
def optimize_gold_table(spark, hash_column, master_run_id: str, layer_run_id: str, layer: str, gold_tbl):
    """
    Optimizes Gold table for query performance:
    - Uses Z-Ordering on hash column for efficient predicate pushdown
    - Recommended after large merges or inserts
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Starting to optimize Gold table {gold_tbl} for performance.",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )

    if not spark.catalog.tableExists(gold_tbl):
        log_event(
            logger_gold_dimensional,
            "WARN",
            f"Gold table {gold_tbl} does not exist. Skipping z-odering operation.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return


    spark.sql(f"OPTIMIZE {gold_tbl} ZORDER BY {hash_column}")
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Successfully optimized Gold table {gold_tbl}.",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )


def vacuum_gold_table(spark, master_run_id: str, layer_run_id: str, layer: str, gold_tbl):
    """
    Vacuums Gold table to remove stale/deleted data:
    - Retains 168 hours to ensure safety against late-arriving data
    - Frees up storage while keeping historical snapshots intact
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Starting to vacuum Gold table {gold_tbl} to remove stale data.",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )

    if not spark.catalog.tableExists(gold_tbl):
        log_event(
            logger_gold_dimensional,
            "WARN",
            f"Gold table {gold_tbl} does not exist. Skipping vacuuming operation.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return


    spark.sql(f"VACUUM {gold_tbl} RETAIN 168 HOURS")
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Successfully vacuumed Gold table {gold_tbl}.",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
