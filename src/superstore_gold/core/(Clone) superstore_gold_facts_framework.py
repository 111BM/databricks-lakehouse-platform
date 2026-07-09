"""
==============================================================
Module: Silver to Gold-Facts ETL Pipeline with SCD2, Merge Operations & Performance Optimization

Purpose:
    This module orchestrates the ETL process to move data from Silver to Gold tables in a Databricks Delta Lake environment. 
    The pipeline performs the following operations:
    - Fetches incremental data from Silver, ensuring that only new records are processed.
    - Prepares data for Slowly Changing Dimension Type 2 (SCD2) processing, including adding `effective_from`, `effective_to`, 
      and `is_current` fields to track historical changes.
    - Uses idempotent merge operations to safely insert or update records in the Gold table, ensuring data integrity.
    - Collects detailed metrics on the pipeline's performance, including the number of rows processed, inserted, updated, and deleted.
    - Optimizes the Gold table using Z-Ordering and performs VACUUM operations to maintain storage efficiency and query performance.

Key Features:
1. Incremental Data Ingestion:
    - Loads only new or updated records from the Silver table based on `silver_ingestion_ts`.
    - Ensures that the Gold table is updated without reprocessing old records.

2. SCD2 (Slowly Changing Dimension Type 2) Processing:
    - Tracks historical changes in records by adding `effective_from` and `effective_to` timestamps.
    - Identifies current records with the `is_current` flag to allow for incremental and historical analysis.

3. Merge Operations:
    - Idempotent Delta `MERGE` operations to update existing records or insert new records in the Gold table.
    - Ensures that updates only occur when a hash of the data has changed, avoiding unnecessary duplication.

4. Metrics Collection and Monitoring:
    - Tracks pipeline metrics such as rows read, inserted, updated, unchanged, and soft-deleted.
    - Computes throughput and skew ratios for better observability of pipeline performance.

5. Gold Table Optimization:
    - Z-Orders the Gold table based on key columns to improve query performance and partition pruning.
    - Performs a VACUUM operation to clean up old data, retaining the specified number of hours of data for safety.

6. Serverless Compatibility & Performance:
    - Fully DataFrame-based pipeline optimized for Databricks serverless clusters.
    - Uses dynamic partitioning and bucketing strategies to handle large datasets efficiently.

Best Practices / Notes:
- Ensure business keys and hash columns are properly configured for correct merge operations and deduplication.
- The `effective_from` and `effective_to` columns in the Gold table must be carefully managed to maintain data history.
- Z-Ordering is essential for efficient querying, particularly after large inserts or updates in the Gold table.
- The pipeline is designed to handle incremental loads, but if necessary, full loads can be performed when required.
- The metrics collection helps monitor pipeline performance and troubleshoot issues in production environments.
- Ensure that schema evolution is handled carefully, especially when adding new fields or modifying existing ones.

==============================================================
"""
# -------------------------------
# Core Python + Utility Imports
# -------------------------------
# Standard libraries for logging, UUID generation, Spark session handling, and system operations
import logging  # Python logging framework for structured logs
from uuid import uuid4  # Generates unique identifiers for pipeline run tracking
import sys  # Enables path manipulation for shared code imports

# PySpark core session and transformation functions
from pyspark.sql import SparkSession  # Entry point for Spark execution
from pyspark.sql.functions import (
    col,
    current_timestamp,
    max as spark_max,
    lit,
    spark_partition_id
)

# Delta Lake support for ACID operations (MERGE / UPDATE / DELETE)
from delta.tables import DeltaTable

# Spark SQL data types used for schema definitions in metrics and validation tables
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    IntegerType,
    DoubleType
)


# -------------------------------
# Shared Utilities Path
# -------------------------------
# Adds reusable platform utilities (logging, config, and orchestration helpers)
sys.path.append(
    "/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/src/superstore_shared_utilities"
)


# -------------------------------
# Core Framework Imports
# -------------------------------
# Logging and event tracking utilities for observability
from superstore_logger import get_superstore_logger, log_event

# Platform constants (ensures consistent layer naming and governance rules)
from superstore_platform_constants import GOLD_LAYER


# -------------------------------
# Logger Initialization (Gold Facts Framework)
# -------------------------------
# Creates a dedicated logger for Gold facts layer processing
# Used for tracking MERGE, SCD2, and metrics-related events
logger_gold_facts = get_superstore_logger("superstore_gold_facts_framework")

def get_incremental_silver_for_facts(
    spark,
    silver_table: str,
    gold_table: str,
    master_run_id: str,
    layer_run_id: str,
    layer: str,
    ingestion_col: str = "silver_ingestion_ts",
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
            layer=GOLD_LAYER,
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
        ingestion_col=ingestion_col,
    )

    # -----------------------------
    # Get max ingestion from Gold
    # -----------------------------
    max_ingestion_ts = None
    if spark.catalog.tableExists(gold_table):
        max_ingestion_ts_row = spark.table(gold_table).agg(spark_max(ingestion_col).alias("max_ingest_ts")).first()
        max_ingestion_ts = max_ingestion_ts_row["max_ingest_ts"]
        log_event(
            logger_gold_facts,
            "INFO",
            f"Max ingestion timestamp found in gold table '{gold_table}': {max_ingestion_ts}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
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
            layer=GOLD_LAYER,
        )
    row_count = incremental_df.count()
    log_event(
        logger_gold_facts,
        "INFO",
        f"Incremental silver rows to process: {row_count}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
    )

    return incremental_df


# -----------------------------
# 1. Read Silver Table
# -----------------------------
def read_silver_table(
    spark, silver_table: str, gold_table: str, required_columns, master_run_id: str, layer_run_id: str, layer: str
):
    """
    Reads data from the Silver fact table, validates the required columns are present,
    and logs success or failure for transparency in pipeline execution.

    Args:
        spark (SparkSession): Active Spark session.
        gold_table (str): Name of the Silver table to read from.
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
        layer=GOLD_LAYER,
    )

    try:
        # Read the Delta table into a DataFrame incrementally
        df = get_incremental_silver_for_facts(
            spark,
            silver_table,
            gold_table,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
            ingestion_col="silver_ingestion_ts",
        )

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
                layer=GOLD_LAYER,
            )

            return df  # Skip column validation completely

        # Check if dataframe is empty (table missing scenario)
        if len(df.columns) == 0:
            log_event(
                logger_gold_facts,
                "WARNING",
                f"Silver table '{silver_table}' does not exist or returned empty schema. Skipping processing.",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER,
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
            raise ValueError(f"Missing required columns in Silver table '{silver_table}': {missing_required}")

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
            layer=GOLD_LAYER,
        )
        return df
    except Exception as e:
        log_event(
            logger_gold_facts,
            "ERROR",
            f"Failed to read Silver table {silver_table}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
        )  # Log error if loading fails
        raise


# -----------------------------
# 2. Prepare Fact Table Columns
# -----------------------------
def prepare_fact_columns(df, master_run_id: str, entity_columns, layer_run_id: str, layer: str):
    """
    Prepares the fact table by adding a 'gold_ingestion_ts' timestamp column for auditing and idempotency.

    Args:
        df (DataFrame): Silver fact table DataFrame to be transformed.
        log_event (logger_gold_facts): log_event instance to track the process.

    Returns:
        DataFrame: Transformed DataFrame with added 'gold_ingestion_ts' column.
    """
    log_event(
        logger_gold_facts,
        "INFO",
        "Preparing fact table columns for Gold",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
    )
    df_prepared = df.select(*entity_columns)\
        .withColumn("gold_ingestion_ts", current_timestamp())\
        # .dropDuplicates([entity_id_column, hash_column])
    log_event(
        logger_gold_facts,
        "INFO",
        "Fact table columns prepared successfully",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
    )
    return df_prepared


# -----------------------------
# 3. Create Gold Table if Not Exists
# -----------------------------
def create_gold_table_if_not_exists(df, gold_tbl, master_run_id: str, layer_run_id: str, layer: str):
    """
    Checks if the Gold fact table exists. If not, creates an empty Delta Gold table to ensure
    the merge operation works smoothly later in the pipeline.

    Args:
        df (DataFrame): DataFrame that will be merged into Gold.
        gold_tbl (str): Name of the Gold table.
        log_event (logger_gold_facts): log_event instance to track the process.
    """
    log_event(
        logger_gold_facts,
        "INFO",
        f"Checking if Gold table {gold_tbl} exists",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
    )
    if not df.sparkSession.catalog.tableExists(gold_tbl):  # Check if the Gold table exists in the catalog
        df.limit(0).write.format("delta").mode("ignore").saveAsTable(gold_tbl)  # Create an empty table if not exists
        log_event(
            logger_gold_facts,
            "INFO",
            f"Gold table {gold_tbl} created",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
        )
    else:
        log_event(
            logger_gold_facts,
            "INFO",
            f"Gold table {gold_tbl} already exists",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
        )


# -----------------------------
# 4. Merge Fact Table into Gold
# -----------------------------
def merge_fact_into_gold(df, gold_tbl, natural_keys, hash_column, master_run_id: str, layer_run_id: str, layer: str):
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
    try:
        log_event(
            logger_gold_facts,
            "INFO",
            f"Starting merge into Gold table {gold_tbl}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
        )
        
        # Get Delta table
        gold_delta = DeltaTable.forName(df.sparkSession, gold_tbl)

        # Build merge condition
        merge_condition = " AND ".join([
            f"tgt.{k} = src.{k}" for k in natural_keys
        ])

        # Execute MERGE
        gold_delta.alias("tgt").merge(
            df.alias("src"),
            merge_condition
        ).whenMatchedUpdate(
            # Only update if hash values differ
            condition=f"tgt.{hash_column} <> src.{hash_column}",
            # Set updated values from the source DataFrame
            set={c: f"src.{c}" for c in df.columns},
            # Insert new records if they don't match
        ).whenNotMatchedInsertAll().execute()

        log_event(
            logger_gold_facts,
            "INFO",
            f"Merge completed for Gold table {gold_tbl}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
        )
        
        # --- METRICS: read immediately after merge ---
        hist = gold_delta.history(1).select("operationMetrics").collect()[0][0]

        # inserted row fetch from history include actual new and new updated version of existing rows 
        new_versions_inserted = int(hist.get("numTargetRowsInserted", 0))

        #updated rows only 
        updated_records = int(hist.get("numTargetRowsUpdated", 0))
        
        # To compute actual new insterted rows 
        inserted= new_versions_inserted-updated_records

        merge_metrics = {
            "inserted": inserted,
            "updated": updated_records
        }

    except Exception as e:
        log_event(
            logger_gold_facts,
            "ERROR",
            f"Merge failed for Gold table {gold_tbl}: {str(e)}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
        )
        raise

    return {
        "merge_metrics": merge_metrics,
    }

# -----------------------------
# 5. Collect Fact Metrics
# -----------------------------
def collect_fact_metrics(
    spark,
    df,
    metrics_table,
    merge_metrics, 
    is_initial_load,
    layer_name,
    gold_table: str,
    silver_table: str,
    table_type: str,
    start_ts,
    end_ts,
    duration_secs,
    master_run_id: str,
    layer_run_id: str,
    layer: str
):
    """
    Collects operational observability metrics for fact pipelines, ensuring transparency into
    the performance of each pipeline run.

    Captures the following metrics:
    - read_rows(from Silver table)
    - inserted_rows (from Delta MERGE operation)
    - updated_rows (from Delta MERGE operation)
    - unchanged_rows (calculated as remaining rows)

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
        f"Collecting fact metrics for {gold_table}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
    )
    # -----------------------------
    # DEFAULTS
    # -----------------------------
    read_rows = 0
    inserted_rows = 0
    updated_rows = 0
    unchanged_rows = 0

    active_rows = 0
    total_rows = 0

    throughput_rows_per_sec = 0.0
    skew_ratio = 0.0
    duration_secs=duration_secs or 0

    merge_metrics = merge_metrics or {}
    soft_deleted_rows = 0
    is_initial_load = bool(is_initial_load)

    run_status = "SUCCESS"
    load_type = None
    notes = None

    
    # -----------------------------
    # CASE 1: Silver missing
    # -----------------------------
    if df is None:
        load_type = "NO_DATA"
        run_status = "SKIPPED"
        notes = f"Source silver table {silver_table} does not exist" 
        has_rows = False

    # -----------------------------
    # CASE 2: Silver empty
    # -----------------------------
    elif df.limit(1).count() == 0:
        load_type = "INCREMENTAL"
        run_status = "SKIPPED"
        notes = f"No incremental rows in silver table {silver_table}" 
        has_rows = False
        
    # --------------------------------------
    # CASE 3: Normal processing/Data present
    # --------------------------------------
    else:
        has_rows = True
        # -----------------------------
        # If rows exist, get Delta MERGE metrics
        # -----------------------------
        try:
            read_rows= df.count()  # Only count if there are rows

            # -----------------------------
            # Read input rows from Silver
            # -----------------------------
            read_rows = df.count()

            # -----------------------------
            # Extract MERGE metrics
            # -----------------------------
            inserted_rows = merge_metrics.get("inserted", 0)
            updated_rows = merge_metrics.get("updated", 0)

            # -----------------------------
            # DERIVED METRICS
            # unchanged_rows = rows that came from Silver but did not result in insert/update
            # -----------------------------
            unchanged_rows = max(read_rows - (inserted_rows + updated_rows), 0)

            # -----------------------------
            # CHANGE DETECTION
            # Fact tables do NOT have soft deletes (unlike dimensions)
            # -----------------------------
            has_changes = (inserted_rows + updated_rows) > 0
        
            # -----------------------------
            # Status + Execution Mode
            # -----------------------------
            # INITIAL_LOAD  → first time full population
            # NO_CHANGE     → data arrived but no changes detected
            # UPSERT_APPLIED  → inserts/updates applied
            # ---------------------------------------------------

           
            # CASE 3.1: INITIAL LOAD
            # First-time load into Gold table
            
            if is_initial_load:
                load_type = "INITIAL_LOAD"
                notes = f"Initial load completed for {gold_table}"
            
            # CASE 3.2: NO_CHANGE
            elif  not has_changes:
                load_type = "INCREMENTAL"
                notes = f"No changes detected for {gold_table}"
                
            # CASE 3.3: Real UPSERT changes occurred
            else:
                load_type = "INCREMENTAL"
                notes = f"UPSERT changes applied to {gold_table}"

            run_status = "SUCCESS"
                    
            # -----------------------------
            # Compute active and total rows
            # -----------------------------
            try:
                active_rows = spark.table(gold_table).count()
                total_rows = active_rows
            except Exception:
                active_rows = 0
                total_rows = 0
                    
        except Exception as e:
            log_event(
                logger_gold_facts,
                "WARN",
                f"Unable to fetch Delta MERGE metrics for {gold_table}: {e}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER,
            )

        # -----------------------------
        # Ensure defaults for timestamps & notes
        # -----------------------------
        start_ts = start_ts or spark.sql("SELECT current_timestamp() as ts").first()["ts"]
        end_ts = end_ts or spark.sql("SELECT current_timestamp() as ts").first()["ts"]
        duration_secs = duration_secs or 0
        notes = notes or ("No rows/files processed" if read_rows== 0 else "")

        # -----------------------------
        # Compute throughput and skew
        # -----------------------------
        throughput_rows_per_sec = round(read_rows/ duration_secs, 2) if duration_secs > 0 else 0.0

        if has_rows:
            partition_counts = df.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().collect()

            counts = [r["count"] for r in partition_counts]

            max_partition_rows = max(counts) if counts else 0
            avg_partition_rows = sum(counts) / len(counts) if counts else 1

            skew_ratio = round(max_partition_rows / avg_partition_rows, 2) if avg_partition_rows > 0 else 0.0
        else:
            skew_ratio = 0.0

    # ------------------------------------------------------------------
    # 1. Create table if not exists (enterprise-grade template pattern)
    # ------------------------------------------------------------------
    spark.sql(
        f"""
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

        read_rows BIGINT,
        inserted_rows BIGINT,
        updated_rows BIGINT,
        unchanged_rows BIGINT,
        soft_deleted_rows BIGINT,
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
    """
    )

    # ------------------------------------------------------------------
    # 2. Explicit schema definition for DataFrame (avoids inference issues)
    # ------------------------------------------------------------------
    schema = StructType(
        [
            StructField("master_run_id", StringType(), False),
            StructField("layer_run_id", StringType(), False),
            StructField("layer_name", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("target_table", StringType(), False),
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
            StructField("notes", StringType(), True)
        ]
    )

    # ------------------------------------------------------------------
    # 3. Create a single-row DataFrame representing this execution event
    # ------------------------------------------------------------------
    metrics_df = spark.createDataFrame(
        [
            (
                master_run_id,
                layer_run_id,
                layer_name,
                silver_table,
                gold_table,
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

    # Append metrics to dedicated dashboard table
    metrics_df.write.format("delta").mode("append").saveAsTable(metrics_table)

    log_event(
        logger_gold_facts,
        "INFO",
        f"Fact metrics recorded | " f"read={read_rows}, inserted={inserted_rows}, updated={updated_rows}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
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
        f"Optimizing {gold_tbl} using ZORDER BY {z_order_cols}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
    )

    if not spark.catalog.tableExists(gold_tbl):
        log_event(
            logger_gold_facts,
            "WARN",
            f"Gold table {gold_tbl} does not exist. Skipping z-odering operation.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
        )
        return
    spark.sql(f"OPTIMIZE {gold_tbl} ZORDER BY ({','.join(z_order_cols)})")  # Perform Z-Ordering for optimization
    log_event(
        logger_gold_facts,
        "INFO",
        f"Optimization completed for {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
    )


# -----------------------------
# 7. Vacuum Gold Table
# -----------------------------
def vacuum_gold_table(spark, gold_tbl, master_run_id: str, layer_run_id: str, layer: str, retention_hours=168):
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
        layer=GOLD_LAYER,
    )

    if not spark.catalog.tableExists(gold_tbl):
        log_event(
            logger_gold_facts,
            "WARN",
            f"Gold table {gold_tbl} does not exist. Skipping vacuuming operation.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER,
        )
        return

    spark.sql(f"VACUUM {gold_tbl} RETAIN {retention_hours} HOURS")  # Clean up old files from Delta table

    log_event(
        logger_gold_facts,
        "INFO",
        f"Vacuum completed for {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER,
    )
