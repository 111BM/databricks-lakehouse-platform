"""
==============================================================
Module: Silver to Gold-Facts ETL Pipeline with Hash-Based Merge

Purpose:
    This module moves fact data from Silver to Gold tables in a Databricks Delta Lake environment.
    The pipeline performs the following operations:
    - Fetches incremental data from Silver, ensuring that only new records are processed.
    - Adds a row hash over the fact's columns so unchanged rows can be skipped.
    - Uses idempotent merge operations on the natural keys to insert or update records in the Gold table.
    - Collects detailed metrics on the pipeline's performance, including rows read, inserted, updated and unchanged.

    Facts are not SCD2: a fact row is updated in place when its hash changes. History is
    kept on the dimensions (see superstore_gold_dimension_framework).

Key Features:
1. Incremental Data Ingestion:
    - Loads only new or updated records from the Silver table based on `silver_ingestion_ts`.
    - Ensures that the Gold table is updated without reprocessing old records.

2. Merge Operations:
    - Idempotent Delta `MERGE` operations to update existing records or insert new records in the Gold table.
    - Ensures that updates only occur when a hash of the data has changed, avoiding unnecessary duplication.

3. Metrics Collection and Monitoring:
    - Tracks pipeline metrics such as rows read, inserted, updated and unchanged. Facts have no soft deletes.
    - Computes throughput and skew ratios for better observability of pipeline performance.

4. Table Maintenance:
    - OPTIMIZE and VACUUM are handled by Unity Catalog Predictive Optimization, which is
      enabled at the metastore level; this module runs no table maintenance of its own.

5. Serverless Compatibility:
    - Fully DataFrame-based pipeline; no cache()/persist(), which Serverless forbids.

Best Practices / Notes:
- Ensure business keys and hash columns are properly configured for correct merge operations and deduplication.
- The pipeline is designed to handle incremental loads; replay and backfill windows are set by the job's run_mode.
- The metrics collection helps monitor pipeline performance and troubleshoot issues in production environments.
- Ensure that schema evolution is handled carefully, especially when adding new fields or modifying existing ones.

==============================================================
"""
# -------------------------------
# Core Python + Utility Imports
# -------------------------------
# Standard libraries for logging, UUID generation, Spark session handling, and system operations
from uuid import uuid4  # Generates unique identifiers for pipeline run tracking

# PySpark core session and transformation functions
from pyspark.sql import SparkSession  # Entry point for Spark execution
from pyspark.sql.functions import (
    col,
    current_timestamp,
    max as spark_max,
    lit,
    spark_partition_id,
    coalesce,
    concat_ws,
    sha2,
    to_date  # For deriving ingestion_date in backfill mode
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
# Core Framework Imports
# -------------------------------
# Logging and event tracking utilities for observability
from superstore_logger import get_superstore_logger, log_event

# Platform constants (ensures consistent layer naming and governance rules)
from superstore_platform_constants import GOLD_LAYER
from superstore_backfill_utils import (                            # run-mode support
    get_incremental_with_backfill,
    run_mode_load_type
)


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
    backfill_config: dict,
    master_run_id: str, 
    layer_run_id: str, 
    layer: str, 
    ingestion_col: str = "silver_ingestion_ts",
    date_partition_col: str = "ingestion_date"
):
    """
    Returns only new silver rows not yet ingested into gold fact table.
    Enhanced with run-mode support - handles incremental, backfill, replay and
    full_refresh.

    Modes:
    - incremental:       Standard watermark-based processing (default)
    - backfill / replay: Reprocess the start_date..end_date window
    - full_refresh:      Reprocess all data

    Backfill and replay read identically here; they differ only at Bronze,
    which a replay skips entirely.
    
    Key Fix for Fact Tables:
    - Fact tables don't have ingestion_date column (unlike dimensions)
    - They only have bronze_ingestion_ts and silver_ingestion_ts
    - This function derives ingestion_date from bronze_ingestion_ts for windowed runs
    """
    
    log_event(
        logger_gold_facts,
        "INFO",
        f"Reading silver fact table with run mode: {backfill_config.get('mode', 'incremental')}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )
    
    # Windowed runs (backfill and replay) select by date rather than watermark.
    # This table has no ingestion_date column of its own, so it is derived below.
    is_windowed_run = (
        backfill_config.get("is_backfill") and
        backfill_config.get("is_windowed")
    )

    if is_windowed_run:
        # Read the silver table first to check columns
        df_silver = spark.table(silver_table)
        
        if "ingestion_date" not in df_silver.columns:
            log_event(
                logger_gold_facts,
                "INFO",
                f"Fact table missing ingestion_date column - deriving from bronze_ingestion_ts",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )
            
            # Derive ingestion_date from bronze_ingestion_ts
            df_silver = df_silver.withColumn("ingestion_date", to_date(col("bronze_ingestion_ts")))
            
            # Apply date range filter manually since we derived the column
            start_date = backfill_config.get("start_date")
            end_date = backfill_config.get("end_date")
            
            log_event(
                logger_gold_facts,
                "INFO",
                f"Filtering fact table by derived ingestion_date: {start_date} to {end_date}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )
            
            # Apply date filter
            df_filtered = df_silver.filter(
                (col("ingestion_date") >= lit(start_date)) & 
                (col("ingestion_date") <= lit(end_date))
            )
            
            return df_filtered
    
    # For incremental mode or full_refresh, use the standard utility
    # (it handles watermark logic and full table reads)
    return get_incremental_with_backfill(
        spark=spark,
        source_table=silver_table,
        target_table=gold_table,
        backfill_config=backfill_config,
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=layer,
        ingestion_col=ingestion_col,
        date_partition_col=date_partition_col
    )
# What just happened?
# ✅ Added backfill_config parameter to signature
# ✅ Added date_partition_col parameter to signature
# ✅ REMOVED all 80+ lines of manual watermark logic (checking max timestamp, filtering, etc.)
# ✅ REPLACED with single call to get_incremental_with_backfill() which handles all 3 modes



# -----------------------------
# 1. Read Silver Table
# -----------------------------
def read_silver_table(
    spark, silver_table: str, 
    gold_table: str, 
    backfill_config: dict,  # backfill parameter
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
        # Read the Delta table into a DataFrame incrementally (with backfill support)
        df = get_incremental_silver_for_facts(
            spark, 
            silver_table, 
            gold_table,
            backfill_config=backfill_config,  # backfill
            master_run_id=master_run_id, 
            layer_run_id=layer_run_id, 
            layer=GOLD_LAYER, 
            ingestion_col="silver_ingestion_ts",
            date_partition_col="ingestion_date"  # backfill
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
def prepare_fact_columns(df, master_run_id: str, entity_columns, hash_column:str, meta_columns, layer_run_id: str, layer: str):
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
    
    # Add gold load timestamp
    df = (
        df.withColumn(hash_column, sha2(concat_ws("||", *[coalesce(col(c), lit("")) for c in entity_columns]), 256))  # Generate SHA-256 hash for full row
        .withColumn("gold_ingestion_ts", current_timestamp())  # Add timestamp for tracking
    )

    # Select required columns
    cols_for_fact = entity_columns + [hash_column, *meta_columns, "gold_ingestion_ts"]

    df_prepared = df.select(*cols_for_fact)

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
    layer: str,
    backfill_config: dict = None
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
                load_type = run_mode_load_type(backfill_config, "INCREMENTAL")
                notes = f"No changes detected for {gold_table}"
                
            # CASE 3.3: Real UPSERT changes occurred
            else:
                load_type = run_mode_load_type(backfill_config, "INCREMENTAL")
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
