import math
import logging
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    lead,
    expr,
    sha2,
    concat_ws,
    coalesce,
    lit,
    current_timestamp,
    from_utc_timestamp,
    broadcast,
    hash,
    max as spark_max,
    spark_partition_id
)
from delta.tables import DeltaTable
# from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

import sys

sys.path.append(
    "/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/src/superstore_shared_utilities"
)
from superstore_logger import get_superstore_logger, log_event
from superstore_platform_constants import GOLD_LAYER

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
        silver_df = get_incremental_silver_for_dims(spark, silver_table, gold_table, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=GOLD_LAYER, ingestion_col="ingestion_ts")

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
    entity_columns,
    hash_column: str,
    master_run_id: str,
    layer_run_id: str,
    layer: str
):
    """
    Prepares columns for Slowly Changing Dimension Type 2:
    - Computes consistent hash for change detection
    - Sets effective_from and load timestamps
    - Excludes irrelevant columns from hash calculation
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
        scd_df = (
            silver_df.select(*entity_columns)
            .withColumn(
                "gold_load_ts",
                current_timestamp()
            )
            .withColumn("effective_from", col("ingestion_ts"))
            .withColumn(
                # "gold_entity_hash_id",
                hash_column,
                sha2(
                    concat_ws(
                        "||",
                        *[
                            coalesce(col(col_name), lit(""))
                            for col_name in entity_columns
                            if col_name
                            not in [
                                "ingestion_ts",
                                "source_file_path",
                                "source_file_name",
                            ]
                        ],
                    ),
                    256,
                ),
            )
        )
        log_event(
            logger_gold_dimensional,
            "INFO",
            "Successfully prepared SCD2 columns.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
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
                        - expr("INTERVAL 1 SECOND")
            )
            .withColumn("is_current", col("next_effective_from").isNull())
            .drop("next_effective_from")
            .dropDuplicates([entity_id_column, hash_column])
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
    run_id=None,
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
    from delta.tables import DeltaTable
    from pyspark.sql.functions import col, expr, hash as hash_fn
    import math

    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Starting SCD2 merge into Gold table: {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )

    if repaired_df.limit(1).count() == 0:
        log_event(
            logger_gold_dimensional,
            "INFO",
            "No records to merge. Skipping SCD2 merge.",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        return

    try:
        gold_delta = DeltaTable.forName(spark, gold_tbl)
        gold_cols = [c.name for c in gold_delta.toDF().schema.fields]

        # Determine actual hash column in Gold for change detection
        if hash_column not in gold_cols:
            matches = [c for c in gold_cols if "hash" in c.lower()]
            if matches:
                log_event(
                    logger_gold_dimensional,
                    "WARNING",
                    f"Hash column '{hash_column}' not found in Gold table. Using '{matches[0]}' instead.",
                    master_run_id=master_run_id,
                    layer_run_id=layer_run_id,
                    layer=GOLD_LAYER
                )
                tgt_hash_column = matches[0]
            else:
                raise ValueError(
                    f"Hash column '{hash_column}' not found and no hash-like column in Gold table",
                    master_run_id=master_run_id,
                    layer_run_id=layer_run_id,
                    layer=GOLD_LAYER
                )
        else:
            tgt_hash_column = hash_column

        # Calculate number of buckets for safe merge
        rows_read = repaired_df.count()
        num_buckets = max(1, math.ceil(rows_read / max_rows_per_bucket))
        log_event(
            logger_gold_dimensional,
            "INFO",
            f"Total records: {rows_read}, splitting into {num_buckets} bucket(s)",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

        repaired_df_buckets = repaired_df.withColumn(
            "_bucket", hash_fn(col(entity_id_column)) % num_buckets
        )

        for b in range(num_buckets):
            log_event(
                logger_gold_dimensional,
                "INFO",
                f"Processing bucket {b+1}/{num_buckets}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )
            bucket_df = repaired_df_buckets.filter(col("_bucket") == b).drop("_bucket")

            if bucket_df.limit(1).count() == 0:
                log_event(
                    logger_gold_dimensional,
                    "INFO",
                    f"Bucket {b+1} is empty. Skipping.",
                    master_run_id=master_run_id,
                    layer_run_id=layer_run_id,
                    layer=GOLD_LAYER
                )
                continue

            merge_condition = f"""
                tgt.{entity_id_column} = src.{entity_id_column}
                AND (tgt.is_current = true OR tgt.effective_to >= src.effective_from)
            """
            # merge_condition = f"""
            # #     tgt.{entity_id_column} = src.{entity_id_column}
            # #     AND (tgt.is_current = true)
            # # """

            insert_values = {str(c): f"src.{c}" for c in bucket_df.columns}
            insert_values["effective_to"] = "cast(null as timestamp)"
            insert_values["is_current"] = "true"

            try:
                gold_delta.alias("tgt").merge(
                    bucket_df.alias("src"), merge_condition
                ).whenMatchedUpdate(
                    condition=f"NOT (tgt.{tgt_hash_column} <=> src.{hash_column})",
                    set={
                        "effective_to": expr("src.effective_from - INTERVAL 1 SECOND"),
                        "is_current": "false",
                    },
                ).whenNotMatchedInsert(
                    values=insert_values
                ).execute()
                log_event(
                    logger_gold_dimensional,
                    "INFO",
                    f"Bucket {b+1} merged successfully",
                    master_run_id=master_run_id,
                    layer_run_id=layer_run_id,
                    layer=GOLD_LAYER
                )
            except Exception as e:
                log_event(
                    logger_gold_dimensional,
                    "ERROR",
                    f"Bucket {b+1} merge failed: {e}",
                    master_run_id=master_run_id,
                    layer_run_id=layer_run_id,
                    layer=GOLD_LAYER
                )
                raise

        log_event(
            logger_gold_dimensional,
            "INFO",
            f"SCD2 merge completed successfully for Gold table {gold_tbl})",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

    except Exception as e:
        log_event(
            logger_gold_dimensional,
            "ERROR",
            f"SCD2 merge failed for Gold table {gold_tbl}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )
        raise

def handle_soft_deletes(
    spark, master_run_id, layer_run_id, layer: str, silver_df, gold_tbl, entity_id_column
):
    gold_delta = DeltaTable.forName(spark, gold_tbl)
    
    # Only distinct IDs from Silver
    silver_ids = silver_df.select(entity_id_column).distinct()
    
    # Current Gold rows not in Silver
    gold_current = gold_delta.toDF().filter(col("is_current") == True)
    soft_delete_ids = gold_current.join(
        broadcast(silver_ids), on=entity_id_column, how="left_anti"
    ).select(entity_id_column).distinct()
    
    # Re-join to get full row for merge
    soft_delete_df = soft_delete_ids.join(gold_current, on=entity_id_column, how="inner")
    
    if soft_delete_df.count() > 0:
        soft_delete_df = soft_delete_df.repartition(200)
        gold_delta.alias("tgt").merge(
            soft_delete_df.alias("src"),
            f"tgt.{entity_id_column} = src.{entity_id_column}"
        ).whenMatchedUpdate(
            set={
                "is_current": "false",
                "effective_to": current_timestamp()
            }
        ).execute()

# -----------------------------
# 8.  Metrics for Observability to metrics table
# -----------------------------
def collect_metrics(
    spark,
    repaired_df,
    gold_df,
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
    run_status: str,
    max_rows_per_bucket: int,
    notes: str = ""
    
):
    """
    Collects metrics on SCD2 processing for auditing and monitoring:
    - Counts new, updated, and current records
    - Uses bucketed approach for large datasets to avoid driver overload
    - Writes results to dedicated metrics dashboard table
    - Supports run_id for pipeline observability
    """
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Collecting SCD2 metrics for {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=GOLD_LAYER
    )

    # ------------------------------------------------------------------
    # Detect missing or empty Silver table
    # ------------------------------------------------------------------
    if len(repaired_df.columns) == 0:
        log_event(
            logger_gold_dimensional,
            "WARN",
            f"No table found in Silver for {silver_table}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=GOLD_LAYER
        )

        # Initialize default metrics
        rows_read = 0
        rows_inserted = 0
        rows_updated = 0
        rows_unchanged = 0
        rows_soft_deleted = 0
        rows_current = gold_df.filter(col("is_current") == True).count() if gold_df is not None else 0
        throughput_rows_per_sec = 0.0
        skew_ratio = 0.0
        notes = f"Source silver table '{silver_table}' does not exist"
        has_rows = False

    else:
        # Load gold table if exists
        gold_df = spark.table(gold_tbl) if spark.catalog.tableExists(gold_tbl) else None

        # Check if repaired_df has rows
        has_rows = repaired_df.limit(1).count() > 0

        # -----------------------------
        # Compute metrics safely
        # -----------------------------
        rows_read = repaired_df.count() if has_rows else 0
        rows_inserted = 0
        rows_updated = 0
        rows_unchanged = 0

        if has_rows and gold_df is not None:
            # Split into buckets to avoid driver overload
            num_buckets = max(1, math.ceil(rows_read / max_rows_per_bucket))
            repaired_df_buckets = repaired_df.withColumn("_bucket", (hash(col(entity_id_column)) % num_buckets))

            for b in range(num_buckets):
                bucket_df = repaired_df_buckets.filter(col("_bucket") == b).drop("_bucket")
                if bucket_df.limit(1).count() == 0:
                    continue

                # Inserted rows: new in repaired_df
                inserted = bucket_df.join(
                    gold_df.select(entity_id_column),
                    on=entity_id_column,
                    how="left_anti"
                ).count()
                rows_inserted += inserted

                # Current matching rows
                matching_current = bucket_df.alias("src").join(
                    gold_df.alias("tgt"),
                    on=entity_id_column,
                    how="inner"
                )
                # Updated: hash differs
                updated = matching_current.filter(
                    col(f"src.{silver_hash_column}") != col(f"tgt.{gold_hash_column}")
                ).count()
                rows_updated += updated

                # Unchanged: hash same
                unchanged = matching_current.filter(
                    col(f"src.{silver_hash_column}") == col(f"tgt.{gold_hash_column}")
                ).count()
                rows_unchanged += unchanged

        # Current rows in Gold
        rows_current = gold_df.filter(col("is_current") == True).count() if gold_df is not None else 0

        # Soft-deleted rows: current in Gold but missing in repaired_df
        rows_soft_deleted = 0
        if gold_df is not None and has_rows:
            rows_soft_deleted = gold_df.filter(col("is_current") == True).join(
                repaired_df.select(entity_id_column).distinct(),
                on=entity_id_column,
                how="left_anti"
            ).count()

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
            repaired_df.withColumn("partition_id", spark_partition_id())
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

        throughput_rows_per_sec DOUBLE,
        skew_ratio DOUBLE,

        rows_read BIGINT,
        rows_inserted BIGINT,
        rows_updated BIGINT,
        rows_unchanged BIGINT,
        rows_soft_deleted  BIGINT,
        rows_current BIGINT,

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

        StructField("throughput_rows_per_sec", DoubleType(), True),
        StructField("skew_ratio", DoubleType(), True),

        StructField("rows_read", LongType(), True),
        StructField("rows_inserted", LongType(), True),
        StructField("rows_updated", LongType(), True),
        StructField("rows_unchanged", LongType(), True),
        StructField("rows_soft_deleted", LongType(), True),
        StructField("rows_current", LongType(), True),

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

                throughput_rows_per_sec,
                skew_ratio,

                rows_read,
                rows_inserted,
                rows_updated,
                rows_unchanged,                  #row_unchanged is null for dimensions
                rows_soft_deleted,
                rows_current,
                
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
        f"SCD2 Metrics | new={rows_inserted}, updated={rows_updated}, "
        f"current={rows_current}",
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
