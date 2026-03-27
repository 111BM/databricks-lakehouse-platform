# ==============================================================
# Module: Bronze Entity Split – Superstore append
#
# Author: Senior Data Engineering Team
# Date: 2026-02-20
# Version: 1.0.0
#
# Purpose:
#     Incrementally splits Bronze raw data into multiple entity tables
#     using append logic. Handles new and updated records while maintaining
#     idempotency and optimized partitioning.
#
# Key Features:
# 1. Incremental append Logic:
#     - Inserts new rows and updates existing rows based on configured business keys.
#     - Ensures no duplicate data across repeated runs.
#
# 2. Partitioning & Optional Z-Ordering:
#     - Entity tables partitioned by `ingestion_date`.
#     - Optional Z-Order for selective query acceleration on key lookup columns.
#
# 3. Observability:
#     - Structured JSON logging with `run_id`, timestamps, table names, and row counts.
#     - Logs module-level technical details; orchestrator logs high-level pipeline progress.
#
# 4. Error Handling & Resilience:
#     - Safe to rerun without affecting existing data.
#     - Logs warnings for Z-Order failures or table creation issues without stopping execution.
#
# 5. Configuration-Driven:
#     - Entity table definitions, business keys, column selections, and Z-Order columns
#       are centralized for consistent and repeatable execution.
#
# Best Practices / Notes:
# - Avoid `.count()` on massive tables; use async metrics where possible.
# - Optimize and Z-Order large entity tables during off-peak hours.
# - Ensure partition columns exist and are consistent across pipelines.
# - Module is designed for serverless Databricks compute with minimal resource usage.
# ==============================================================

import sys
sys.path.append("/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/src/superstore_shared_utilities")

import yaml

# Load YAML config
with open(
    "/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/configs/superstore_bronze_config/superstore_bronze_config.yaml",
    "r"
) as f:
    bronze_config = yaml.safe_load(f)

from superstore_logger import get_superstore_logger, log_event
from superstore_platform_constants import BRONZE_LAYER
# from superstore_bronze_config import bronze_entities
from superstore_platform_config import get_catalog, get_bronze_schema, get_metrics_schema, table
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType
)
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp,sum, avg, min, max
import time       # used for ETL timing

# -----------------------------
# Logger Setup for Bronze Ingestion
# -----------------------------
# Initialize logger to capture events in the Bronze entity split pipeline
logger_bronze_entity = get_superstore_logger("bronze_entity_superstore_module_02")

def collect_entity_metrics(
    spark,
    entity_df,
    metrics_table: str,
    master_run_id: str,
    layer_run_id: str,
    layer_name: str,
    source_table: str,
    target_table: str,
    entity_name: str,
    start_ts: str,
    end_ts: str,
    duration_secs: int,
    run_status: str,
    notes: str = ""
):
    """
    Collects metrics specific to each entity processed in the Bronze layer.
    Tracks volume, file info, ingestion timestamps, and auditability metadata.

    Arguments:
        spark: Spark session object.
        entity_df: DataFrame of the current entity.
        metrics_table: Target Delta table for storing the entity metrics.
        master_run_id: Identifier for the master run.
        layer_run_id: Identifier for the current layer run.
        layer_name: Name of the current layer.
        entity_name: Name of the current entity (e.g., "customers").
        start_ts: Timestamp when the batch started.
        end_ts: Timestamp when the batch finished.
        duration_secs: Duration of the batch run in seconds.
        run_status: Status of the run (e.g., "SUCCESS", "FAILED").
        notes: Optional notes or additional info.

    Returns:
        None: Writes metrics directly to the metrics table.
    """
    log_event(
        logger_bronze_entity,
        "INFO",
        f"Collecting metrics for {entity_name}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=BRONZE_LAYER
    )

    # -----------------------------
    # Safe check if df has any rows
    # -----------------------------
    try:
        has_rows = entity_df.limit(1).count() > 0
    except Exception as e:
        log_event(
            logger_bronze_entity,
            "ERROR",
            f"Failed to check rows for {entity_name}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )
        raise

    # -----------------------------
    # Initialize defaults
    # -----------------------------
    rows_read = 0
    rows_written = 0
    files_processed = 0
    total_file_size_bytes = 0
    max_file_size_bytes = 0
    file_skew_ratio = 0.0
    avg_file_size_bytes = 0
    ingestion_ts_min = None
    ingestion_ts_max = None
    earliest_file_mod_time = None
    latest_file_mod_time = None
    throughput_rows_per_sec = 0.0
    notes = "No rows/files processed"

    # -----------------------------
    # Calculate entity metrics if rows exist
    # -----------------------------
    if has_rows:
        try:
            # Get a file level dataframe
            file_df = entity_df.select(
                "source_file_name",
                "source_file_size_bytes",
                "source_file_modification_time",
                "ingestion_ts"
            ).dropDuplicates(["source_file_name"])

            entity_file_metrics = file_df.agg(
                sum("source_file_size_bytes").alias("total_file_size_bytes"),
                max("source_file_size_bytes").alias("max_file_size_bytes"),
                avg("source_file_size_bytes").alias("avg_file_size_bytes"),
                min("source_file_modification_time").alias("earliest_file_mod_time"),
                max("source_file_modification_time").alias("latest_file_mod_time"),
                min("ingestion_ts").alias("ingestion_ts_min"),
                max("ingestion_ts").alias("ingestion_ts_max"),
            ).collect()[0]

            rows_read = entity_df.count()
            rows_written = rows_read
            files_processed = entity_df.select("source_file_name").distinct().count()

            total_file_size_bytes = entity_file_metrics["total_file_size_bytes"]
            max_file_size_bytes = entity_file_metrics["max_file_size_bytes"]
            avg_file_size_bytes = entity_file_metrics["avg_file_size_bytes"]
            file_skew_ratio = round(max_file_size_bytes / (avg_file_size_bytes or 1),2)
            ingestion_ts_min = entity_file_metrics["ingestion_ts_min"]
            ingestion_ts_max = entity_file_metrics["ingestion_ts_max"]
            earliest_file_mod_time = entity_file_metrics["earliest_file_mod_time"]
            latest_file_mod_time = entity_file_metrics["latest_file_mod_time"]

            throughput_rows_per_sec = round(rows_read / duration_secs,2) if duration_secs > 0 else 0
            notes = ""  # Clear default note if rows exist

        except Exception as e:
            log_event(
                logger_bronze_entity,
                "ERROR",
                f"Failed to calculate metrics for {entity_name}: {e}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=BRONZE_LAYER
            )
            raise

            
    # -----------------------------
    # Ensure metrics table exists
    # -----------------------------
    try:
        if not spark.catalog.tableExists(metrics_table):
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {metrics_table} (
                    master_run_id STRING,
                    layer_run_id STRING,
                    layer_name STRING,
                    source_table STRING,
                    entity_name STRING,
                    target_table STRING,
                    start_ts TIMESTAMP,
                    end_ts TIMESTAMP,
                    duration_secs BIGINT,
                    files_processed BIGINT,
                    rows_read BIGINT,
                    rows_written BIGINT,
                    total_file_size_bytes BIGINT,
                    avg_file_size_bytes BIGINT,
                    max_file_size_bytes BIGINT,
                    file_skew_ratio DOUBLE,
                    ingestion_ts_min TIMESTAMP,
                    ingestion_ts_max TIMESTAMP,
                    earliest_file_mod_time TIMESTAMP,
                    latest_file_mod_time TIMESTAMP,
                    throughput_rows_per_sec DOUBLE,
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
    except Exception as e:
        log_event(
            logger_bronze_entity,
            "ERROR",
            f"Failed to create metrics table {metrics_table}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )
        raise


    # -----------------------------
    # Prepare Metrics Row
    # -----------------------------
    try:
        schema = StructType(
            [
                StructField("master_run_id", StringType(), True),
                StructField("layer_run_id", StringType(), True),
                StructField("layer_name", StringType(), True),
                StructField("source_table", StringType(), True),
                StructField("entity_name", StringType(), True),
                StructField("target_table", StringType(), True),
                StructField("start_ts", TimestampType(), True),
                StructField("end_ts", TimestampType(), True),
                StructField("duration_secs", LongType(), True),
                StructField("files_processed", LongType(), True),
                StructField("rows_read", LongType(), True),
                StructField("rows_written", LongType(), True),
                StructField("total_file_size_bytes", LongType(), True),
                StructField("avg_file_size_bytes", LongType(), True),
                StructField("max_file_size_bytes", LongType(), True),
                StructField("file_skew_ratio", DoubleType(), True),
                StructField("ingestion_ts_min", TimestampType(), True),
                StructField("ingestion_ts_max", TimestampType(), True),
                StructField("earliest_file_mod_time", TimestampType(), True),
                StructField("latest_file_mod_time", TimestampType(), True),
                StructField("throughput_rows_per_sec", DoubleType(), True),
                StructField("run_status", StringType(), True),
                StructField("notes", StringType(), True),
            ]
        )

        metrics_df = spark.createDataFrame(
            [
                (
                    master_run_id,
                    layer_run_id,
                    layer_name,
                    source_table,
                    entity_name,
                    target_table,
                    start_ts,
                    end_ts,
                    duration_secs,
                    files_processed,
                    rows_read,
                    rows_written,
                    total_file_size_bytes,
                    avg_file_size_bytes,
                    max_file_size_bytes,
                    file_skew_ratio,
                    ingestion_ts_min,
                    ingestion_ts_max,
                    earliest_file_mod_time,
                    latest_file_mod_time,
                    throughput_rows_per_sec,
                    run_status,
                    notes,
                )
            ],
            schema=schema,
        ).withColumn("load_timestamp", current_timestamp())
    except Exception as e:
        log_event(
            logger_bronze_entity,
            "ERROR",
            f"Failed to prepare metrics DataFrame for {entity_name}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )
        raise

    # -----------------------------
    # Write metrics to table
    # -----------------------------
    try:
        metrics_df.write.format("delta").mode("append").saveAsTable(metrics_table)
        log_event(
            logger_bronze_entity,
            "INFO",
            f"Entity metrics successfully written for {entity_name} to {metrics_table}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER,
            entity_name=entity_name
        )
    except Exception as e:
        log_event(
            logger_bronze_entity,
            "ERROR",
            f"Failed to write entity metrics for {entity_name} to {metrics_table}: {e}",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER,
            entity_name=entity_name
        )
        raise

def get_last_processed_ts(spark, table_name):
    if spark.catalog.tableExists(table_name):
        max_ts = (
            spark.table(table_name)
            .agg({"ingestion_ts": "max"})
            .collect()[0][0]
        )
        return max_ts
    else:
        return None


# --------------------------------------
# Function: Incremental Append for One Bronze Entity
# --------------------------------------
def bronze_entity_incremental_append(
    entity_name: str,
    entity_cfg: dict,
    spark,
    metadata_columns: list,
    master_run_id: str,
    layer_run_id: str
):
    """
    Handles incremental read and append for a single Bronze entity.

    Args:
        entity_name: Logical entity name (e.g., 'customers')
        entity_cfg: Configuration dict for the entity
        spark: SparkSession
        metadata_columns: List of metadata columns
        master_run_id: Master run ID
        layer_run_id: Layer run ID
    """
    log_event(
        logger_bronze_entity,
        "INFO",
        f"Starting incremental append for entity: {entity_name}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=BRONZE_LAYER
    )

    table_name = table(get_bronze_schema(), entity_cfg["table_name"].split('.')[-1])
    partition_col = entity_cfg["partition_col"]
    all_columns = entity_cfg["columns"] + entity_cfg["metadata_columns"]
    # source_table = entity_cfg["source_table"]
    source_table = table(get_bronze_schema(), entity_cfg["source_table"].split('.')[-1])

    # Read raw Bronze table
    raw_df = spark.table(source_table)

    # Incremental filter
    last_ts = get_last_processed_ts(spark, table_name)
    if last_ts:
        df_entity = raw_df.filter(raw_df.ingestion_ts > last_ts)
    else:
        df_entity = raw_df

    rows_count = df_entity.count()
    if rows_count == 0:
        log_event(
            logger_bronze_entity,
            "INFO",
            f"No new rows/files ingested for {entity_name}; skipping append",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )
        return df_entity  # empty DF for metrics

    # Select relevant columns
    entity_df = df_entity.select(*all_columns)

    # Append to Bronze Delta table
    entity_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy(partition_col) \
        .saveAsTable(table_name)

    log_event(
        logger_bronze_entity,
        "INFO",
        f"Entity {entity_name} successfully appended to {table_name}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=BRONZE_LAYER
    )

    return entity_df  # return DF for metrics computation

# def optimize_zorder_bronze_tables(spark, bronze_entities: dict, table_name: str, master_run_id: str, layer_run_id: str):
#     """
#     Optimizes Delta tables and optionally Z-Orders by high-cardinality columns.
#     """
#     log_event(
#         logger_bronze_entity,
#         "INFO",
#         "Starting Bronze entity tables optimize and z-order ...",
#         master_run_id=master_run_id,
#         layer_run_id=layer_run_id,
#         layer=BRONZE_LAYER
#     )
#     for entity_name, entity_conf in bronze_entities.items():
#         # table_name = entity_conf["table_name"]
#         table_name= 
#         z_order_cols = entity_conf.get("z_order_columns", [])

#         try:
#             spark.sql(f"OPTIMIZE {table_name}")
#             log_event(logger_bronze_entity, "INFO", f"Delta table optimized", 
#                     table=table_name, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=BRONZE_LAYER)
#         except Exception as e:
#             log_event(logger_bronze_entity, "WARNING", f"Optimize failed for {table_name}: {e}",
#                     table=table_name, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=BRONZE_LAYER)

#         if z_order_cols:
#             try:
#                 z_order_cols_str = ",".join(z_order_cols)
#                 spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_order_cols_str})")
#                 log_event(logger_bronze_entity, "INFO", f"Entity table Z-ordered by {z_order_cols_str}", 
#                         table=table_name, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=BRONZE_LAYER)
#             except Exception as e:
#                 log_event(logger_bronze_entity, "WARNING", f"Z-Order failed for {table_name}: {e}",
#                         table=table_name, master_run_id=master_run_id, layer_run_id=layer_run_id,layer=BRONZE_LAYER)
                

def optimize_zorder_bronze_tables(
    spark,
    entity_name: str,
    entity_table_name: str,
    z_order_cols: list,
    master_run_id: str,
    layer_run_id: str
):
    """
    Optimizes a single Bronze table and applies Z-Order if configured.
    Assumes fully qualified table name is passed from orchestrator.
    """

    log_event(
        logger_bronze_entity,
        "INFO",
        f"Starting optimize for {entity_name}",
        table=entity_table_name,
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=BRONZE_LAYER
    )

    # -------------------------------
    # Check table exists
    # -------------------------------
    if not spark.catalog.tableExists(entity_table_name):
        log_event(
            logger_bronze_entity,
            "WARNING",
            f"Skipping optimize. Table not found: {entity_table_name}",
            table=entity_table_name,
            entity_name=entity_name,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )
        return

    # -------------------------------
    # OPTIMIZE
    # -------------------------------
    try:
        spark.sql(f"OPTIMIZE {entity_table_name}")

        log_event(
            logger_bronze_entity,
            "INFO",
            "Delta table optimized",
            table=entity_table_name,
            entity_name=entity_name,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )

    except Exception as e:
        log_event(
            logger_bronze_entity,
            "WARNING",
            f"Optimize failed: {e}",
            table=entity_table_name,
            entity_name=entity_name,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )

    # -------------------------------
    # Z-ORDER
    # -------------------------------
    if z_order_cols:
        try:
            z_cols = ",".join(z_order_cols)

            spark.sql(
                f"OPTIMIZE {entity_table_name} ZORDER BY ({z_cols})"
            )

            log_event(
                logger_bronze_entity,
                "INFO",
                f"Z-ordered by {z_cols}",
                table=entity_table_name,
                entity_name=entity_name,
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=BRONZE_LAYER
            )

        except Exception as e:
            log_event(
                logger_bronze_entity,
                "WARNING",
                f"Z-Order failed: {e}",
                table=entity_table_name,
                entity_name=entity_name,
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=BRONZE_LAYER
            )
                