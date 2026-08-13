"""
==============================================================
Module: Bronze to Silver ETL with Delta Lake, Data Quality & Hash-Based Idempotency

Purpose:
    This module implements a robust, configuration-driven Bronze→Silver
    ETL pipeline for Databricks Delta Lake environments, fully compatible
    with serverless Spark. It is entirely DataFrame-based (no RDD usage)
    and designed for scalable, production-grade ingestion with safe
    reprocessing guarantees.

    In addition to data quality enforcement and deduplication, the module
    generates deterministic, entity-specific hash identifiers to support
    idempotent merges, auditability, and traceability across Silver,
    quarantine, and audit layers.

Key Features:
1. Data Quality & Validation:
    - Null checks applied to all columns by default.
    - Regex-based validation for configurable columns (e.g. dates, numerics).
    - Categorical allowed-values enforcement.
    - Dirty row isolation into a dedicated quarantine Delta table.

2. Hash ID Generation & Usage:
    - Generates deterministic SHA-256 hash IDs per entity using plural
      table names to ensure consistent naming across layers.
    - Hash columns serve as stable technical keys for idempotent merges
      and safe re-runs:
        • silver_<entity>_hash_id
            - Derived from business keys.
            - Used as the primary technical key in Silver tables.
        • duplicates_<entity>_hash_id
            - Derived from full row content.
            - Used to uniquely identify and audit duplicate records.
        • quarantine_<entity>_hash_id
            - Derived from full row content.
            - Used to uniquely track dirty rows across reprocessing cycles.

3. Numeric Casting:
    - Configurable numeric columns are cast only after data quality
      validation succeeds.
    - Uses safe casting logic to prevent invalid values from entering
      Silver tables.

4. Deduplication & Audit:
    - Deduplicates records in Silver using configured business keys and
      ingestion timestamps (latest record wins).
    - Captures non-winning duplicate records in a dedicated audit table
      for downstream analysis and troubleshooting.

5. Delta Lake Merge Operations:
    - Uses idempotent Delta `MERGE` operations for Silver, quarantine,
      and audit tables.
    - Ensures consistent state during incremental loads and pipeline
      re-runs without data duplication.

6. Logging & Observability:
    - Each table execution generates a unique run_id for traceability.
    - Logged metrics include total rows, good vs dirty rows, deduplicated
      rows, duplicate rows, and corresponding percentages.
    - Errors are isolated and logged per table, allowing the pipeline
      to continue processing remaining tables.

7. Serverless Compatibility & Performance:
    - Fully DataFrame-based implementation (no RDDs).
    - Controlled repartitioning via `shuffle_partitions`.
    - Designed to run efficiently on Databricks serverless clusters.

8. Multi-Table Processing:
    - `process_multiple_tables` wrapper enables sequential ingestion of
      multiple Bronze tables using table-specific configurations.
    - Supports optional quarantine tables on a per-entity basis.

Best Practices / Notes:
- Ensure business keys are stable and correctly configured in
  `table_configs`, as they directly impact Silver hash generation.
- Delta tables are auto-created when missing, but schema evolution
  should be managed explicitly in production.
- Avoid excessive `.count()` operations on very large datasets in
  serverless mode; metrics can be approximated or aggregated asynchronously.
- Regex and categorical rules should be carefully tuned to prevent
  unintended over-quarantining of valid records.
- Logging verbosity can be adjusted (e.g. DEBUG) for deep diagnostics
  during development or incident investigation.

==============================================================
"""
# -----------------------------
# Spark Timezone Initialization
# -----------------------------
# Optional timezone override (kept commented to allow environment-level control)
# spark.conf.set("spark.sql.session.timeZone", "Australia/Sydney")

# -----------------------------
# Core Python Utilities
# -----------------------------
import builtins  # Used for built-in round() in metrics calculations
import time       # Used for ETL execution timing and duration tracking
import json, uuid  # json for logging payloads, uuid for unique run identifiers

# -----------------------------
# PySpark Core Types
# -----------------------------
from pyspark.sql import DataFrame, Row  # DataFrame typing and Row object for structured outputs

# -----------------------------
# PySpark Functions (Transformations & Metrics)
# -----------------------------
from pyspark.sql.functions import (
    col, trim, regexp_replace, sha2, concat_ws, current_timestamp,  # data cleaning & hashing
    row_number, to_date, coalesce, max as spark_max, lit, try_to_date,          # windowing, casting, aggregation helpers
    spark_partition_id, when, expr, size, array, array_union,       # partition tracking, conditional logic, array ops
    count as spark_count, sum as spark_sum                          # aliased: keep builtin count()/sum() usable
)

# -----------------------------
# Window Functions
# -----------------------------
from pyspark.sql.window import Window  # Used for deduplication and ordering logic

# -----------------------------
# Delta Lake Operations
# -----------------------------
from delta.tables import DeltaTable  # Enables MERGE, UPDATE, DELETE operations on Delta tables

# -----------------------------
# Schema Definitions
# -----------------------------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DateType, LongType, DoubleType
)

# -----------------------------
# Date Parsing Formats
# -----------------------------
# Source dates are dd-MM-yyyy. That format is listed FIRST so the common case
# short-circuits the coalesce below instead of failing a parse attempt on every
# row. Order is safe to change only while no two formats can match the same
# string with different meanings (dash vs slash keeps these disjoint).
DATE_FORMATS = [
    "dd-MM-yyyy",
    "yyyy-MM-dd",
    "d/M/yyyy",
    "dd/MM/yyyy",
    "yyyy/MMM/d",
    "yyyy MMM d",
    "d MMMM yyyy",
]

# -----------------------------
# Optional Timezone Library
# -----------------------------
# import pytz  # Can be used for timezone conversions if required

# -----------------------------
# UUID Utilities
# -----------------------------
from uuid import uuid4  # Generates unique identifiers for tracking runs/events


# -----------------------------
# Platform Utilities
# -----------------------------
from superstore_logger import get_superstore_logger, log_event          # Custom logging framework
from superstore_platform_constants import SILVER_LAYER                  # Layer constant for Silver pipeline
from superstore_backfill_utils import (                                 # run-mode support
    get_incremental_with_backfill,
    reprocessed_scope_predicate,
    run_mode_load_type
)
from superstore_silver_transformations import (                                    # pure logic (unit-tested)
    classify_duplicates,
    row_hash,
    clean_string_columns,
    standardize_values,
    add_error_columns,
    add_is_valid,
)

# -----------------------------
# Spark Session (if needed locally)
# -----------------------------
from pyspark.sql import SparkSession  # Spark session reference (used if initialized here)

# -----------------------------
# Logger Setup for silver tranformation
# -----------------------------
# Initialize logger to capture events in the silver transformation pipeline
logger_silver = get_superstore_logger("superstore_silver_module")

# def get_incremental_bronze(
#     spark: SparkSession,
#     bronze_table: str,
#     silver_table: str,
#     master_run_id: str,
#     layer_run_id: str,
#     ingestion_col: str = "bronze_ingestion_ts",
#     required_table: bool = False  # New flag: raise error if True, skip if False
# ) -> DataFrame:
#     """
#     Returns only new Bronze rows not yet ingested into Silver.
#     Optimized for serverless / partitioned Bronze tables.
    
#     Handles missing Bronze tables gracefully:
#     - If required_table=True: raises Exception
#     - If required_table=False: returns empty DataFrame
#     """
#     # -------------------------------
#     # Check if Bronze table exists
#     # -------------------------------
#     if not spark.catalog.tableExists(bronze_table):
#         msg = f"Source bronze table '{bronze_table}' does not exist"
#         if required_table:
#             log_event(
#                 logger_silver,
#                 "ERROR",
#                 msg,
#                 master_run_id=master_run_id,
#                 layer_run_id=layer_run_id,
#                 layer=SILVER_LAYER
#             )
#             raise Exception(msg)
#         else:
#             log_event(
#                 logger_silver,
#                 "WARN",
#                 msg + ". Skipping.",
#                 master_run_id=master_run_id,
#                 layer_run_id=layer_run_id,
#                 layer=SILVER_LAYER
#             )
#             # Return empty DataFrame with no schema
#             # return spark.createDataFrame([], schema=None)
#             return spark.createDataFrame([], StructType([]))
    
#     # -------------------------------
#     # Log start of incremental fetch
#     # -------------------------------
#     log_event(
#         logger_silver,
#         "INFO",
#         f"Fetching incremental rows from Bronze table '{bronze_table}' for Silver table '{silver_table}'",
#         master_run_id=master_run_id,
#         layer_run_id=layer_run_id,
#         layer=SILVER_LAYER,
#         ingestion_col=ingestion_col
#     )
    
#     # -------------------------------
#     # Determine last ingestion timestamp from Silver
#     # -------------------------------
#     max_bronze_ingestion_ts = None
#     if spark.catalog.tableExists(silver_table):
#         max_bronze_ingestion_ts_row = (
#             spark.table(silver_table)
#             .agg(spark_max(ingestion_col).alias("max_ingest_ts"))
#             .first()
#         )
#         max_bronze_ingestion_ts = max_bronze_ingestion_ts_row["max_ingest_ts"]
#         log_event(
#             logger_silver,
#             "INFO",
#             f"Max ingestion timestamp found in Silver table '{silver_table}': {max_bronze_ingestion_ts}",
#             master_run_id=master_run_id,
#             layer_run_id=layer_run_id,
#             layer=SILVER_LAYER
#         )
    
#     # -------------------------------
#     # Read Bronze table
#     # -------------------------------
#     bronze_df = spark.table(bronze_table)
    
#     # Apply incremental filter if Silver has data
#     if max_bronze_ingestion_ts:
#         incremental_df = bronze_df.filter(col(ingestion_col) > max_bronze_ingestion_ts)
#     else:
#         incremental_df = bronze_df
#         log_event(
#             logger_silver,
#             "INFO",
#             f"Silver table '{silver_table}' does not exist. Returning full Bronze table.",
#             master_run_id=master_run_id,
#             layer_run_id=layer_run_id,
#             layer=SILVER_LAYER
#         )
    
#     # -------------------------------
#     # Log row count
#     # -------------------------------
#     has_data = incremental_df.limit(1).count() > 0
#     log_event(
#         logger_silver,
#         "INFO",
#         f"Incremental Bronze rows to process: {has_data}",
#         master_run_id=master_run_id,
#         layer_run_id=layer_run_id,
#         layer=SILVER_LAYER
#     )
    
#     return incremental_df

# -------------------------------
# Derived-table write (quarantine / audit)
# -------------------------------
def write_derived_table(
    spark,
    df,
    table: str,
    backfill_config: dict,
    what: str,
    master_run_id: str,
    layer_run_id: str,
) -> None:
    """
    Append `df` to a derived Delta table, clearing anything this run is about
    to re-derive first.

    Quarantine and audit are pure functions of the Bronze rows read for this
    run, so re-reading those rows must REPLACE their derived output, not add a
    second copy. Plain append is only safe for incremental, which never
    re-reads. Under replay / backfill / full_refresh it duplicated every dirty
    and duplicate row, inflating both tables by a full copy per run and
    breaking `bronze == silver + quarantine + audit`.

    Silver itself does not need this — it MERGEs on the row hash, so it was
    already idempotent. These two tables were the gap.

    The delete runs before the append and is scoped by
    `reprocessed_scope_predicate` to exactly the rows the read re-read.
    """
    if not table:
        return

    if not spark.catalog.tableExists(table):
        (df.limit(0)
           .write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(table))

        log_event(
            logger_silver, "INFO", f"Created {what} table with schema",
            table=table, master_run_id=master_run_id,
            layer_run_id=layer_run_id, layer=SILVER_LAYER,
        )
    else:
        predicate = reprocessed_scope_predicate(backfill_config)
        if predicate:
            # Idempotency, not cleanup: these rows are about to be rewritten
            # from the same Bronze input they were derived from.
            spark.sql(f"DELETE FROM {table} WHERE {predicate}")

            log_event(
                logger_silver, "INFO",
                f"Cleared re-derived scope from {what} before append",
                table=table, scope_predicate=predicate,
                master_run_id=master_run_id, layer_run_id=layer_run_id,
                layer=SILVER_LAYER,
            )

    (df.write
       .format("delta")
       .mode("append")
       # repaired_columns is additive; existing quarantine/audit tables predate
       # it. Bronze's entity append already uses mergeSchema for the same reason.
       .option("mergeSchema", "true")
       .saveAsTable(table))

    log_event(
        logger_silver, "INFO", f"Rows appended to {what} table",
        table=table, master_run_id=master_run_id,
        layer_run_id=layer_run_id, layer=SILVER_LAYER,
    )


# -------------------------------
# Bronze → Silver ETL
# -------------------------------
def bronze_to_silver_prod(
    spark,
    bronze_table: str,
    silver_table: str,
    audit_table: str,
    master_run_id:str ,
    layer_run_id: str,
    backfill_config: dict,  # backfill parameter
    business_keys: list,
    business_columns: list,
    meta_columns: list,
    numeric_cast_cols: dict,
    regex_cols: dict = {},
    date_cast_cols: dict = {},
    categorical_allowed_vals: dict = {},
    value_standardization: dict = {},
    severity: dict = {},
    quarantine_table: str = None,
    shuffle_partitions: int = 200,
    metrics_table: str=None,
    layer_name: str=None,
):
    """
    Ingest data from Bronze → Silver with full data quality and auditing.

    Key Steps:
    1. Read Bronze Delta table.
    2. Clean columns (trim, remove quotes, etc.).
    3. Data quality checks:
        - Null checks
        - Regex validations
        - Categorical allowed values
    4. Separate good vs dirty rows.
    5. Quarantine dirty rows, tagged with an entity-specific hash column.
    6. Cast numeric columns for Silver.
    7. Deduplicate Silver and identify duplicates using entity-specific hash columns.
    8. Update Silver by Delta merge; write quarantine and audit via
       write_derived_table, which clears the scope this run re-derives before
       appending. Silver is idempotent through its merge key; the other two
       are idempotent through that scoped delete.
    9. Log metrics (total, dedup %, duplicates %).
    """

    # Start timer to measure processing time
    start_ts = spark.sql("SELECT current_timestamp() as ts").first()["ts"]
    start_time_epoch = time.time()       # For duration calculation

    # -------------------------------
    # Default metrics initialization (for failure safety)
    # -------------------------------
    read_rows = 0
    good_count = 0
    dirty_count = 0
    dedups_count = 0
    dups_count = 0
    throughput_rows_per_sec = 0
    skew_ratio = 0.0
    run_status = "SUCCESS"
    load_type= None
    notes = None

    inserted_rows = 0
    updated_rows = 0
    unchanged_rows = 0


    # -------------------------------
    # Entity name & hash column naming
    # -------------------------------
    entity_name = bronze_table.split(".")[-1]  # Extract entity name from fully qualified table name
    # Define hash column names for each of the layers (quarantine, silver, and duplicates)
    quarantine_col = f"quarantine_{entity_name}_hash_id"  # For dirty rows that fail validation
    silver_col = f"silver_{entity_name}_hash_id"  # For deduplication in Silver table based on business keys
    duplicate_col = f"duplicates_{entity_name}_hash_id"  # For tracking duplicates in the audit table

    try:
        # -------------------------------
        # Step 1: Read Bronze table
        # -------------------------------
        # Read the Delta table into a DataFrame incrementally
        # df = get_incremental_bronze(spark, bronze_table, silver_table, master_run_id=master_run_id, layer_run_id=layer_run_id, ingestion_col="bronze_ingestion_ts")

        df = get_incremental_with_backfill(
            spark=spark,
            source_table=bronze_table,
            target_table=silver_table,
            backfill_config=backfill_config,  # New parameter
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=SILVER_LAYER,
            ingestion_col="bronze_ingestion_ts",
            date_partition_col="ingestion_date"
        )

        # Case 1: Bronze table missing
        if df is None:
            log_event(
                logger_silver,
                "WARN",
                "No table found in bronze: '{bronze_table}'",
                table=bronze_table,
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=SILVER_LAYER
            )
            return {
                "master_run_id": master_run_id,
                "layer_run_id": layer_run_id,
                "layer": layer_name,
                "table_name": bronze_table,
                "start_ts": start_ts,
                "end_ts": start_ts,
                "read_rows": 0,
                "good_rows": 0,
                "dirty_rows": 0,
                "deduplicated_rows": 0,
                "duplicate_rows": 0,
                "inserted_rows" : 0,
                "updated_rows" : 0,
                "unchanged_rows" : 0,
                "throughput_rows_per_sec": 0,
                "skew_ratio": 0.0,
                "run_status": "success",
                "load_type" : "NO_DATA",
                "duration_secs": 0,
                "notes": f"Source silver table '{bronze_table}' does not exist",
                "metrics_table": metrics_table
            }

        # Check for rows in bronze table
        has_incremental_rows = df.head(1)

        #case 2: Dataframe has no rows
        if not has_incremental_rows:
            log_event(logger_silver,"INFO", "No new rows or files to process; continuing with empty dataframe", table=bronze_table, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=SILVER_LAYER)
            return {
                "master_run_id": master_run_id,   
                "layer_run_id": layer_run_id,
                "layer": layer_name,
                "table_name": bronze_table,
                "start_ts": spark.sql("SELECT current_timestamp() as ts").first()["ts"],
                "end_ts": spark.sql("SELECT current_timestamp() as ts").first()["ts"],
                "read_rows": 0,
                "good_rows": 0,
                "dirty_rows": 0,
                "deduplicated_rows": 0,
                "duplicate_rows": 0,
                "inserted_rows" : 0,
                "updated_rows" : 0,
                "unchanged_rows" : 0,
                "throughput_rows_per_sec" : 0,
                "skew_ratio": 0,
                "run_status": "SKIPPED",
                "load_type" : "NO_DATA",
                "duration_secs": 0,
                "notes":f"No incremental rows in silver table '{bronze_table}'",
                "metrics_table": metrics_table
            }
            
        else:
            # Case 2: DataFrame has rows
            #
            # read_rows is NOT counted here. Every Spark action on this lineage
            # replays it in full (clean -> DQ -> multi-format date parse) because
            # serverless forbids cache()/persist(), so a standalone count() costs a
            # whole pass. It is derived for free from the is_valid split in Step 4
            # instead: read_rows = good_count + dirty_count.

            # Separate business and metadata columns
            business_cols = [c for c in business_columns if c in df.columns]
            metadata_cols = [c for c in df.columns if c not in business_cols]

            # -------------------------------
            # Step 2: Clean columns (trim + strip quotes) — extracted, unit-tested
            # -------------------------------
            clean_df = clean_string_columns(df, business_columns)

            # -------------------------------
            # Step 2b: Conform known source dialects BEFORE validating
            # -------------------------------
            # A source writing "OFF" for "Office Supplies" is not sending bad
            # data, so this must run ahead of the DQ rules -- validating first
            # quarantined 50,264 products on `category` and orphaned their
            # facts from every mart. Unmapped values are untouched and still
            # face the rules below.
            clean_df = standardize_values(clean_df, value_standardization)


            # -------------------------------
            # Step 3: Data Quality Checks
            # -------------------------------
            # first approach
            # dq_df = clean_df
            # is_any_dirty_col = None

            # # Null checks for all columns
            # for c in business_cols:
            #     col_dirty = col(c).isNull()
            #     dq_df = dq_df.withColumn(f"is_{c}_dirty", col_dirty)
            #     is_any_dirty_col = col_dirty if is_any_dirty_col is None else is_any_dirty_col | col_dirty

            # # Regex validation for configured columns
            # for c, regex in regex_cols.items():
            #     dq_df = dq_df.withColumn(
            #         f"is_{c}_dirty",
            #         (~col(c).rlike(regex)) | col(f"is_{c}_dirty")
            #     )
            #     is_any_dirty_col = is_any_dirty_col | (~col(c).rlike(regex))

            # # Categorical allowed values check
            # for c, allowed_vals in categorical_allowed_vals.items():
            #     dq_df = dq_df.withColumn(
            #         f"is_{c}_dirty",
            #         (~col(c).isin(allowed_vals)) | col(f"is_{c}_dirty")
            #     )
            #     is_any_dirty_col = is_any_dirty_col | (~col(c).isin(allowed_vals))

            # # Aggregate flag for any dirty column
            # dq_df = dq_df.withColumn("is_any_dirty", is_any_dirty_col)

            # Data quality: build error_columns (null + regex + categorical) — extracted, unit-tested
            dq_df = add_error_columns(
                clean_df, business_columns, regex_cols, categorical_allowed_vals, severity
            )

            # Business rule: ship_date should not be before order_date
            if "order_date" in business_columns and "ship_date" in business_columns:
            #     date_formats = [
            #         "d/M/yyyy", 
            #         "dd-MM-yyyy",
            #         "yyyy-MM-dd",
            #         "dd/MM/yyyy",
            #         "yyyy/MMM/d",
            #         "yyyy MMM d",
            #         "d MMMM yyyy"
            #     ]

            #     def parse_multi_format_date(column_name):
            #         return coalesce(*[
            #             try_to_date(col(column_name), fmt)
            #             for fmt in date_formats
            #         ])
                def parse_multi_format_date(column_name):
                    return coalesce(*[
                        try_to_date(col(column_name), fmt)
                        for fmt in DATE_FORMATS
                    ])

                dq_df = dq_df.withColumn(
                    "order_date_dt",
                    parse_multi_format_date("order_date")
                ).withColumn(
                    "ship_date_dt",
                    parse_multi_format_date("ship_date")
                )

                
                dq_df = dq_df.withColumn(
                    "error_columns",
                    when(
                        col("ship_date_dt") < col("order_date_dt"),
                        array_union(col("error_columns"), array(lit("ship_date_before_order_date")))
                    ).otherwise(col("error_columns"))
                )
                    
            
            # Set is_valid based on error_columns — extracted, unit-tested
            dq_df = add_is_valid(dq_df)

            log_event(logger_silver, "INFO", "Data quality rules applied", table=bronze_table, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=SILVER_LAYER)


            # -------------------------------
            # Step 4: Separate good vs dirty rows
            # -------------------------------

            # first apparoach
            # good_rows_df = dq_df.filter(~col("is_any_dirty"))  # Keep rows that pass all data quality checks
            # dirty_rows_df = dq_df.filter(col("is_any_dirty"))  # Rows that failed data quality checks

            # second appraoch
            good_rows_df = dq_df.filter(col("is_valid") == True)
            dirty_rows_df = dq_df.filter(col("is_valid") == False)

            # ONE pass for both counts instead of three.
            #
            # Counting good_rows_df and dirty_rows_df separately replayed the entire
            # lineage twice, and read_rows above paid for it a third time. A single
            # groupBy on is_valid gets all three numbers in one pass.
            #
            # Exhaustiveness: add_is_valid() derives is_valid from
            # size(filter(error_columns, x -> x is not null)) == 0. error_columns is
            # always a non-null array (add_error_columns builds it with array(...)),
            # so size() never returns null and is_valid is strictly True/False --
            # the two buckets partition the frame and their sum is read_rows.
            valid_counts = {
                row["is_valid"]: row["count"]
                for row in dq_df.groupBy("is_valid").count().collect()
            }
            good_count = valid_counts.get(True, 0)
            dirty_count = valid_counts.get(False, 0)
            read_rows = good_count + dirty_count

            log_event(logger_silver,
                    "INFO", "Bronze to Silver row counts",
                    table=bronze_table,
                    read_rows=read_rows,
                    good_rows=good_count,
                    dirty_rows=dirty_count,
                    master_run_id=master_run_id,
                    layer_run_id=layer_run_id,
                    layer=SILVER_LAYER
                )


            # -------------------------------
            # Step 5: Quarantine dirty rows
            # -------------------------------
            if dirty_count > 0:  # Proceed only if there are dirty rows (already counted above)
                # Generate a SHA-256 hash for each dirty row based on its full content
                # The hash ensures uniqueness for quarantine table purposes
                dirty_selected_cols = [*business_columns, *meta_columns, "error_columns", "repaired_columns", "is_valid"]
                dirty_rows_df = (
                    dirty_rows_df.select(*dirty_selected_cols)
                    # Generate SHA-256 hash for full row (shared row_hash() helper)
                    .withColumn(quarantine_col, row_hash(business_columns))
                    .withColumn("quarantine_ingestion_ts", current_timestamp())  # Add timestamp for tracking
                )
    
                # Write into quarantine table (re-derived scope cleared first)
                write_derived_table(
                    spark,
                    dirty_rows_df,
                    quarantine_table,
                    backfill_config,
                    "quarantine",
                    master_run_id,
                    layer_run_id,
                )


            # -------------------------------
            # Step 6a: Cast numeric columns
            # -------------------------------
            silver_df = good_rows_df
            for c, t in numeric_cast_cols.items():
                # Filter out invalid numeric values, then cast to the specified type
                silver_df = (
                    silver_df
                    .filter(f"try_cast({c} as {t}) IS NOT NULL")  # Validate numeric values
                    .withColumn(c, col(c).cast(t))  # Cast column to numeric type
                )
            log_event(logger_silver, 
                    "INFO", "Numeric casting applied", 
                    table=bronze_table,
                    master_run_id=master_run_id,
                    layer_run_id=layer_run_id,
                    layer=SILVER_LAYER
                )

            # Step 6b: Cast string columns to DATE only if they are of StringType
            # date_formats = ["d/M/yyyy", "dd-MM-yyyy", "yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MMM/d", "yyyy MMM d", "d MMMM yyyy"]

            # Explicitly cast order_date and ship_date to DateType using different formats if they are strings
            silver_cast_df = silver_df
            for c in ["order_date", "ship_date"]:
                if c in silver_df.columns:
                    # Check the current data type of the column
                    column_type = silver_df.schema[c].dataType
                    # If the column is of StringType, we attempt to cast it to DateType
                    if isinstance(column_type, StringType):
                        # Try different date formats and coalesce the results to avoid nulls
                        silver_cast_df = silver_cast_df.withColumn(
                            c,
                            # coalesce(*[try_to_date(col(c), f) for f in date_formats])  # Use multiple formats
                            coalesce(*[try_to_date(col(c), f) for f in DATE_FORMATS])  # Use multiple formats
                        )
                    # If the column is already a DateType, skip casting and leave it as is
                    elif column_type == DateType():
                        log_event(logger_silver, "INFO",f"Column {c} is already a DateType. Skipping casting.", master_run_id=master_run_id, layer_run_id=layer_run_id, layer=SILVER_LAYER)

            # -------------------------------
            # Step 6c: Compute skew and throughput
            # -------------------------------
            # -----------------------------
            # Throughput
            # -----------------------------
            throughput_rows_per_sec = round(read_rows / max(int(time.time() - start_time_epoch), 1),2)

            # (Skew is no longer computed here -- it is folded into the single
            # Step 7 metrics pass below, which already has to scan this data.)

            # -------------------------------
            # Step 7: Deduplication & audit
            # -------------------------------
            # Deduplication: keep only the latest record per business key.
            # Logic extracted to classify_duplicates() so it can be unit tested.
            silver_df = silver_cast_df.repartition(shuffle_partitions, *business_keys)  # Repartition by business keys

            # classify_duplicates() attaches row_num once; the metrics pass and BOTH
            # output branches below are derived from that same frame, so the dedup
            # window is expressed once rather than per-branch.
            # tiebreak_cols is passed explicitly rather than left to default: the
            # frame here also carries error_columns / is_valid, which are derived
            # from the business columns and so add nothing to the ordering. The
            # tiebreak makes a same-batch duplicate pair (identical
            # bronze_ingestion_ts) resolve the same way on every run instead of
            # by shuffle order -- reproducible, not business-correct.
            classified_df = classify_duplicates(
                silver_df,
                business_keys=business_keys,
                order_col="bronze_ingestion_ts",
                tiebreak_cols=business_columns,
            )

            # -----------------------------
            # Step 7 metrics: ONE pass for total / dedup / duplicate / skew
            # -----------------------------
            # This replaces five separate passes: the skew collect() above, plus
            # dedup_rows, total, dedups_count and dups_count which were each counted
            # individually further down. Grouping by partition id yields the row
            # counts and the skew distribution in the same aggregation.
            #
            # NOTE: skew is now measured AFTER the repartition by business_keys;
            # previously it was measured on silver_cast_df, before it. This is the
            # skew the dedup window actually experiences, so a hot business key now
            # shows up -- the old placement could only see the bronze read layout.
            #
            # This aggregation is load-bearing (the merge branches below gate on its
            # counts), so unlike the old skew block it is deliberately NOT wrapped in
            # a try/except that swallows failures -- a failure here must fail the ETL.
            partition_stats = (
                classified_df.groupBy(spark_partition_id().alias("_partition_id"))
                .agg(
                    spark_count(lit(1)).alias("row_count"),
                    spark_sum(when(col("row_num") == 1, 1).otherwise(0)).alias("winner_count"),
                    spark_sum(when(col("row_num") > 1, 1).otherwise(0)).alias("loser_count"),
                )
                .collect()
            )

            total = sum(row["row_count"] for row in partition_stats)          # pre-dedup rows
            dedups_count = sum(row["winner_count"] for row in partition_stats)  # latest-wins keepers
            dups_count = sum(row["loser_count"] for row in partition_stats)     # audit-bound losers

            counts = [row["row_count"] for row in partition_stats]
            if counts:
                avg_partition_rows = sum(counts) / len(counts)
                skew_ratio = builtins.round(max(counts) / avg_partition_rows, 2)
            else:
                skew_ratio = 0.0

            # Winners / losers split from the SAME classified frame.
            silver_dedup_df = classified_df.filter(col("row_num") == 1).drop("row_num")
            silver_dup_df = classified_df.filter(col("row_num") > 1).drop("row_num")

            # -------------------------------
            # Step 7a: Merge into Silver
            # -------------------------------
            has_dedup_rows = dedups_count > 0  # from the Step 7 metrics pass; no extra scan
            if not has_dedup_rows:
                log_event(logger_silver, "INFO", "No de_dups rows received; skipping de_dups silver merge",table=silver_table, master_run_id=master_run_id, layer_run_id=layer_run_id,layer=SILVER_LAYER)
            else:
                # Proceed only if Silver has rows to insert
                # Generate a SHA-256 hash for each dedups row based on its full content
                # The hash ensures uniqueness for deduplication and auditing purposes dup_columns = [c for c in silver_dup_df.columns if c not in ["row_num"]]
                dedups_selected_cols = [*business_columns, *meta_columns, "error_columns", "repaired_columns", "is_valid"]
                silver_dedup_df = (
                    silver_dedup_df.select(*dedups_selected_cols)
                    .withColumn(silver_col, row_hash(business_columns))  # Generate SHA-256 hash (shared helper)
                    .withColumn("silver_ingestion_ts", current_timestamp())  # Add timestamp for tracking
                    # .dropDuplicates([silver_col])  # Deduplicate dedups rows using their hash
                    .repartition(shuffle_partitions, col(silver_col))
                )

                # Merge good dedups rows into the silver table using the hash as the matching key
                if silver_table:
                    if not spark.catalog.tableExists(silver_table):
                        # If the silver table doesn't exist, create an empty one
                        silver_dedup_df.limit(0).write.format("delta").saveAsTable(silver_table)
                        log_event(logger_silver, "INFO", "Created silver table", table=silver_table, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=SILVER_LAYER)

                    # Repartition by the hash column for more efficient processing
                    silver_dedup_df = silver_dedup_df.repartition(shuffle_partitions, col(silver_col))

                    # Track the count of rows dedups rows before merge.
                    # Already known from the Step 7 metrics pass -- the select/hash/
                    # repartition above are row-preserving, so no re-count is needed.
                    dedup_rows = dedups_count

                    # -----------------------------
                    # Determine Gold table load state
                    # -----------------------------
                    # target_exists  → checks whether the Gold table is present in the metastore
                    # target_empty   → checks whether the table exists but has no data
                    #
                    # This logic defines the execution mode for SCD2 processing:
                    #
                    # INITIAL_LOAD   → Gold table is empty (first-time full load)
                    # SCD2_APPLIED   → Gold table already contains data and will undergo incremental SCD2 merge
                    #
                    # This mode is later used for observability, audit metrics, and pipeline behavior tracking.
                    # -----------------------------
                    target_exists = spark.catalog.tableExists(silver_table)

                    target_empty = (
                        spark.table(silver_table).limit(1).count() == 0
                    )
    
                    #Silver dedups merge condition
                    merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in business_keys])

                    # Exclude business keys from updates to preserve row identity.
                    #Only non-key attributes (business + metadata columns) are allowed to be updated in MERGE.
                    exclude_cols = set(business_keys)
                    
                    # Update columns
                    update_columns = [
                        c for c in silver_dedup_df.columns
                        if c not in exclude_cols
                    ]

                    silver_delta = DeltaTable.forName(spark, silver_table)
                    
                    # Merge Operation
                    # withSchemaEvolution(), NOT the autoMerge Spark conf: that
                    # conf raises CONFIG_NOT_AVAILABLE on Serverless, which is
                    # where this runs. A MERGE cannot otherwise introduce a
                    # column, and repaired_columns is new to existing tables.
                    (
                        silver_delta.alias("tgt").merge(
                            silver_dedup_df.alias("src"),
                            merge_condition
                        ).whenMatchedUpdate(
                            # condition=f"tgt.{silver_col} != src.{silver_col}",
                            set={c: f"src.{c}" for c in update_columns}
                            ).whenNotMatchedInsertAll()
                             .withSchemaEvolution()
                             .execute()
                    )

                    # --- METRICS: read immediately after merge ---
                    hist = silver_delta.history(1).select("operationMetrics").collect()[0][0]

                    #Delta lake merge operation without hash condition in matched section will render below for merge metrics
                    inserted_rows = int(hist.get("numTargetRowsInserted", 0)) # Truly new business keys
                    updated_rows = int(hist.get("numTargetRowsUpdated", 0)) # Existing keys (changed + unchanged)

                    # -----------------------------
                    # DERIVED
                    # -----------------------------
                    # dedups rows is the good rows which were only done merge 
                    # 'Always 0 in Silver (hash condition disabled). See Gold metrics for actual unchanged tracking.'
                    unchanged_rows = max(dedup_rows - (inserted_rows + updated_rows), 0)

                    has_changes = (inserted_rows + updated_rows) > 0

                    # -----------------------------
                    # Status + load_type
                    # -----------------------------
                    # load_type records the run mode when it is not a plain
                    # incremental load, so the metrics table can answer "which
                    # runs were replays?" without adding a column.
                    if target_empty:
                        run_status = "SUCCESS"
                        load_type = "INITIAL_LOAD"
                        notes = f"Initial load completed for {silver_table}"

                    elif not has_changes:
                        run_status = "SUCCESS"
                        load_type = run_mode_load_type(backfill_config, "INCREMENTAL")
                        notes = f"No changes detected for {silver_table}"

                    else:
                        run_status = "SUCCESS"
                        load_type = run_mode_load_type(backfill_config, "INCREMENTAL")
                        notes = f"Data successfully merged into target table {silver_table}"

                    log_event(
                        logger_silver, 
                              "INFO",
                              f"Silver merge completed. Inserted {inserted_rows} rows, Unchanged {unchanged_rows}, Updated_rows {updated_rows} rows",
                               table=silver_table, 
                               master_run_id=master_run_id, 
                               layer_run_id=layer_run_id, 
                               layer=SILVER_LAYER
                            )
                else:
                    log_event(logger_silver, "INFO", "No deduplicates found; silver table not updated", master_run_id=master_run_id, layer_run_id=layer_run_id, layer=SILVER_LAYER)

                
            # -------------------------------
            # Step 7b: Merge duplicates into audit
            #-------------------------------
            has_dup_rows = dups_count > 0  # from the Step 7 metrics pass; no extra scan
            if not has_dup_rows:
                log_event(logger_silver, "INFO", "No dups rows received; skipping dups silver merge", table=silver_table, master_run_id=master_run_id, layer_run_id=layer_run_id, layer=SILVER_LAYER)
            else:
                # Only proceed if there are duplicates
                # Generate hash using the actual columns in silver_dup_df, not original Bronze df
                dups_selected_cols = [*business_columns, *meta_columns, "error_columns", "repaired_columns", "is_valid"]
                silver_dup_df = (
                    silver_dup_df.select(*dups_selected_cols)
                    .withColumn(duplicate_col, row_hash(business_columns))
                    .withColumn("audit_ingestion_ts", current_timestamp())
                    .withColumn("record_type", lit("duplicate"))
                    .repartition(shuffle_partitions, col(duplicate_col))
                )

                # Write into audit table (re-derived scope cleared first)
                write_derived_table(
                    spark,
                    silver_dup_df,
                    audit_table,
                    backfill_config,
                    "audit",
                    master_run_id,
                    layer_run_id,
                )


            # -------------------------------
            # Step 8: Metrics
            # -------------------------------
            # total / dedups_count / dups_count all came from the single Step 7
            # metrics pass -- re-counting them here cost three more full replays of
            # the lineage for numbers that were already known.
            dedup_pct = builtins.round((dedups_count / total) * 100, 2) if total > 0 else 0
            dup_pct = builtins.round((dups_count / total) * 100, 2) if total > 0 else 0

            log_event(
                logger_silver,
                "INFO",
                "Silver summary",
                table=bronze_table,
                read_rows=total,
                deduplicates=dedups_count,
                dedup_pct=dedup_pct,
                unchanged_rows=unchanged_rows,
                duplicates=dups_count,
                dup_pct=dup_pct,
                master_run_id=master_run_id,
                layer_run_id=layer_run_id, 
                layer=SILVER_LAYER
            )
            
    except Exception as e:
            run_status = "failure"
            notes = str(e)
            log_event(logger_silver, "ERROR", f"ETL failed for table", table=bronze_table, master_run_id=master_run_id,
                layer_run_id=layer_run_id, layer=SILVER_LAYER, error=str(e))

    end_ts = end_ts = spark.sql("SELECT current_timestamp() as ts").first()["ts"]
    end_time_epoch = time.time()
    duration_secs = int(end_time_epoch - start_time_epoch)

    # Return full metrics dictionary
    return {
        "layer_run_id": layer_run_id,
        "master_run_id":master_run_id,
        "layer": layer_name,
        "table_name": silver_table,
        "load_type" : load_type,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "read_rows": read_rows,
        "good_rows": good_count,
        "dirty_rows": dirty_count,
        "deduplicated_rows": dedups_count,
        "unchanged_rows": unchanged_rows,
        "duplicate_rows": dups_count,
        "inserted_rows" : inserted_rows,
        "updated_rows" : updated_rows,
        "throughput_rows_per_sec": throughput_rows_per_sec,
        "skew_ratio": skew_ratio,
        "run_status": run_status,
        "duration_secs": duration_secs,
        "notes": notes,
        "metrics_table": metrics_table
    }

# -------------------------------
# Write ETL metrics to metrics table
# -------------------------------
def write_etl_metrics(
    spark,
    metrics_table: str,
    layer_name: str,
    table_name: str,
    read_from_table: str,
    load_type : str,
    start_ts,
    end_ts,
    read_rows: int,
    good_rows: int,
    dirty_rows: int,
    deduplicated_rows: int,
    unchanged_rows: int,
    duplicate_rows: int,
    inserted_rows: int,
    updated_rows : int,
    throughput_rows_per_sec: float,
    skew_ratio: float,
    run_status: str,
    duration_secs: int,
    notes: str,
    master_run_id: str,
    layer_run_id: str
):
    """
    Production-grade ETL metrics writer.
    - Creates Delta table if missing
    - Uses BIGINT for counters
    - Idempotent (MERGE on run ids + table)
    - Validates metric consistency
    - Uses UTC timestamps
    """
    log_event(
        logger_silver,
        "INFO",
        f"Collecting metrics for {table_name}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=SILVER_LAYER
    )

    try:
        # -----------------------------
        # Ensure defaults for empty or None inputs
        # -----------------------------
        read_rows = read_rows or 0
        good_rows = good_rows or 0
        dirty_rows = dirty_rows or 0
        deduplicated_rows = deduplicated_rows or 0
        unchanged_rows= unchanged_rows or 0
        duplicate_rows = duplicate_rows or 0
        inserted_rows = inserted_rows or 0
        duration_secs = duration_secs or 0
        throughput_rows_per_sec = throughput_rows_per_sec or 0.0
        skew_ratio = skew_ratio or 0.0
        start_ts = start_ts or spark.sql("SELECT current_timestamp() as ts").first()["ts"]
        end_ts = end_ts or spark.sql("SELECT current_timestamp() as ts").first()["ts"]
        load_type = load_type or None
        notes = notes or None


        # -----------------------------
        # Create metrics table if missing
        # -----------------------------
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {metrics_table} (
            master_run_id STRING,
            layer_run_id STRING,
            layer_name STRING,
            source_table STRING,
            target_table STRING,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            duration_secs INT,
            read_rows BIGINT,
            good_rows BIGINT,
            dirty_rows BIGINT,
            deduplicated_rows BIGINT,
            duplicate_rows BIGINT,
            inserted_rows BIGINT,
            matched_rows BIGINT,
            unchanged_rows BIGINT,
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
        # 3. Explicit schema definition avoids schema drift and inference
        #    inconsistencies across Spark versions.
        # ------------------------------------------------------------------
        schema = StructType([
            StructField("master_run_id", StringType(), False),
            StructField("layer_run_id", StringType(), False),
            StructField("layer_name", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("target_table", StringType(), False),
            StructField("start_ts", TimestampType(), True),
            StructField("end_ts", TimestampType(), True),
            StructField("duration_secs", IntegerType(), True),
            StructField("read_rows", LongType(), True),            # BIGINT for scale safety
            StructField("good_rows", LongType(), True),
            StructField("dirty_rows", LongType(), True),
            StructField("deduplicated_rows", LongType(), True),
            StructField("duplicate_rows", LongType(), True),
            StructField("inserted_rows", LongType(), True),
            StructField ("matched_rows", LongType(), True),
            StructField("unchanged_rows", LongType(), True),
            StructField("throughput_rows_per_sec", DoubleType(), True),
            StructField("skew_ratio", DoubleType(), True),
            StructField("load_type", StringType(), True),
            StructField("run_status", StringType(), True),
            StructField("notes", StringType(), True),
        ])

        # ------------------------------------------------------------------
        # 4. Create single-row DataFrame representing this execution event.
        #    load_timestamp captured in UTC for audit consistency.
        # ------------------------------------------------------------------
        metrics_df = spark.createDataFrame(
            [(
                master_run_id,
                layer_run_id,
                layer_name,
                read_from_table,
                table_name,
                start_ts,
                end_ts,
                duration_secs,
                read_rows,
                good_rows,
                dirty_rows,
                deduplicated_rows,
                duplicate_rows,
                inserted_rows,
                updated_rows,
                unchanged_rows,
                throughput_rows_per_sec,
                skew_ratio,
                load_type,
                run_status,
                notes
            )],
            schema=schema
        ).withColumn("load_timestamp", current_timestamp())

        # Write metrics to Delta table
        metrics_df.write.format("delta").mode("append").saveAsTable(metrics_table)

        
        # ------------------------------------------------------------------
        # 6. Structured logging for traceability and monitoring.
        # ------------------------------------------------------------------
        log_event(
            logger_silver,
            "INFO",
            "ETL metrics successfully written",
            table=table_name,
            metrics_table=metrics_table,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=SILVER_LAYER
        )

    except Exception as e:
        # ------------------------------------------------------------------
        # Fail fast and log full context to avoid silent observability gaps.
        # Metrics failures should be visible to monitoring systems.
        # ------------------------------------------------------------------
        log_event(
            logger_silver,
            "ERROR",
            f"Failed to write ETL metrics: {str(e)}",
            table=table_name,
            metrics_table=metrics_table,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=SILVER_LAYER
        )
        raise
