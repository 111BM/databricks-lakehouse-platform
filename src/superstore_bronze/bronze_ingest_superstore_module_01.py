
"""
==============================================================
Module: Bronze Ingest – Superstore Raw Data

Purpose:
    Ingests raw Superstore CSV/Parquet files into a Bronze Delta table
    incrementally, enriching them with metadata, and preparing for downstream
    entity splits. Fully compatible with serverless Databricks clusters.

Key Features:
1. Incremental Ingestion:
    - Uses Databricks Auto Loader with `availableNow` trigger.
    - AUTO-DETECTS initial vs incremental runs by checking checkpoint existence.
    - Initial run (no checkpoint): loads all existing files in source path.
    - Incremental runs (checkpoint exists): processes only new files added since last run.
    - No manual configuration needed - automatically adapts per environment.
    - Supports schema evolution for new columns without breaking ingestion.

2. Metadata Enrichment:
    - Adds ingestion timestamp (`bronze_ingestion_ts`), ingestion date (`ingestion_date`),
      source file path, size, modification time, and sanitized column names.
    - Enables auditability, reproducibility, and downstream partition pruning.

3. Partitioning & Query Optimization:
    - Partitioned by `ingestion_date` for cost-efficient serverless queries.
    - Column sanitization ensures valid Delta column names.

4. Observability:
    - Structured JSON logging with `run_id` and row counts.
    - Captures ingestion start/end, batch-level info, and warnings/errors.

5. Serverless & Cost Optimization:
    - Avoids full table scans.
    - Incremental batch collection ensures minimal compute usage.
    - Safe to re-run without duplicating data.

Best Practices / Notes:
- Avoid frequent `.count()` calls on large tables; prefer async metrics.
- Ensure checkpoint locations are durable and unique per pipeline run.
- Column sanitization prevents Delta write errors from invalid names.
- Designed for seamless integration into orchestrated pipelines.
==============================================================
"""
# -----------------------------
# PySpark DataFrame core
# Base abstraction for distributed data processing
# -----------------------------
from pyspark.sql import DataFrame

# -----------------------------
# PySpark SQL functions
# Used for transformations, parsing, and column operations
# -----------------------------
from pyspark.sql.functions import (
    current_timestamp,   # adds ingestion timestamps
    col,                 # column reference
    element_at,          # array/map extraction
    split,               # string splitting
    to_date,             # date conversion
    lit,                 # literal values
    expr                 # SQL expression, for the backfill exclusion predicate
)

# -----------------------------
# Standard Python libraries
# Used for regex handling and system-level operations
# -----------------------------
import re
import os

# -----------------------------
# Custom logging utilities
# Used for pipeline observability, debugging, and audit logging
# -----------------------------
from superstore_logger import get_superstore_logger, log_event

# -----------------------------
# Platform constants
# Defines medallion architecture layers (Bronze, Silver, Gold)
# -----------------------------
from superstore_platform_constants import BRONZE_LAYER

# -----------------------------
# Platform configuration utilities
# Used for resolving schemas and table naming conventions
# -----------------------------
from superstore_platform_config import (
    get_bronze_schema,
    table,
    get_env
)
from superstore_backfill_utils import (  # backfill
    BACKFILL_WINDOW_MATCHED_NOTHING,
    backfill_exclusion_predicate,
    classify_backfill_scope,
    get_bronze_backfill_config,
)

# -----------------------------
# Logger Setup for Bronze Ingestion
# -----------------------------
# Initialize logger to capture events in the Bronze ingestion pipeline
logger_bronze_ingest = get_superstore_logger("bronze_ingest_superstore_module_01")


# -----------------------------
# Checkpoint Detection Helper
# -----------------------------
# Detects if this is the initial run by checking if checkpoint location exists
def is_initial_run(checkpoint_location: str) -> bool:
    """
    True when the Auto Loader stream has never committed against this checkpoint.

    A missing OR empty directory both mean "first run" - an empty directory is a
    real case (a prior run created the path but never committed, or the path was
    pre-created), and treating it as incremental would silently ingest 0 rows.
    
    Arguments:
        checkpoint_location: Path to the checkpoint directory.
    
    Returns:
        True if checkpoint doesn't exist or is empty (initial run), 
        False if it exists and has content (incremental run).
    """
    return not os.path.isdir(checkpoint_location) or not os.listdir(checkpoint_location)


# -----------------------------
# Column Sanitization Function
# -----------------------------
# This function ensures that all column names are sanitized to conform with Delta table standards:
# - Lowercase conversion
# - Replacement of spaces with underscores
# - Removal of special characters
# - Prefix added if the column name doesn't start with a letter
def sanitize_column(name: str) -> str:
    """
    Sanitize column names by converting to lowercase, replacing spaces with underscores,
    and removing any non-alphanumeric characters. If the column doesn't start with a letter,
    prepend "col_" to avoid errors in Delta tables.

    Arguments:
        name: The original column name.

    Returns:
        Sanitized column name.
    """
    name = name.lower().replace(
        " ", "_"
    )  # Replace spaces with underscores and convert to lowercase
    name = re.sub(r"[^a-z0-9_]", "", name)  # Remove any non-alphanumeric characters
    if not re.match(r"^[a-z]", name):  # Ensure the column starts with a letter
        name = f"col_{name}"  # Add a "col_" prefix if it doesn't start with a letter
    return name


# -----------------------------
# Bronze Ingestion - Incremental Data Processing
# -----------------------------
# This function is responsible for ingesting raw CSV files from CloudFiles into the Bronze Delta table
# incrementally, enriching them with metadata, renaming columns, and partitioning by ingestion date.
# It supports schema evolution to accommodate changes in the input data.


def bronze_ingest_incremental(
    spark,
    raw_source_file_path: str,
    schema_location: str,
    checkpoint_location: str,
    column_rename_map: dict,
    metadata_columns: list,
    table_name: str,
    master_run_id: str,
    layer_run_id: str,
    backfill_config: dict = None  # backfill
) -> DataFrame:
    """
    Ingests raw CSV data into the Bronze Delta table incrementally. The function adds necessary metadata columns,
    sanitizes column names, and writes the enriched data into the Delta table, partitioned by ingestion date.

    Arguments:
        spark: Spark session object.
        raw_source_file_path: Path to the raw data files (CSV/Parquet).
        schema_location: Location for schema tracking and evolution.
        checkpoint_location: Location for Spark checkpointing to maintain state.
        column_rename_map: Dictionary mapping raw column names to sanitized/standardized names.
        metadata_columns: List of columns already included as metadata (to exclude from renaming).
        table_name: Target Delta table name for storing the Bronze data.

    Returns:
        DataFrame: A DataFrame containing only the newly ingested rows for downstream processing.
    """
    log_event(
        logger_bronze_ingest,
        "INFO",
        "Bronze ingestion started",
        source_path=raw_source_file_path,
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=BRONZE_LAYER
    )

    # Get backfill settings for Auto Loader.
    # Only 'backfill' re-reads existing files; replay and full_refresh never
    # reach this module because the orchestrator exits before ingestion.
    if backfill_config and backfill_config["mode"] == "backfill":
        include_existing, checkpoint_suffix = get_bronze_backfill_config(backfill_config)

        # Update checkpoint location for backfill
        if checkpoint_suffix:
            checkpoint_location = checkpoint_location + checkpoint_suffix

        log_event(
            logger_bronze_ingest,
            "INFO",
            "Bronze backfill mode detected",
            run_mode=backfill_config["mode"],
            include_existing_files=include_existing,
            checkpoint_suffix=checkpoint_suffix,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )
    else:
        # AUTO-DETECT: Initial run vs incremental run
        # Initial run (no checkpoint) → include_existing=True (load all existing files)
        # Incremental run (checkpoint exists) → include_existing=False (only new files)
        initial_run = is_initial_run(checkpoint_location)
        include_existing = initial_run
        
        log_event(
            logger_bronze_ingest,
            "INFO",
            "Auto-detected run mode",
            initial_run=initial_run,
            include_existing_files=include_existing,
            checkpoint_exists=not initial_run,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )

    # Integration tests seed the raw file BEFORE the pipeline starts, so the seed
    # is "pre-existing" at stream start. With includeExistingFiles=False Auto Loader
    # would skip it (0 rows ingested). The isolated integration_test env must
    # backfill the seed; dev/qa/prod keep incremental (new-files-only) behavior.
    if get_env() == "integration_test":
        include_existing = True

    # -----------------------------
    # Step 2: Read stream from CloudFiles using Auto Loader
    # -----------------------------
    # Using Databricks Auto Loader to read raw CSV data incrementally. Only new files are processed
    # based on the `availableNow` trigger, and schema evolution is enabled to handle new columns.
    df_stream = (
        spark.readStream.format("cloudFiles")
        .option(
            "cloudFiles.format", "csv"
        )  # Process CSV files, can be changed to Parquet or other formats
        .option("header", "true")  # First row contains headers
        .option(
            # AUTO-DETECT mode: initial run (no checkpoint) = true (load existing files),
            # incremental runs (checkpoint exists) = false (only new files).
            # integration_test overrides to true. Backfill config can also override.
            "cloudFiles.includeExistingFiles", str(include_existing).lower()
        )
        .option(
            "cloudFiles.schemaLocation", schema_location
        )  # Schema evolution support
        .option(
            "cloudFiles.schemaEvolutionMode", "addNewColumns"
        )  # Allow schema changes
        .load(raw_source_file_path)  # Raw source data path
    )
    # -----------------------------
    # Step 3: Add Metadata Columns
    # -----------------------------
    # Enrich the DataFrame with metadata such as ingestion timestamp, file details, and custom metadata.
    # These columns are useful for auditing, partitioning, and ensuring downstream processes can be optimized.
    df_stream = (
        df_stream.withColumn(
            "bronze_ingestion_ts", current_timestamp()
        )  # Localize the timestamp
        .withColumn(
            "ingestion_date", to_date(col("bronze_ingestion_ts"))
        )  # Extract the date for partitioning
        .withColumn(
            "source_file_path", col("_metadata.file_path")
        )  # Store the source file path
        .withColumn(
            "source_file_name", element_at(split(col("_metadata.file_path"), "/"), -1)
        )  # Extract file name
        .withColumn(
            "source_file_size_bytes", col("_metadata.file_size")
        )  # Track file size in bytes
        .withColumn(
            "source_file_modification_time", col("_metadata.file_modification_time")
        )  # Track last modified time
    )

    # -----------------------------
    # Step 4: Rename & Sanitize Columns
    # -----------------------------
    # Rename columns as per the mapping provided and sanitize remaining columns to ensure Delta compatibility.
    # This step ensures that the column names follow best practices, such as lowercase and removing invalid characters.
    for raw_col, clean_col in column_rename_map.items():
        if raw_col in df_stream.columns:
            df_stream = df_stream.withColumnRenamed(
                raw_col, clean_col
            )  # Rename columns as specified

    # Sanitize remaining columns that were not part of the rename map or metadata
    for c in df_stream.columns:
        if (
            c not in column_rename_map.values()
            and c not in metadata_columns
            and not c.startswith("source_file")
        ):
            df_stream = df_stream.withColumnRenamed(
                c, sanitize_column(c)
            )  # Apply sanitization to remaining columns

    
    # Apply the date window for a backfill.
    #
    # This filters source_file_modification_time, NOT ingestion_date. The two
    # look interchangeable and are not: ingestion_date is derived from
    # bronze_ingestion_ts, which is stamped current_timestamp() a few lines
    # above, on every read. Re-reading a file therefore re-stamps it with
    # today, so filtering ingestion_date against a historical window matches
    # nothing and the write receives an empty stream — a backfill that silently
    # ingests zero rows while reporting success.
    #
    # source_file_modification_time comes from Auto Loader's _metadata and is a
    # property of the file itself, so it is stable across re-reads and is what
    # "when did this data arrive" actually means for a file source.
    if backfill_config and backfill_config["mode"] == "backfill":
        start_date = backfill_config["start_date"].strftime("%Y-%m-%d")
        end_date = backfill_config["end_date"].strftime("%Y-%m-%d")

        df_stream = df_stream.filter(
            (to_date(col("source_file_modification_time")) >= lit(start_date)) &
            (to_date(col("source_file_modification_time")) <= lit(end_date))
        )

        log_event(
            logger_bronze_ingest,
            "INFO",
            "Backfill window applied to Bronze stream (source_file_modification_time)",
            run_mode=backfill_config["mode"],
            start_date=start_date,
            end_date=end_date,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )

    # -----------------------------
    # Step 4b: Exclude files Bronze has already ingested (backfill only)
    # -----------------------------
    # A backfill exists to fetch what is MISSING in a window, not to reload the
    # window -- reloading is what full_refresh is for. Without this, it reloads.
    #
    # Measured in dev 2026-08-20: a backfill over 2026-07-27 took
    # Superstore_12-02-2026.csv from 505 rows to 1,010, an exact duplicate of a
    # file that had landed weeks earlier. The run reported SUCCESS.
    #
    # The cause is structural, not accidental: the per-window checkpoint that
    # correctly protects the incremental one also guarantees Auto Loader has no
    # memory of any file in the window, so every in-window file looks new. The
    # write is a plain append with no deduplication.
    #
    # Keyed on name AND modification time. A vendor re-exporting the same
    # filename with new content is a real event, and skipping it on name alone
    # would drop genuine data -- the opposite failure, and a worse one.
    backfill_scope = None
    if backfill_config and backfill_config["mode"] == "backfill":
        already_in_window = 0
        exclusion = None

        if spark.catalog.tableExists(table_name):
            target = spark.table(table_name)
            already_ingested = [
                (r["source_file_name"], r["source_file_modification_time"])
                for r in target.select(
                    "source_file_name", "source_file_modification_time"
                ).distinct().collect()
            ]
            exclusion = backfill_exclusion_predicate(already_ingested)

            already_in_window = (
                target.where(
                    (to_date(col("source_file_modification_time")) >= lit(start_date))
                    & (to_date(col("source_file_modification_time")) <= lit(end_date))
                )
                .select("source_file_name")
                .distinct()
                .count()
            )

        if exclusion:
            df_stream = df_stream.filter(expr(exclusion))

        # How many files the window matches AT SOURCE, which is a different
        # population from what Bronze already holds -- and the two are what make
        # the classification meaningful. binaryFile is used purely as a file
        # lister: only path and modificationTime are selected, so no content is
        # materialised.
        files_in_window = (
            spark.read.format("binaryFile")
            .load(raw_source_file_path)
            .where(
                (to_date(col("modificationTime")) >= lit(start_date))
                & (to_date(col("modificationTime")) <= lit(end_date))
            )
            .count()
        )

        # Distinguishes "the gap is already filled" (healthy) from "you asked for
        # a range that never had data" (a typo, far more often than a fact).
        # Both ingest zero rows and both used to report success identically --
        # the same distinction as SOURCE_DRAINED vs NO_DATA_ANYWHERE in
        # superstore_source_acquisition.
        backfill_scope = classify_backfill_scope(files_in_window, already_in_window)

        log_event(
            logger_bronze_ingest,
            "WARNING" if backfill_scope == BACKFILL_WINDOW_MATCHED_NOTHING else "INFO",
            f"Backfill scope {backfill_scope}: window {start_date}..{end_date} "
            f"matches {files_in_window} source file(s), {already_in_window} of "
            f"which Bronze already holds and will NOT re-read. A backfill fetches "
            f"what is missing; reloading known data is full_refresh.",
            run_mode="backfill",
            backfill_scope=backfill_scope,
            files_in_window=files_in_window,
            files_already_ingested=already_in_window,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )

    # -----------------------------
    # Step 5: Write Stream to Bronze Delta Table
    # -----------------------------
    # The data is written incrementally to the Delta table. Partitioning is done by ingestion date
    # to optimize querying and storage, especially for large datasets.
    query = (
        df_stream.writeStream.format("delta")
        .outputMode("append")  # Append the data to the existing table
        .option(
            "checkpointLocation", checkpoint_location
        )  # Ensure checkpointing for fault tolerance
        .option("mergeSchema", "True")  # Support schema evolution for new columns
        .trigger(availableNow=True)  # Process the data as soon as it's available
        .partitionBy(
            "ingestion_date"
        )  # Partition data by ingestion date for optimized querying
        .toTable(table_name)  # Write to the Delta table
    )

    # Wait for the streaming query to complete and process the data
    query.awaitTermination()

    log_event(
        logger_bronze_ingest,
        "INFO",
        "Bronze ingestion completed",
        table=table_name,
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=BRONZE_LAYER
    )