
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
    - Processes only new files; ignores previously ingested data.
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
# Used for transformations, parsing, aggregation, and column operations
# -----------------------------
from pyspark.sql.functions import (
    current_timestamp,   # adds ingestion timestamps
    col,                 # column reference
    element_at,          # array/map extraction
    split,               # string splitting
    to_date,             # date conversion
    lit,                 # literal values
    regexp_replace,      # string cleanup / normalization
    sum, avg, min, max   # aggregation functions
)

# -----------------------------
# PySpark data types
# Used for defining explicit schemas for DataFrames and tables
# -----------------------------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)

# -----------------------------
# Standard Python libraries
# Used for regex handling, unique IDs, and system-level operations
# -----------------------------
import re
import uuid
import sys

# -----------------------------
# Extend Python path for shared utilities
# Enables importing project-specific reusable modules in Databricks workspace
# -----------------------------
sys.path.append(
    "/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/src/superstore_shared_utilities"
)

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
    table
)
from superstore_backfill_utils import get_bronze_backfill_config # backfill

# -----------------------------
# Logger Setup for Bronze Ingestion
# -----------------------------
# Initialize logger to capture events in the Bronze ingestion pipeline
logger_bronze_ingest = get_superstore_logger("bronze_ingest_superstore_module_01")


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

    # Get backfill settings for Auto Loader
    if backfill_config and backfill_config["mode"] != "incremental":
        include_existing, checkpoint_suffix = get_bronze_backfill_config(backfill_config)
        
        # Update checkpoint location for backfill
        if checkpoint_suffix:
            checkpoint_location = checkpoint_location + checkpoint_suffix
        
        log_event(
            logger_bronze_ingest,
            "INFO",
            "Bronze backfill mode detected",
            include_existing_files=include_existing,
            checkpoint_suffix=checkpoint_suffix,
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=BRONZE_LAYER
        )
    else:
        include_existing = False

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
            "cloudFiles.includeExistingFiles", "False"
            # "cloudFiles.includeExistingFiles", 
            # str(include_existing)  # backfill

        )  # Read files that exist at the beginning, when true=scan and ingest all files and when false= ingest only files after ingest start igonre before stream or exist files
        .option(
            "cloudFiles.schemaLocation", schema_location
        )  # Schema evolution support
        .option(
            "cloudFiles.schemaEvolutionMode", "addNewColumns"
        )  # Allow schema changes
        .load(raw_source_file_path)  # Raw source data path
    )

    # if df_stream.limit(1).count() == 0:
    #     log_event(ogger_bronze_ingest, "INFO", "No new files detected since last run. Pipeline skipped.")
    #     raise SystemExit("No new data")

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
        # .withColumn(
        #     "raw_table_name", regexp_replace(col("source_file_name"), r"\.csv$", "")
        # )  # Clean file name for table name
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

    
    # Apply date filter for backfill
    if backfill_config and backfill_config["mode"] == "date_range":
        start_date = backfill_config["start_date"].strftime("%Y-%m-%d")
        end_date = backfill_config["end_date"].strftime("%Y-%m-%d")
        
        df_stream = df_stream.filter(
            (col("ingestion_date") >= lit(start_date)) &
            (col("ingestion_date") <= lit(end_date))
        )
        
        log_event(
            logger_bronze_ingest,
            "INFO",
            "Date filter applied to Bronze stream",
            start_date=start_date,
            end_date=end_date,
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