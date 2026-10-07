"""
==============================================================
Module: Silver to Gold ETL Pipeline with SCD2 and Delta Lake Operations

Purpose:
    This module is designed to transform data from a Silver Delta table to a Gold Delta table in a Databricks Delta Lake environment.
    It handles the following operations:
    - Incrementally loads new data from the Silver table into the Gold table based on the `silver_ingestion_ts`.
    - Applies Slowly Changing Dimension Type 2 (SCD2) logic for capturing historical data changes, ensuring accurate tracking of data changes over time.
    - Performs an idempotent merge from Silver to Gold using Delta Lake's `MERGE` operation, ensuring no data duplication and maintaining data integrity.
    - Leaves OPTIMIZE and VACUUM to Unity Catalog Predictive Optimization.
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
    - Change detection by row hash: unchanged rows are never rewritten.
    - OPTIMIZE and VACUUM are handled by Unity Catalog Predictive Optimization, which is
      enabled at the metastore level; this module runs no table maintenance of its own.

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
- The pipeline can be modified for other types of slowly changing dimensions or different Delta tables as needed.

==============================================================
"""

# -------------------------------
# Standard Python Libraries
# -------------------------------
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
    spark_partition_id,   # used for partition-level skew analysis
    to_date,
    when,                # conditional substitution of untrusted attributes
    array_contains       # test membership of Silver's repaired_columns flags
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
# Custom Framework Imports
# -------------------------------
from superstore_logger import get_superstore_logger, log_event  # centralized logging framework
from superstore_platform_constants import GOLD_LAYER            # constant to enforce Gold layer naming consistency
from superstore_backfill_utils import (                            # run-mode support
    get_incremental_with_backfill,
    run_mode_load_type
)

# -----------------------------
# log_event Setup for gold dimensions
# -----------------------------
# Initialize log_event to capture events in the gold dimensions pipeline
logger_gold_dimensional = get_superstore_logger("superstore_gold_dimension_framework")


# The clock a windowed run selects on. Bronze ingestion time, matching what
# Silver filters on and what the facts framework already used, so one window
# means one thing at every layer.
WINDOW_TS_COL = "bronze_ingestion_ts"


def apply_window_filter(df, start_date, end_date, window_ts_col: str = WINDOW_TS_COL):
    """
    Restrict a Silver frame to the run's window, deriving `ingestion_date` from
    `window_ts_col` because Silver tables do not carry that column.

    Pure: a DataFrame in, a DataFrame out. Extracted from
    get_incremental_silver_for_dims so the selection can be unit tested without
    a catalog -- it is the piece that silently selected nothing for two run
    modes, and no test could reach it while it was welded inside an I/O path.

    Bounds are inclusive at both ends, matching the Silver-side read.

    Bounds are formatted to "YYYY-MM-DD" strings rather than passed as datetime
    objects, again matching Silver. A naive datetime literal is converted from
    the DRIVER's timezone while the derived date is rendered in the SESSION
    timezone, so comparing against one could shift the window by a day whenever
    those differ -- reintroducing the very misalignment this function exists to
    remove. A date string has no timezone to convert.

    Args:
        df: Silver frame.
        start_date / end_date: window bounds (date or datetime).
        window_ts_col: timestamp column the window applies to.

    Returns:
        Filtered frame, with the derived `ingestion_date` column retained
        (downstream selects columns by name, so the extra column is inert).
    """
    start = start_date.strftime("%Y-%m-%d")
    end = end_date.strftime("%Y-%m-%d")

    return (
        df.withColumn("ingestion_date", to_date(col(window_ts_col)))
          .filter(
              (col("ingestion_date") >= lit(start)) &
              (col("ingestion_date") <= lit(end))
          )
    )


DIMENSION_PLACEHOLDER = "Unknown"


def substitute_untrusted_attributes(
    df,
    attribute_columns,
    placeholder: str = DIMENSION_PLACEHOLDER,
):
    """
    Replace dimension attributes that cannot be trusted with a descriptive
    placeholder — both those that are MISSING and those that are INVALID.

    Kimball's rule: a dimension attribute is never NULL. Nulls behave badly in
    group-bys (the bucket renders inconsistently or vanishes), in joins, and in
    every BI tool, and they push a COALESCE into each consumer. A descriptive
    token keeps the row usable and the aggregate honest.

    This is the second half of the severity-tier design. Silver decides which
    violations are fatal and records the rest in `repaired_columns` WITHOUT
    touching the value, so Silver stays diffable against Bronze. Gold fills the
    gap here, at the point the dimension is built -- so completeness is
    guaranteed by construction rather than by the tier config happening to stay
    in sync with the dimension config. 'Unknown' is also a presentation
    decision, and Gold is the presentation layer.

    Runs BEFORE the row hash is computed, so a repaired row and a later clean
    row hash differently and SCD2 records the enrichment as a genuine change.

    Only string columns are substituted; a numeric or date attribute is left
    alone rather than being coerced into a string placeholder.

    Business keys should not be passed in `attribute_columns`. They cannot be
    null in practice -- they are the fatal tier, so a null key is quarantined at
    Silver and never arrives -- but excluding them explicitly means this can
    never manufacture a dimension member out of a missing key.

    Args:
        df: Silver frame about to become dimension rows.
        attribute_columns: descriptive columns (entity columns minus keys).
        placeholder: token to substitute.

    Returns:
        Frame with nulls in those columns replaced.
    """
    string_cols = {
        f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
    }
    has_flags = "repaired_columns" in df.columns

    out = df
    for c in attribute_columns:
        if c not in string_cols:
            continue

        # Null is not the only untrustworthy state. A categorical violation like
        # segment='Premium' is PRESENT and wrong, so coalesce leaves it alone and
        # it reaches the dimension looking like a legitimate value. Silver already
        # named it in repaired_columns; substitute on that, not on null-ness.
        untrusted = col(c).isNull()
        if has_flags:
            untrusted = untrusted | array_contains(col("repaired_columns"), c)

        out = out.withColumn(c, when(untrusted, lit(placeholder)).otherwise(col(c)))

    return out


def get_incremental_silver_for_dims(
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
    Returns only new silver rows not yet ingested into gold dimension table.
    Enhanced with run-mode support - handles incremental, backfill, replay and
    full_refresh.

    Modes:
    - incremental:       Standard watermark-based processing (default)
    - backfill / replay: Reprocess the start_date..end_date window
    - full_refresh:      Reprocess all data

    Backfill and replay read identically here; they differ only at Bronze,
    which a replay skips entirely.
    
    Windowed runs and the choice of clock:
    - The Silver table has no ingestion_date column, so one is derived below.
    - It is derived from bronze_ingestion_ts, matching what Silver filters on
      and what the facts framework already used. The window then means one
      thing at every layer: "rows whose data was ingested to Bronze in this
      period".
    - This previously derived from silver_ingestion_ts, on the reasoning that
      SCD2 tracks dimension changes at the Silver layer. That reasoning
      conflated two separate questions -- when a version is *dated* (see
      docs/SCD2_VALIDITY_DATING.md) and which rows a run *re-derives* -- and
      made replay structurally impossible for dimensions: a replay always
      rewrites Silver with a timestamp of now, which can never fall inside a
      historical window. Silver updated, Gold silently did not.
      See docs/GOLD_WINDOW_ALIGNMENT.md.
    """
    
    log_event(
        logger_gold_dimensional,
        "INFO",
        f"Reading silver dimension table with run mode: {backfill_config.get('mode', 'incremental')}",
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
                logger_gold_dimensional,
                "INFO",
                f"Dimension table missing ingestion_date column - deriving from {WINDOW_TS_COL}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )

            start_date = backfill_config.get("start_date")
            end_date = backfill_config.get("end_date")

            log_event(
                logger_gold_dimensional,
                "INFO",
                f"Filtering dimension table by derived ingestion_date: {start_date} to {end_date}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=GOLD_LAYER
            )

            return apply_window_filter(df_silver, start_date, end_date)
    
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

# -----------------------------
def read_silver_table(
    spark,
    silver_table: str,
    gold_table: str,
    backfill_config: dict,  # backfill parameter
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

        # Read the Delta table into a DataFrame incrementally (with backfill support)
        silver_df = get_incremental_silver_for_dims(
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
    silver_df,
    dim_type: str,
    entity_columns,
    hash_column: str,
    master_run_id: str,
    layer_run_id: str,
    layer: str,
    natural_keys: list = None,
):
    """
    Prepares columns for Slowly Changing Dimension Type 2:
    - Sets effective_from and load timestamps
    - Ensures auditability and reproducibility of SCD2 process

    effective_from is when this version of the row was OBSERVED, and the one
    property it must have is that it ADVANCES between versions of the same
    entity. merge_into_gold_table_scd2 closes an old version with
    "src.effective_from - 1 SECOND", so a value that is constant across an
    entity's versions closes each row one second before its own start: a
    negative-length interval that no point-in-time query can ever match. The
    row exists, is_current is false, effective_to is populated - and the
    history is silently unusable.

    An earlier version of this function dated the customers dimension from the
    customer's first order date. That is a business date, but it is the same
    value for every version of a customer, which is exactly the failure above.

    silver_ingestion_ts is therefore used for every dimension. That is
    PROCESSING time, not business time: it records when the pipeline saw the
    state, not when the state changed. The Superstore source carries no change
    timestamp - nothing says when a customer's segment actually changed - so
    processing time is the most precise honest answer available. With a CDC
    source, effective_from would come from the commit timestamp instead.
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
        # -------------------------------
        # effective_from — when this state was observed
        # -------------------------------
        # One rule for every dimension. It must advance between versions of the
        # same entity or the merge closes a row before its own start; see the
        # docstring for why this is processing time rather than business time.
        # -------------------------------
        # Repairable attributes -> 'Unknown', before anything is hashed
        # -------------------------------
        # Silver flags a repairable violation without altering the value; the
        # dimension is where the gap gets filled, so a dimension row can never
        # be written with a null attribute. Must precede the hash below, or a
        # repaired row and a later clean row would not differ and SCD2 would
        # miss the enrichment.
        attribute_columns = [
            c for c in entity_columns if c not in set(natural_keys or [])
        ]
        silver_df = substitute_untrusted_attributes(silver_df, attribute_columns)

        df = silver_df.withColumn("effective_from", col("silver_ingestion_ts"))

        # Add gold load timestamp
        df = (
            df.withColumn(hash_column, sha2(concat_ws("||", *[coalesce(col(c), lit("")) for c in entity_columns]), 256))  # Generate SHA-256 hash for full row
            .withColumn("gold_ingestion_ts", current_timestamp())  # Add timestamp for tracking
        )

        # Select required columns
        cols_for_scd = entity_columns + [hash_column, "effective_from", "silver_ingestion_ts", "gold_ingestion_ts"]
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
):
    """
    Production-grade SCD2 merge for Delta Gold table:
    - Idempotent: updates effective_to and is_current safely
    - Tracks run_id for observability and debugging
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

        # Deliberately two statements, not one. A single MERGE cannot express
        # SCD2 here: the merge condition matches a source row against the
        # CURRENT target row, so once it matches, whenNotMatchedInsert never
        # fires for that key and the replacement version is never inserted.
        # Closing the old version and inserting the new one are therefore
        # separate steps -- which also makes each one safe to re-run alone.
        # Soft deletes are not a third branch of this merge either: they act
        # on rows absent from the source, and are handled by handle_soft_deletes.

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
    has_soft_deletes = soft_delete_df.limit(1).count() > 0

    if not has_soft_deletes == 0:
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
        f"Soft delete completed in {gold_tbl}",
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=layer
    )

# -----------------------------
# Metrics for Observability to metrics table
# -----------------------------
def collect_metrics(
    spark,
    repaired_df,
    merge_metrics, 
    is_initial_load,   
    gold_tbl: str,
    layer_name: str,
    silver_table: str,
    table_name: str,
    table_type: str,
    entity_id_column: str,
    metrics_table: str,
    master_run_id: str,
    layer_run_id: str,
    layer: str,
    start_ts,
    end_ts,
    duration_secs,
    backfill_config: dict = None,
):
    """
    Optimized metrics collection using Delta merge metrics

    backfill_config is optional and only affects load_type: it records the run
    mode when the run was not a plain incremental load, so the metrics table
    can answer "which runs were replays?" without adding a column.
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
    soft_deleted_rows = 0
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
            load_type = run_mode_load_type(backfill_config, "INCREMENTAL")
            notes = f"No changes detected for {gold_tbl}"
            
        # CASE 3.3: Real SCD2 changes occurred
        else:
            load_type = run_mode_load_type(backfill_config, "INCREMENTAL")
            notes = f"SCD2 changes applied to {gold_tbl}"
        
        run_status = "SUCCESS"

        # -----------------------------
        # Compute active and total rows 
        # -----------------------------
        gold_df = spark.table(gold_tbl)
        total_rows = gold_df.count() 
        active_rows =gold_df.filter(col("is_current") == True).count() 
        soft_deleted_rows=gold_df.filter(col("is_current") == False).count() 

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
