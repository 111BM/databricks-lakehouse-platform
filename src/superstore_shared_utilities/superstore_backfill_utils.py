"""
==============================================================
Module: Superstore Run Mode (Backfill / Replay) Utilities

Purpose:
    Turns the job's run parameters into a validated config that every
    layer reads, so that backfill, replay and dry-run behave the same
    way across Bronze, Silver and Gold.

The four run modes
------------------
    incremental   Watermark-based load of new data only. The scheduled
                  default; ~99% of runs.

    backfill      Data that was never loaded. Re-acquires from source, so
                  Bronze runs. Windowed by start_date/end_date.

    replay        Data already in Bronze, re-derived because the logic
                  changed. Bronze is SKIPPED entirely — replays never
                  contact the source system. Windowed.

    full_refresh  Replay with no window. Kept as a distinct mode, rather
                  than "replay with no dates", so that an unbounded
                  reprocess is always an explicit choice and never the
                  result of a forgotten parameter. Gated behind
                  allow_full_refresh.

The distinction that drives the design: backfill is missing data with the
same logic, replay is existing data with new logic. Only incremental and
backfill read from the source; replay and full_refresh stay inside the
lakehouse, which matters because a source system rarely still holds the
files six months later.

dry_run is orthogonal to all four. It is a modifier, not a mode: you dry
run *a backfill* or *a replay*. Every task that writes must honour it, or
it is worse than not having it — a dry run that writes while reporting
success creates false confidence exactly when an operator is being careful.

Usage:
    from superstore_backfill_utils import (
        get_backfill_config,
        get_incremental_with_backfill,
        reads_from_source,
        is_dry_run,
    )

    config = get_backfill_config(dbutils)
    df = get_incremental_with_backfill(
        spark, source_table, target_table, config,
        master_run_id, layer_run_id, layer
    )
==============================================================
"""

from datetime import datetime
from typing import Dict, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, max as spark_max
from pyspark.sql.types import StructType


from superstore_logger import get_superstore_logger, log_event

logger = get_superstore_logger("superstore_backfill_utils")


# The complete set of run modes. Anything outside this set is an operator
# error and must fail loudly: a mode that silently degrades to "incremental"
# means a typo runs a normal load while the operator believes a replay is in
# progress, and the job still reports success.
VALID_RUN_MODES = ("incremental", "backfill", "replay", "full_refresh")

# Modes that re-read the source system. Replay and full_refresh re-derive from
# data already in Bronze, so the Bronze tasks skip themselves for those.
SOURCE_READING_MODES = ("incremental", "backfill")

# Modes that select rows by a date window rather than a watermark.
WINDOWED_MODES = ("backfill", "replay")


def _read_widget(dbutils, name: str) -> Optional[str]:
    """
    Read a job parameter, treating "not set" and "set to empty" alike.

    Databricks pushes every declared job parameter to every notebook task, and
    the bundle declares empty defaults for start_date/end_date. An unset widget
    raises; an unset *bundle variable* arrives as "". Both mean "not provided".
    """
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return None

    if value is None:
        return None

    value = value.strip()
    return value or None


def get_backfill_config(
    dbutils,
    allow_full_refresh: bool = False,
    max_days: int = 365
) -> Dict:
    """
    Parse and validate the run parameters shared by every layer.

    Expected Parameters:
    - run_mode: 'incremental' (default) | 'backfill' | 'replay' | 'full_refresh'
    - start_date: 'YYYY-MM-DD' (required for backfill and replay)
    - end_date: 'YYYY-MM-DD' (optional, defaults to today)
    - dry_run: 'true' | 'false' (default false)

    Returns:
        Dict with:
        - mode: str                one of VALID_RUN_MODES
        - is_backfill: bool        True for anything other than incremental
        - reads_source: bool       True when Bronze should acquire and ingest
        - is_windowed: bool        True when start_date/end_date apply
        - start_date: datetime | None
        - end_date: datetime | None
        - dry_run: bool

    Raises:
        ValueError: unknown mode, missing/invalid dates, range too large, or
                    full_refresh without an explicit opt-in.
    """

    run_mode_raw = _read_widget(dbutils, "run_mode")
    start_date_str = _read_widget(dbutils, "start_date")
    end_date_str = _read_widget(dbutils, "end_date")

    dry_run_raw = _read_widget(dbutils, "dry_run")
    dry_run = (dry_run_raw or "").lower() == "true"

    # An absent parameter is not an error — a scheduled run sets nothing and
    # must load incrementally. A *present but unrecognised* one is an error.
    mode = (run_mode_raw or "incremental").lower()
    if mode not in VALID_RUN_MODES:
        raise ValueError(
            f"Unknown run_mode '{mode}'. Valid modes: {', '.join(VALID_RUN_MODES)}. "
            f"Refusing to run: an unrecognised mode previously fell back to "
            f"'incremental', which silently performs the wrong operation."
        )

    # full_refresh reprocesses everything and is the most expensive operation
    # the pipeline can perform. Reaching it takes a code change, not a parameter.
    if mode == "full_refresh" and not allow_full_refresh:
        raise ValueError(
            "full_refresh mode is disabled. Set allow_full_refresh=True if you really want this."
        )

    start_date = None
    end_date = None

    if mode in WINDOWED_MODES:
        if not start_date_str:
            raise ValueError(f"start_date is required when run_mode='{mode}'")

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid start_date format: '{start_date_str}'. Use YYYY-MM-DD")

        # Default end_date to today if not provided
        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid end_date format: '{end_date_str}'. Use YYYY-MM-DD")
        else:
            end_date = datetime.now()

        # Validate date range
        if start_date > end_date:
            raise ValueError(f"start_date ({start_date_str}) cannot be after end_date ({end_date})")

        # Safety check: prevent accidentally replaying years of data
        days_diff = (end_date - start_date).days
        if days_diff > max_days:
            raise ValueError(
                f"Date range too large: {days_diff} days. Maximum allowed: {max_days} days. "
                f"Set max_days parameter higher if this is intentional."
            )

        log_event(
            logger,
            "INFO",
            f"Run window validated for mode '{mode}'",
            run_mode=mode,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            days=days_diff,
            layer="RUN_MODE"
        )

    config = {
        "mode": mode,
        "is_backfill": mode != "incremental",
        "reads_source": mode in SOURCE_READING_MODES,
        "is_windowed": mode in WINDOWED_MODES,
        "start_date": start_date,
        "end_date": end_date,
        "dry_run": dry_run
    }

    log_event(
        logger,
        "INFO",
        f"Run mode resolved: {mode}" + (" (DRY RUN — no data will be written)" if dry_run else ""),
        run_mode=mode,
        dry_run=dry_run,
        reads_source=config["reads_source"],
        is_windowed=config["is_windowed"],
        layer="RUN_MODE"
    )

    return config


def reads_from_source(backfill_config: Dict) -> bool:
    """
    True when the Bronze tasks should acquire and ingest from the source.

    False for replay and full_refresh, which re-derive Silver and Gold from
    the Bronze data already held. Bronze exists so the source is never asked
    twice; re-acquiring for a replay is wasted work at best, and impossible
    once the source has aged the files out.
    """
    return backfill_config.get("reads_source", True)


def is_dry_run(dbutils) -> bool:
    """
    Read only the dry_run flag, for tasks that write but have no mode logic.

    The serving layer (marts, features, KPI views) rebuilds from Gold and does
    not care which mode produced it — but it does write, so it must not write
    during a dry run. This deliberately avoids get_backfill_config so that a
    serving task never raises on the full_refresh gate, which is enforced
    upstream where it belongs.
    """
    value = _read_widget(dbutils, "dry_run")
    return (value or "").lower() == "true"


def run_mode_load_type(backfill_config: Dict, default: str) -> str:
    """
    Metrics label for a run: the mode when it is not a plain incremental load.

    Lets the per-layer metrics tables answer "which runs were replays?" without
    adding a column — load_type already exists in all three metrics schemas.
    """
    mode = backfill_config.get("mode", "incremental") if backfill_config else "incremental"
    return default if mode == "incremental" else mode.upper()


def get_incremental_with_backfill(
    spark,
    source_table: str,
    target_table: str,
    backfill_config: Dict,
    master_run_id: str,
    layer_run_id: str,
    layer: str,
    ingestion_col: str = "bronze_ingestion_ts",
    date_partition_col: str = "ingestion_date"
) -> DataFrame:
    """
    Incremental read that also serves backfill, replay and full_refresh.

    Replaces get_incremental_bronze/silver/gold functions.

    Logic:
    - incremental:        Use watermark (max ingestion_ts from target)
    - backfill / replay:  Filter by date partition, ignore watermark
    - full_refresh:       Read all data, ignore watermark

    Backfill and replay read identically here. They differ upstream, at Bronze:
    a backfill re-acquires from source, a replay does not run Bronze at all.
    By the time Silver and Gold read, both mean "re-derive this window".

    Args:
        spark: SparkSession
        source_table: Fully qualified source table name
        target_table: Fully qualified target table name  
        backfill_config: Config from get_backfill_config()
        master_run_id: Master run ID for logging
        layer_run_id: Layer run ID for logging
        layer: Layer name (BRONZE_LAYER, SILVER_LAYER, etc.)
        ingestion_col: Timestamp column for watermark
        date_partition_col: Date column for partition filtering
    
    Returns:
        DataFrame with appropriate filter applied
    """
    
    mode = backfill_config["mode"]
    
    # Check source exists
    if not spark.catalog.tableExists(source_table):
        log_event(
            logger,
            "WARN",
            f"Source table '{source_table}' does not exist",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=layer
        )
        return spark.createDataFrame([], StructType([]))
    
    # Read source table
    source_df = spark.table(source_table)
    
    # MODE 1: Incremental (existing logic)
    if mode == "incremental":
        max_ingestion_ts = None
        if spark.catalog.tableExists(target_table):
            max_ingestion_ts_row = (
                spark.table(target_table)
                .agg(spark_max(ingestion_col).alias("max_ingest_ts"))
                .first()
            )
            max_ingestion_ts = max_ingestion_ts_row["max_ingest_ts"]
            
            log_event(
                logger,
                "INFO",
                f"Incremental: Max {ingestion_col} in target = {max_ingestion_ts}",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=layer
            )
        
        if max_ingestion_ts:
            return source_df.filter(col(ingestion_col) > max_ingestion_ts)
        else:
            log_event(
                logger,
                "INFO",
                f"Target '{target_table}' empty or doesn't exist. Processing all source data.",
                master_run_id=master_run_id,
                layer_run_id=layer_run_id,
                layer=layer
            )
            return source_df
    
    # MODE 2: Full Refresh
    if mode == "full_refresh":
        log_event(
            logger,
            "WARN",
            f"⚠️  FULL REFRESH: Processing ALL data from '{source_table}'",
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=layer
        )
        return source_df
    
    # MODE 3: Windowed reprocessing (backfill or replay)
    if mode in WINDOWED_MODES:
        start_date = backfill_config["start_date"]
        end_date = backfill_config["end_date"]

        filtered_df = source_df.filter(
            (col(date_partition_col) >= lit(start_date.strftime("%Y-%m-%d"))) &
            (col(date_partition_col) <= lit(end_date.strftime("%Y-%m-%d")))
        )

        log_event(
            logger,
            "INFO",
            f"{mode.capitalize()}: Filtering {source_table} by {date_partition_col}",
            run_mode=mode,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer=layer
        )

        return filtered_df

    return source_df


def get_bronze_backfill_config(backfill_config: Dict) -> Tuple[bool, Optional[str]]:
    """
    Generate Auto Loader config for Bronze.

    Only incremental and backfill reach Bronze — the orchestrator exits before
    ingestion for replay and full_refresh, so those modes never need an Auto
    Loader config and fall through to normal operation here.

    Returns:
        Tuple of (should_include_existing_files, checkpoint_suffix)
    """
    mode = backfill_config["mode"]

    if mode == "backfill":
        # A separate checkpoint per window, so re-reading files for one window
        # cannot disturb the incremental checkpoint the scheduled runs rely on.
        start = backfill_config["start_date"].strftime("%Y%m%d")
        end = backfill_config["end_date"].strftime("%Y%m%d")
        return True, f"_backfill_{start}_{end}"

    return False, None  # Normal operation


def validate_backfill_impact(
    spark,
    table_name: str,
    backfill_config: Dict,
    date_column: str = "ingestion_date"
) -> Dict:
    """
    Estimate impact of backfill before execution (dry-run analysis).
    
    Returns:
        Dict with:
        - total_rows_in_table: int
        - rows_to_process: int  
        - partitions_affected: list
        - estimated_runtime_mins: float
    """
    
    if not spark.catalog.tableExists(table_name):
        return {
            "total_rows_in_table": 0,
            "rows_to_process": 0,
            "partitions_affected": [],
            "estimated_runtime_mins": 0
        }
    
    df = spark.table(table_name)
    
    # For large tables, use approximate count
    try:
        total_rows = df.count()
    except:
        total_rows = -1  # Unknown
    
    # Apply filter based on mode
    if backfill_config["mode"] == "full_refresh":
        rows_to_process = total_rows
        try:
            partitions = df.select(date_column).distinct().limit(100).collect()
            partitions_affected = [str(row[date_column]) for row in partitions]
        except:
            partitions_affected = ["unknown"]
    
    elif backfill_config["mode"] in WINDOWED_MODES:
        start_date = backfill_config["start_date"]
        end_date = backfill_config["end_date"]
        
        filtered_df = df.filter(
            (col(date_column) >= lit(start_date.strftime("%Y-%m-%d"))) &
            (col(date_column) <= lit(end_date.strftime("%Y-%m-%d")))
        )
        try:
            rows_to_process = filtered_df.count()
        except:
            rows_to_process = -1
        
        try:
            partitions = filtered_df.select(date_column).distinct().collect()
            partitions_affected = [str(row[date_column]) for row in partitions]
        except:
            partitions_affected = ["unknown"]
    
    else:  # incremental
        rows_to_process = 0
        partitions_affected = []
    
    # Rough estimate: 10k rows per minute (adjust based on your cluster)
    estimated_runtime_mins = max(1, rows_to_process / 10000) if rows_to_process > 0 else 0

    return {
        "total_rows_in_table": total_rows,
        "rows_to_process": rows_to_process,
        "partitions_affected": partitions_affected,
        "estimated_runtime_mins": round(estimated_runtime_mins, 2)
    }


def validate_bronze_impact(
    dbutils,
    spark,
    raw_source_file_path: str,
    table_name: str,
    backfill_config: Dict
) -> Dict:
    """
    Dry-run impact for Bronze, which is measured in files rather than rows.

    Silver and Gold can preview by counting rows in a source table. Bronze has
    no such table yet — its input is a landing volume — so the meaningful
    preview is which files Auto Loader would read and how large they are.

    Returns:
        Dict with:
        - files_in_landing: int
        - bytes_in_landing: int
        - sample_files: list       up to 10 names, so the operator can eyeball them
        - existing_rows_in_bronze: int
    """

    files = []
    try:
        files = [f for f in dbutils.fs.ls(raw_source_file_path) if not f.name.endswith("/")]
    except Exception as exc:
        return {
            "files_in_landing": -1,
            "bytes_in_landing": -1,
            "sample_files": [],
            "existing_rows_in_bronze": -1,
            "error": f"Could not list '{raw_source_file_path}': {exc}"
        }

    existing_rows = 0
    if spark.catalog.tableExists(table_name):
        try:
            existing_rows = spark.table(table_name).count()
        except Exception:
            existing_rows = -1

    return {
        "files_in_landing": len(files),
        "bytes_in_landing": sum(getattr(f, "size", 0) or 0 for f in files),
        "sample_files": [f.name for f in files[:10]],
        "existing_rows_in_bronze": existing_rows
    }
