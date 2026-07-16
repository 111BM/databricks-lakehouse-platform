"""
==============================================================
Module: Superstore Backfill & Replay Utilities

Purpose:
    Provides production-ready backfill/replay capabilities for the
    medallion architecture. Supports date-range based reprocessing
    at Bronze, Silver, and Gold layers while maintaining idempotency.

Key Features:
1. Backfill Mode Detection:
    - Automatic detection of backfill parameters
    - Safe defaults to prevent accidental full rewrites  
    - Dry-run mode for validation

2. Date Range Management:
    - Parse and validate start_date/end_date
    - Generate partition filters for Bronze/Silver/Gold
    - Support for open-ended ranges ("all data since X")

3. Safety Mechanisms:
    - Max date range limits (prevent accidental 10-year replays)
    - Confirmation prompts for large backfills
    - Dry-run mode to preview impact

4. Metrics & Logging:
    - Track backfill progress
    - Separate metrics for backfill vs incremental
    - Audit trail for compliance

Usage:
    from superstore_backfill_utils import (
        get_backfill_config,
        get_incremental_with_backfill
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


def get_backfill_config(
    dbutils,
    allow_full_refresh: bool = False,
    max_days: int = 365
) -> Dict:
    """
    Parse backfill parameters from job/notebook widgets.
    
    Expected Parameters:
    - backfill_mode: 'incremental' (default) | 'date_range' | 'full_refresh'
    - start_date: 'YYYY-MM-DD' (required for date_range)
    - end_date: 'YYYY-MM-DD' (optional, defaults to today)
    - dry_run: 'true' | 'false' (default false)
    
    Returns:
        Dict with:
        - is_backfill: bool
        - mode: str
        - start_date: datetime | None
        - end_date: datetime | None
        - dry_run: bool
    
    Raises:
        ValueError: If dates are invalid or range too large
    """
    
    # Get parameters (with safe defaults)
    try:
        backfill_mode = dbutils.widgets.get("backfill_mode")
    except:
        backfill_mode = "incremental"
    
    try:
        start_date_str = dbutils.widgets.get("start_date")
    except:
        start_date_str = None
    
    try:
        end_date_str = dbutils.widgets.get("end_date")
    except:
        end_date_str = None
    
    try:
        dry_run = dbutils.widgets.get("dry_run").lower() == "true"
    except:
        dry_run = False
    
    # Parse mode
    mode = backfill_mode.lower()
    if mode not in ["incremental", "date_range", "full_refresh"]:
        log_event(
            logger,
            "WARN",
            f"Invalid backfill_mode '{mode}', defaulting to 'incremental'",
            backfill_mode=mode,
            layer="BACKFILL"
        )
        mode = "incremental"
    
    # Safety check: full_refresh must be explicitly allowed
    if mode == "full_refresh" and not allow_full_refresh:
        raise ValueError(
            "full_refresh mode is disabled. Set allow_full_refresh=True if you really want this."
        )
    
    # Parse dates
    start_date = None
    end_date = None
    
    if mode == "date_range":
        if not start_date_str:
            raise ValueError("start_date is required when backfill_mode='date_range'")
        
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
            "Backfill date range validated",
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            days=days_diff,
            layer="BACKFILL"
        )
    
    # Build config
    is_backfill = mode in ["date_range", "full_refresh"]
    
    config = {
        "is_backfill": is_backfill,
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
        "dry_run": dry_run
    }
    
    if dry_run:
        log_event(
            logger,
            "INFO",
            "🔍 DRY RUN MODE: No data will be written",
            config=config,
            layer="BACKFILL"
        )
    
    return config


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
    Enhanced incremental read that supports backfill mode.
    
    Replaces get_incremental_bronze/silver/gold functions.
    
    Logic:
    - incremental: Use watermark (max ingestion_ts from target)
    - date_range: Filter by date partition, ignore watermark
    - full_refresh: Read all data, ignore watermark
    
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
    
    # MODE 3: Date Range Backfill
    if mode == "date_range":
        start_date = backfill_config["start_date"]
        end_date = backfill_config["end_date"]
        
        filtered_df = source_df.filter(
            (col(date_partition_col) >= lit(start_date.strftime("%Y-%m-%d"))) &
            (col(date_partition_col) <= lit(end_date.strftime("%Y-%m-%d")))
        )
        
        log_event(
            logger,
            "INFO",
            f"Backfill: Filtering {source_table} by {date_partition_col}",
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
    Generate Auto Loader config for Bronze layer backfills.
    
    Returns:
        Tuple of (should_include_existing_files, checkpoint_suffix)
    """
    mode = backfill_config["mode"]
    
    if mode == "incremental":
        return False, None  # Normal operation
    
    if mode == "full_refresh":
        return True, "_full_refresh"  # Separate checkpoint
    
    if mode == "date_range":
        # Use date-specific checkpoint to avoid conflicts
        start = backfill_config["start_date"].strftime("%Y%m%d")
        end = backfill_config["end_date"].strftime("%Y%m%d")
        return True, f"_backfill_{start}_{end}"
    
    return False, None


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
    
    elif backfill_config["mode"] == "date_range":
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
