"""
==============================================================
Module: Silver Pure Transformations (Functional Core)
==============================================================

Purpose
-------
Holds the pure, side-effect-free transformation logic used by the
Bronze -> Silver ETL. "Pure" means: takes a DataFrame (+ params) and
returns a DataFrame. No reads, no writes, no Delta, no logging.

Why this module exists
----------------------
The main `superstore_silver_module.bronze_to_silver_prod` function mixes
business logic with I/O (Delta MERGE, table creation, metrics writes),
which makes it impossible to unit test. By extracting the pure logic here,
we can test the actual rules used in production with a local Spark session
in milliseconds — no Databricks, no cluster, no tables.

This is the "functional core, imperative shell" pattern: pure logic here,
the I/O orchestration stays in superstore_silver_module.

Imports only PySpark (no delta-spark), so it loads in a plain local venv.
==============================================================
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array,
    array_union,
    coalesce,
    col,
    concat_ws,
    expr,
    lit,
    regexp_replace,
    row_number,
    sha2,
    size,
    trim,
    when,
)
from pyspark.sql.types import DateType, StringType, TimestampType
from pyspark.sql.window import Window


def clean_string_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Trim whitespace and strip double-quotes from the given columns.

    Type-aware: string columns are cleaned directly; timestamp/date columns
    are cast to string, cleaned, then cast back (handles quoted CSV dates).
    Other types are left untouched.

    This is Step 2 ("clean columns") of bronze_to_silver_prod, extracted so
    it can be unit tested.
    """
    clean_df = df
    for column_name in columns:
        col_type = df.schema[column_name].dataType
        if isinstance(col_type, StringType):
            clean_df = clean_df.withColumn(
                column_name, regexp_replace(trim(col(column_name)), '"', '')
            )
        elif isinstance(col_type, (TimestampType, DateType)):
            clean_df = clean_df.withColumn(
                column_name,
                regexp_replace(trim(col(column_name).cast("string")), '"', '').cast(col_type),
            )
    return clean_df


def add_error_columns(
    df: DataFrame,
    business_columns: list,
    regex_cols: dict = None,
    categorical_allowed_vals: dict = None,
) -> DataFrame:
    """
    Add an "error_columns" array listing which validations each row failed.

    Rules applied (in order):
      1. Null check  -> column name added if value is null
      2. Regex check -> column name added if value does NOT match its pattern
      3. Categorical -> column name added if value not in allowed set

    A row with an empty (all-null) error_columns array passed every rule.
    This is the quarantine-routing core of the Silver layer, extracted from
    bronze_to_silver_prod so it can be unit tested.

    NOTE: the bespoke ship_date<order_date business rule stays in
    bronze_to_silver_prod (it depends on multi-format date parsing) and is
    appended to error_columns there before is_valid is computed.
    """
    regex_cols = regex_cols or {}
    categorical_allowed_vals = categorical_allowed_vals or {}

    error_exprs = [when(col(c).isNull(), lit(c)) for c in business_columns]
    out = df.withColumn("error_columns", array(*error_exprs))

    for c, regex in regex_cols.items():
        if c in business_columns:
            out = out.withColumn(
                "error_columns",
                when(
                    ~col(c).rlike(regex),
                    array_union(col("error_columns"), array(lit(c))),
                ).otherwise(col("error_columns")),
            )

    for c, allowed_vals in categorical_allowed_vals.items():
        if c in business_columns:
            out = out.withColumn(
                "error_columns",
                when(
                    ~col(c).isin(allowed_vals),
                    array_union(col("error_columns"), array(lit(c))),
                ).otherwise(col("error_columns")),
            )

    return out


def add_is_valid(df: DataFrame, error_col: str = "error_columns") -> DataFrame:
    """
    Add a boolean "is_valid" column: True when error_columns has no non-null
    entries (the row passed every data-quality rule). Good rows -> Silver,
    invalid rows -> quarantine.
    """
    return df.withColumn(
        "is_valid",
        size(expr(f"filter({error_col}, x -> x is not null)")) == 0,
    )


def row_hash(columns: list, sep: str = "||") -> Column:
    """
    Deterministic SHA-256 hash over the given columns (null-safe).

    This is the single source of truth for the hash formula used across the
    Silver layer for:
      - silver_<entity>_hash_id   (business-key change detection)
      - quarantine_<entity>_hash_id
      - duplicates_<entity>_hash_id

    Nulls are coalesced to "" so the hash is stable and never null. The
    separator avoids collisions between e.g. ("a","bc") and ("ab","c").

    Returns a Column expression: use as df.withColumn("h", row_hash([...])).
    """
    return sha2(concat_ws(sep, *[coalesce(col(c), lit("")) for c in columns]), 256)


def deduplicate_latest_wins(
    df: DataFrame,
    business_keys: list,
    order_col: str = "bronze_ingestion_ts",
):
    """
    Split rows into (winners, losers) for Silver deduplication.

    For each group of `business_keys`, the row with the most recent
    `order_col` wins (latest record wins). All other rows in the group are
    losers (duplicates) routed to the audit table.

    This is the exact logic `bronze_to_silver_prod` uses for its
    Window + row_number() deduplication, extracted so it can be tested.

    Args:
        df: Input DataFrame (already cleaned/validated).
        business_keys: Columns identifying a logical record.
        order_col: Timestamp column; highest value wins. Default
                   "bronze_ingestion_ts".

    Returns:
        (winners_df, losers_df) tuple. Neither contains the helper
        "row_num" column.
    """
    window_spec = Window.partitionBy(*business_keys).orderBy(col(order_col).desc())

    classified = df.withColumn("row_num", row_number().over(window_spec))

    winners = classified.filter(col("row_num") == 1).drop("row_num")
    losers = classified.filter(col("row_num") > 1).drop("row_num")

    return winners, losers
