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


def standardize_values(df: DataFrame, value_standardization: dict = None) -> DataFrame:
    """
    Translate known source dialects to the canonical vocabulary, BEFORE the
    data-quality rules run.

    Why this is not a data-quality repair
    -------------------------------------
    A source that writes "OFF" where the contract says "Office Supplies" has
    not sent bad data — it has sent the same fact in a different dialect. The
    correct value is fully recoverable, so translating it is *conformance*, and
    the row should reach Silver with its real category.

    Treating it as a DQ failure instead is what caused 50,264 products to be
    quarantined on `category` alone, which in turn orphaned 49,539 fact rows
    (~5% of revenue) from the marts — see docs/REFERENTIAL_COMPLETENESS.md.
    Substituting "Unknown" would have returned the revenue while destroying a
    category that was never actually unknown.

    Deliberately exact-match, not fuzzy
    ----------------------------------
    Only values listed in the mapping are translated. Anything else passes
    through untouched and still faces the categorical/regex rules, so an
    unrecognised value is rejected loudly rather than coerced into whichever
    canonical value looks closest. Adding a dialect is a config change, which
    is the same contract the rest of the DQ rules follow.

    No flag column is written. The mapping is deterministic, lossless and
    declared in config, and Bronze retains the value as received — unlike a
    repair, nothing is lost that a reader would need to know about.

    Args:
        df: Input DataFrame (already cleaned).
        value_standardization: {column: {source_value: canonical_value}}.

    Returns:
        DataFrame with mapped values replaced. Unmapped values unchanged.
    """
    if not value_standardization:
        return df

    out = df
    for column, mapping in value_standardization.items():
        if column not in out.columns or not mapping:
            continue

        expr_col = col(column)
        for source_value, canonical_value in mapping.items():
            expr_col = when(col(column) == lit(source_value), lit(canonical_value)).otherwise(expr_col)

        out = out.withColumn(column, expr_col)

    return out


def add_error_columns(
    df: DataFrame,
    business_columns: list,
    regex_cols: dict = None,
    categorical_allowed_vals: dict = None,
    severity: dict = None,
) -> DataFrame:
    """
    Record which validations each row failed, split by how serious the failure
    is.

    Rules applied (in order), unchanged:
      1. Null check  -> column name recorded if value is null
      2. Regex check -> column name recorded if value does NOT match its pattern
      3. Categorical -> column name recorded if value not in allowed set

    Two output arrays instead of one
    --------------------------------
      error_columns    -> FATAL violations. Still the quarantine-routing signal:
                          add_is_valid derives is_valid from this array alone.
      repaired_columns -> REPAIRABLE violations. Recorded, but the row proceeds
                          to Silver; Gold substitutes a value when it builds the
                          dimension.

    Why the split exists
    --------------------
    Every column was fatal, so a customer missing only a `postal_code` was
    quarantined whole, never reached `dim_customers`, and its facts contributed
    nothing to any mart while every upstream check passed (see
    docs/REFERENTIAL_COMPLETENESS.md). Quarantine should mean *this row cannot
    be trusted at all*, not *this row is imperfect*.

    Only business keys are fatal by default policy, but this function does not
    assume that: any column absent from `severity` is treated as FATAL, so an
    unclassified column keeps today's stricter behaviour rather than being
    silently downgraded to repairable.

    The value is NOT modified here. Silver stays diffable against Bronze, and
    no `_raw` preservation column is needed because nothing was overwritten.
    Substitution happens in the Gold dimension build, where completeness is
    guaranteed by construction and where 'Unknown' is a presentation decision
    rather than a cleansing one.

    NOTE: the bespoke ship_date<order_date business rule stays in
    bronze_to_silver_prod (it depends on multi-format date parsing) and is
    appended to error_columns there before is_valid is computed. It is
    therefore fatal, which is correct -- a shipment before its order is not a
    cosmetic defect.

    Args:
        severity: {column: "fatal" | "repairable"}. Unlisted -> "fatal".
    """
    regex_cols = regex_cols or {}
    categorical_allowed_vals = categorical_allowed_vals or {}
    severity = severity or {}

    fatal_cols = [c for c in business_columns if severity.get(c, "fatal") == "fatal"]
    repairable_cols = [c for c in business_columns if severity.get(c, "fatal") != "fatal"]

    def _record(out, target_col, columns):
        """Apply the three rules over `columns`, collecting names into target_col."""
        if columns:
            seed = array(*[when(col(c).isNull(), lit(c)) for c in columns])
        else:
            # No columns in this tier -- the common case for repaired_columns
            # when no severity map is configured. A bare array() is untyped in
            # Spark, so seed a single null instead: same shape as a row that
            # passed every rule, and filter(x is not null) still yields empty.
            seed = array(lit(None).cast("string"))

        out = out.withColumn(target_col, seed)

        for c, regex in regex_cols.items():
            if c in columns:
                out = out.withColumn(
                    target_col,
                    when(
                        ~col(c).rlike(regex),
                        array_union(col(target_col), array(lit(c))),
                    ).otherwise(col(target_col)),
                )

        for c, allowed_vals in categorical_allowed_vals.items():
            if c in columns:
                out = out.withColumn(
                    target_col,
                    when(
                        ~col(c).isin(allowed_vals),
                        array_union(col(target_col), array(lit(c))),
                    ).otherwise(col(target_col)),
                )

        return out

    out = _record(df, "error_columns", fatal_cols)
    out = _record(out, "repaired_columns", repairable_cols)

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


def content_hash(columns: list, sep: str = "||") -> Column:
    """
    Deterministic SHA-256 hash over the given columns, casting each to string
    first so the input may contain any type — including the array-typed
    `error_columns`, which `row_hash` cannot take.

    Kept separate from `row_hash` on purpose: `row_hash` is the published
    formula behind the persisted `*_hash_id` columns, and adding a cast there
    would change hashes already written to Silver.

    Returns a Column expression.
    """
    return sha2(
        concat_ws(sep, *[coalesce(col(c).cast("string"), lit("")) for c in columns]),
        256,
    )


def classify_duplicates(
    df: DataFrame,
    business_keys: list,
    order_col: str = "bronze_ingestion_ts",
    tiebreak_cols: list = None,
) -> DataFrame:
    """
    Attach a "row_num" column ranking rows within each `business_keys` group.
    row_num == 1 is the winner; row_num > 1 are duplicates destined for the
    audit table.

    Ordering, most significant first:
      1. fewest repaired attributes  (only when `repaired_columns` is present)
      2. most recent `order_col`
      3. content hash of `tiebreak_cols`

    Completeness ranks above recency
    --------------------------------
    Severity tiers let a row with missing attributes reach Silver instead of
    being quarantined. Ranked on arrival alone, such a row can beat a complete
    observation of the same entity, and Gold then substitutes 'Unknown' over a
    value that was present in the batch. Measured in dev: 81 of 99 'Unknown'
    regions had a real value in the audit table for the same customer.

    Preferring completeness costs nothing, because `order_col` carries no
    business meaning (see Determinism below) -- recency here was never evidence
    of truth, so there is nothing to trade away.

    This is the single source of truth for the dedup window.
    `deduplicate_latest_wins` splits on it, and `bronze_to_silver_prod` calls
    it directly so that the metrics aggregation and both output branches share
    one evaluation of the window instead of re-deriving it. Serverless forbids
    cache()/persist(), so re-deriving would mean replaying the whole upstream
    lineage.

    Determinism
    -----------
    `order_col` alone does not totally order a group. Duplicates that arrive in
    the same batch share one `bronze_ingestion_ts`, so the window ties and
    row_number() picks by whatever order the shuffle happened to produce — the
    same input could yield different Silver contents on a re-run. A content
    hash of `tiebreak_cols` is therefore appended as a secondary sort key,
    which totally orders any two rows that differ in content at all.

    This buys *reproducibility, not correctness*: the surviving row is stable
    across runs, but the hash carries no business meaning, so it is not a
    statement about which version is the true latest. That needs a change
    timestamp the source does not carry (see the deduplication-determinism and
    retroactive-history entries in the README backlog).

    Rows that are identical across `tiebreak_cols` still tie, and remain
    interchangeable by definition — either can win without changing the result.

    Args:
        df: Input DataFrame (already cleaned/validated).
        business_keys: Columns identifying a logical record.
        order_col: Timestamp column; highest value wins. Default
                   "bronze_ingestion_ts".
        tiebreak_cols: Columns hashed to break `order_col` ties. Defaults to
                       every column except the business keys and `order_col`
                       (which are constant within a group anyway).

    Returns:
        The input DataFrame plus a "row_num" integer column.
    """
    if tiebreak_cols is None:
        excluded = {*business_keys, order_col, "row_num"}
        tiebreak_cols = [c for c in df.columns if c not in excluded]

    order_by = []

    # Completeness outranks arrival. A row missing repairable attributes is a
    # poorer observation of the same entity, and letting it win overwrites known
    # values with 'Unknown' at Gold -- measured at 81 of 99 'Unknown' regions in
    # dev, i.e. the value existed and was discarded.
    #
    # This costs nothing epistemically: bronze_ingestion_ts carries no business
    # meaning here (the source has no change timestamp -- see the deduplication
    # and retroactive-history entries in the README backlog), so "latest
    # arrival" was never evidence of "current truth". Given two observations of
    # one entity and no way to order them in business time, prefer the one that
    # actually carries its attributes.
    if "repaired_columns" in df.columns:
        order_by.append(
            size(expr("filter(repaired_columns, x -> x is not null)")).asc()
        )

    order_by.append(col(order_col).desc())

    if tiebreak_cols:
        order_by.append(content_hash(tiebreak_cols).desc())

    window_spec = Window.partitionBy(*business_keys).orderBy(*order_by)

    return df.withColumn("row_num", row_number().over(window_spec))


def deduplicate_latest_wins(
    df: DataFrame,
    business_keys: list,
    order_col: str = "bronze_ingestion_ts",
    tiebreak_cols: list = None,
):
    """
    Split rows into (winners, losers) for Silver deduplication.

    For each group of `business_keys`, the row with the most recent
    `order_col` wins (latest record wins); ties on `order_col` are broken by a
    content hash so the split is reproducible. All other rows in the group are
    losers (duplicates) routed to the audit table.

    Thin wrapper over `classify_duplicates` (which owns the window logic and
    documents what the tiebreak does and does not guarantee).

    Args:
        df: Input DataFrame (already cleaned/validated).
        business_keys: Columns identifying a logical record.
        order_col: Timestamp column; highest value wins. Default
                   "bronze_ingestion_ts".
        tiebreak_cols: Columns hashed to break `order_col` ties. Defaults to
                       every column except the business keys and `order_col`.

    Returns:
        (winners_df, losers_df) tuple. Neither contains the helper
        "row_num" column.
    """
    classified = classify_duplicates(df, business_keys, order_col, tiebreak_cols)

    winners = classified.filter(col("row_num") == 1).drop("row_num")
    losers = classified.filter(col("row_num") > 1).drop("row_num")

    return winners, losers
