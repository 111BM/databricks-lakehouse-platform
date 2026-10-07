"""
==============================================================
Module: Liquid Clustering -- converge each table to the layout its config declares
==============================================================

Purpose
-------
Make a table's physical layout match the clustering keys declared in its config,
idempotently, on every run:

    table missing                         -> nothing to do (it is created later)
    clustered on the declared keys        -> nothing to do (every run after the first)
    not partitioned, other or no keys     -> ALTER TABLE ... CLUSTER BY (metadata only)
    Hive-partitioned                      -> convert, once (see below)

Why this exists
---------------
The Bronze and Gold dimension tables were Hive-partitioned (`partitionBy`). At
this data size (about 1M rows) partitioning only fragments files, and the Bronze
entities were partitioned on `bronze_ingestion_ts` -- a timestamp -- so every
run created a new partition of small files that nothing ever merged. Current
Databricks guidance for tables under about 1 TB is Liquid Clustering or no
layout at all. With Predictive Optimization enabled, clustered tables are
re-clustered in the background; nothing here schedules OPTIMIZE.

Converging from the pipeline, rather than from a one-off migration job, means
every environment migrates itself on its next run under the identity that owns
its tables (the service principal in qa and prod), and a fresh environment is
created clustered. There is no migration job left behind to become dead code.

Converting a partitioned table
------------------------------
Two routes, both verified on dev scratch tables on 2026-10-07:

  1. ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY
     In place, metadata only. Keeps the Delta table id, history and grants.
     This is the route that matters for `superstore_raw`, which Auto Loader
     streams into: its sink must keep the same table.
     It FAILS when a partition column is a TIMESTAMP -- the conversion must
     generate file statistics for the partition column and cannot for that
     type, and the setting that skips it is not available on serverless.

  2. CREATE OR REPLACE TABLE ... CLUSTER BY ... AS SELECT * FROM <itself>
     A rewrite. Keeps history (a new version) and grants, but the Delta table
     id CHANGES. Used only when route 1 refuses, and only for tables marked
     `allow_rewrite` -- the Bronze entities, which are written and read in
     batch, never as a stream.

Scope
-----
Layout only. This module never changes data: both conversions preserve every
row (verified: 1,010,456 rows before and after).
==============================================================
"""

from superstore_logger import log_event

# What converge_liquid_clustering did, recorded in logs and the notebook exit.
CLUSTERING_TABLE_MISSING = "TABLE_MISSING"
CLUSTERING_ALREADY_CLUSTERED = "ALREADY_CLUSTERED"
CLUSTERING_KEYS_SET = "KEYS_SET"
CLUSTERING_CONVERTED_IN_PLACE = "CONVERTED_IN_PLACE"
CLUSTERING_REWRITTEN = "REWRITTEN"

# Plans, from the pure decision function.
PLAN_NOTHING = "NOTHING"
PLAN_ALTER_CLUSTER_BY = "ALTER_CLUSTER_BY"
PLAN_CONVERT_PARTITIONED = "CONVERT_PARTITIONED"

# Liquid Clustering accepts at most four keys.
MAX_CLUSTERING_KEYS = 4

# The error the in-place conversion raises for a TIMESTAMP partition column.
_IN_PLACE_UNSUPPORTED = "REPLACE_PARTITIONED_BY_WITH_CLUSTER_BY_STATS_GENERATION_NOT_SUPPORTED"


def validate_cluster_columns(cluster_columns):
    """
    Reject a config that Liquid Clustering would reject, before touching a table.

    Raises ValueError for an empty list, more than four keys, or a duplicate.
    Returns the keys as a list.
    """
    keys = list(cluster_columns or [])
    if not keys:
        raise ValueError("cluster columns are empty; declare at least one key")
    if len(keys) > MAX_CLUSTERING_KEYS:
        raise ValueError(
            f"{len(keys)} cluster columns declared, Liquid Clustering allows at most "
            f"{MAX_CLUSTERING_KEYS}: {keys}"
        )
    if len(set(keys)) != len(keys):
        raise ValueError(f"duplicate cluster columns: {keys}")
    return keys


def missing_cluster_columns(cluster_columns, table_columns):
    """
    Keys declared in config that the table does not have, in declared order.

    Pure. Checked before any ALTER so a wrong key fails with the config's name
    for the problem, not Delta's DELTA_COLUMN_NOT_FOUND_IN_SCHEMA halfway
    through a conversion.
    """
    present = set(table_columns)
    return [c for c in cluster_columns if c not in present]


def plan_clustering(partition_columns, clustering_columns, desired_columns):
    """
    Decide what a table needs, from what DESCRIBE DETAIL reports.

    Pure, so the decision is unit tested without a table.

    Key ORDER matters: it is part of the declared layout, so a reordered list is
    a change.
    """
    if partition_columns:
        return PLAN_CONVERT_PARTITIONED
    if list(clustering_columns or []) == list(desired_columns):
        return PLAN_NOTHING
    return PLAN_ALTER_CLUSTER_BY


def _describe_layout(spark, table_name):
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    return list(detail["partitionColumns"] or []), list(detail["clusteringColumns"] or [])


def converge_liquid_clustering(
    spark,
    logger,
    table_name,
    cluster_columns,
    *,
    allow_rewrite,
    master_run_id,
    layer_run_id,
    layer,
):
    """
    Bring one table to Liquid Clustering on `cluster_columns`. Idempotent.

    allow_rewrite: whether a partitioned table that cannot be converted in place
        may be rewritten (new Delta table id). False for a table a stream writes
        into or reads from; True for batch-only tables.

    Returns one of the CLUSTERING_* outcomes. Raises on any failure: a layout
    change that half-happened must not be reported as success.
    """
    keys = validate_cluster_columns(cluster_columns)
    key_list = ", ".join(keys)

    if not spark.catalog.tableExists(table_name):
        return CLUSTERING_TABLE_MISSING

    missing = missing_cluster_columns(keys, spark.table(table_name).columns)
    if missing:
        raise ValueError(
            f"cluster columns {missing} declared for {table_name} are not columns of "
            f"that table; fix the table's cluster_by keys in its config"
        )

    partitions, clustering = _describe_layout(spark, table_name)
    plan = plan_clustering(partitions, clustering, keys)

    if plan == PLAN_NOTHING:
        return CLUSTERING_ALREADY_CLUSTERED

    if plan == PLAN_ALTER_CLUSTER_BY:
        spark.sql(f"ALTER TABLE {table_name} CLUSTER BY ({key_list})")
        outcome = CLUSTERING_KEYS_SET
    else:
        try:
            spark.sql(
                f"ALTER TABLE {table_name} REPLACE PARTITIONED BY WITH CLUSTER BY ({key_list})"
            )
            outcome = CLUSTERING_CONVERTED_IN_PLACE
        except Exception as e:
            if _IN_PLACE_UNSUPPORTED not in str(e) or not allow_rewrite:
                raise
            spark.sql(
                f"CREATE OR REPLACE TABLE {table_name} CLUSTER BY ({key_list}) "
                f"AS SELECT * FROM {table_name}"
            )
            outcome = CLUSTERING_REWRITTEN
        # The in-place conversion keeps the old partition column as the key;
        # set the declared keys if they differ.
        _, clustering = _describe_layout(spark, table_name)
        if clustering != keys:
            spark.sql(f"ALTER TABLE {table_name} CLUSTER BY ({key_list})")

    log_event(
        logger,
        "INFO",
        f"Liquid Clustering {outcome}: {table_name} CLUSTER BY ({key_list})"
        + (f", was PARTITIONED BY ({', '.join(partitions)})" if partitions else ""),
        table=table_name,
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=layer,
    )
    return outcome
