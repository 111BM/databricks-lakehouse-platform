"""Integration Test — Shared Assertion Helpers.

Imported (NOT %run) by each layer's assert notebook:

    import sys
    sys.path.append(dbutils.widgets.get("helpers_path"))
    from assertion_helpers import check, count, ..., finalize

This file is a PLAIN Python module — it must NOT start with the
`# Databricks notebook source` header, or the bundle deploys it as a notebook
and `from assertion_helpers import ...` fails with NotebookImportException.

Each layer notebook runs as its own job task, collects failures, and calls
finalize() to raise (and fail that task) if anything is wrong.
"""

from pyspark.sql import SparkSession

# The module is imported into a running notebook/job, so an active Spark
# session already exists; getOrCreate() returns it (a plain module has no
# auto-injected `spark` the way a notebook does).
spark = SparkSession.builder.getOrCreate()

# ---- Isolated environment config (CONFIRM matches databricks.yml / bronze config) ----
CATALOG = "superstore_catalog"
ENV = "integration_test"

BRONZE = f"{CATALOG}.{ENV}_bronze"
SILVER = f"{CATALOG}.{ENV}_silver"
GOLD = f"{CATALOG}.{ENV}_gold"
QUARANTINE = f"{CATALOG}.{ENV}_quarantine"
AUDIT = f"{CATALOG}.{ENV}_audit"
METRICS = f"{CATALOG}.{ENV}_metrics"

# Number of data rows written by 01_seed (used for lossless-ingestion checks)
SEED_ROW_COUNT = 9

# Entity -> business key(s)
BUSINESS_KEYS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "sales": ["order_id", "product_id"],
}

# ---- Assertion harness ----
_failures = []

def check(name, condition, detail=""):
    """Record a pass/fail. Never raises here — we report all, then finalize()."""
    status = "PASS" if condition else "FAIL"
    line = f"[{status}] {name}"
    if detail:
        line += f"  -- {detail}"
    print(line)
    if not condition:
        _failures.append(name if not detail else f"{name} ({detail})")
    return bool(condition)

def table_exists(fqn):
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:
        return False

def count(fqn):
    """Row count, or None if the table can't be read."""
    try:
        return spark.table(fqn).count()
    except Exception as e:
        print(f"  (could not read {fqn}: {e})")
        return None

def count_or_zero(fqn):
    """Row count, treating a missing table as 0 (e.g. no dirty rows -> no table)."""
    return count(fqn) or 0 if table_exists(fqn) else 0

def columns(fqn):
    try:
        return set(spark.table(fqn).columns)
    except Exception:
        return set()

def finalize(layer):
    """Raise if this layer had any failed check — fails the job task."""
    if _failures:
        raise AssertionError(
            f"[{layer}] integration checks FAILED ({len(_failures)}):\n  - "
            + "\n  - ".join(_failures)
        )
    print(f"\n[{layer}] all integration checks passed ✅")
