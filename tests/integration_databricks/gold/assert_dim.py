# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — Gold DIMENSION assertions
# MAGIC
# MAGIC Validates the SCD2 dimension framework output in the isolated
# MAGIC `integration_test_gold` schema: dimensions exist and are non-empty,
# MAGIC exactly one current row per business key, and no overlapping validity
# MAGIC ranges. Fact / referential-integrity checks live in `assert_fact`.

# COMMAND ----------

import sys
# Deployed _helpers location is passed in by the job (bundle-relative).
sys.path.append(dbutils.widgets.get("helpers_path"))
from assertion_helpers import check, count, count_or_zero, columns, table_exists, finalize, CATALOG, ENV, BRONZE, SILVER, GOLD, QUARANTINE, AUDIT, METRICS, SEED_ROW_COUNT, BUSINESS_KEYS

from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

DIMS = {"dim_customers": "customer_id", "dim_products": "product_id"}

# COMMAND ----------

# DBTITLE 1,Dimensions exist and are non-empty
for dim in DIMS:
    fqn = f"{GOLD}.{dim}"
    if check(f"{fqn} exists", table_exists(fqn)):
        c = count(fqn)
        check(f"{dim} is non-empty", bool(c), f"rows={c}")

# COMMAND ----------

# DBTITLE 1,SCD2 — exactly one current row per business key
for dim, key in DIMS.items():
    fqn = f"{GOLD}.{dim}"
    if not table_exists(fqn):
        continue
    violations = (
        spark.table(fqn)
        .filter(col("is_current") == True)
        .groupBy(key).count()
        .filter(col("count") != 1)
        .count()
    )
    check(f"{dim}: exactly one current row per {key}", violations == 0,
          f"keys with != 1 current row: {violations}")

# COMMAND ----------

# DBTITLE 1,SCD2 — no overlapping effective ranges per key
for dim, key in DIMS.items():
    fqn = f"{GOLD}.{dim}"
    if not table_exists(fqn):
        continue
    cols = columns(fqn)
    if not {"effective_from", "effective_to"}.issubset(cols):
        check(f"{dim} has effective_from/effective_to", False, "columns missing")
        continue
    w = Window.partitionBy(key).orderBy("effective_from")
    overlaps = (
        spark.table(fqn)
        .withColumn("prev_to", lag("effective_to").over(w))
        .filter(col("prev_to").isNotNull() & (col("effective_from") <= col("prev_to")))
        .count()
    )
    check(f"{dim}: no overlapping validity ranges", overlaps == 0, f"overlaps={overlaps}")

    # Non-overlapping is not the same as valid. A closed row whose effective_to
    # precedes its own effective_from covers no time at all, so it cannot
    # overlap anything - the check above passes while every point-in-time query
    # against that version returns nothing.
    inverted = (
        spark.table(fqn)
        .filter((col("is_current") == False) & (col("effective_to") <= col("effective_from")))
        .count()
    )
    check(f"{dim}: closed versions cover a valid interval", inverted == 0,
          f"inverted intervals={inverted}")

# COMMAND ----------

# DBTITLE 1,Severity tiers — the repairable row reached the dimension, substituted
# AA-10480 carries an invalid segment and region in the seed but a valid key. Under
# severity tiers it is no longer quarantined, so it must reach dim_customers -- and
# every attribute must be populated, because a dimension attribute is never NULL
# (docs/SEVERITY_TIERS.md). Silver keeps the offending values; Gold substitutes.
#
# This is the end-to-end half of the routing policy: assert_silver checks the row
# was kept and flagged, this checks it was rendered usable.
dc = f"{GOLD}.dim_customers"
if table_exists(dc):
    aa = spark.table(dc).filter((col("customer_id") == "AA-10480") & (col("is_current") == True))
    if check("dim_customers has a current row for the repairable AA-10480",
             aa.count() == 1, f"rows={aa.count()}"):
        row = aa.first()
        check("dim_customers substituted the invalid segment",
              row["segment"] == "Unknown", f"segment={row['segment']}")
        check("dim_customers substituted the invalid region",
              row["region"] == "Unknown", f"region={row['region']}")
        check("dim_customers kept the attributes that were valid",
              row["customer_name"] == "Bad Categorical", f"name={row['customer_name']}")

    # No dimension attribute may be null anywhere in the current set.
    attrs = ["customer_name", "segment", "country", "state", "city", "postal_code", "region"]
    present = [c for c in attrs if c in spark.table(dc).columns]
    nulls = spark.table(dc).filter(col("is_current") == True).filter(
        " OR ".join(f"{c} IS NULL" for c in present)
    ).count()
    check("no current dim_customers row has a null attribute", nulls == 0, f"nulls={nulls}")

# COMMAND ----------

# DBTITLE 1,Gold dimension metrics recorded for this run
m = f"{METRICS}.gold_layer_metrics"
if table_exists(m):
    mdf = spark.table(m)
    for dim in DIMS:
        got = mdf.filter(col("target_table").endswith(f"{ENV}_gold.{dim}")).count()
        check(f"gold metrics recorded for {dim}", got >= 1, f"rows={got}")

# COMMAND ----------

finalize("GOLD_DIM")
dbutils.notebook.exit("GOLD_DIM_ASSERTIONS_PASSED")
