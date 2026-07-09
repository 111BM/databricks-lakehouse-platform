# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — SCD2 change detection & idempotency (second run)
# MAGIC
# MAGIC Runs after the SECOND incremental load (seed_v2). Verifies the dimension
# MAGIC framework correctly historizes a real change and does NOT churn on
# MAGIC unchanged data.
# MAGIC
# MAGIC - `CG-12520` (city changed Henderson -> Oakland): a new current version
# MAGIC   exists with the new city, and the previous version is closed.
# MAGIC - `DV-13045` (unchanged): still a single version — no phantom SCD2 row.
# MAGIC - Global: still exactly one current row per customer.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/tests/integration_databricks/_helpers")
from assertion_helpers import check, count, count_or_zero, columns, table_exists, finalize, CATALOG, ENV, BRONZE, SILVER, GOLD, QUARANTINE, AUDIT, METRICS, SEED_ROW_COUNT, BUSINESS_KEYS

from pyspark.sql.functions import col

DIM = f"{GOLD}.dim_customers"

# COMMAND ----------

# DBTITLE 1,Changed customer (CG-12520) — new version opened, old version closed
if check(f"{DIM} exists", table_exists(DIM)):
    dim = spark.table(DIM)
    cg = dim.filter(col("customer_id") == "CG-12520")

    total_versions = cg.count()
    check("CG-12520 now has >= 2 versions (history retained)", total_versions >= 2,
          f"versions={total_versions}")

    current = cg.filter(col("is_current") == True)
    check("CG-12520 has exactly one current version", current.count() == 1,
          f"current={current.count()}")

    if current.count() == 1 and "city" in dim.columns:
        cur_city = current.first()["city"]
        check("CG-12520 current version reflects the CHANGE (city = Oakland)",
              cur_city == "Oakland", f"city={cur_city}")

    # at least one closed (historical) version with effective_to populated
    closed = cg.filter((col("is_current") == False) & col("effective_to").isNotNull())
    check("CG-12520 previous version was closed (is_current=false, effective_to set)",
          closed.count() >= 1, f"closed={closed.count()}")

# COMMAND ----------

# DBTITLE 1,Unchanged customer (DV-13045) — NO phantom version (idempotency)
if table_exists(DIM):
    dv = spark.table(DIM).filter(col("customer_id") == "DV-13045")
    check("DV-13045 has exactly one version (unchanged data made no new version)",
          dv.count() == 1, f"versions={dv.count()}")
    check("DV-13045 remains current", dv.filter(col("is_current") == True).count() == 1)

# COMMAND ----------

# DBTITLE 1,Global SCD2 invariant still holds after the second run
if table_exists(DIM):
    violations = (
        spark.table(DIM)
        .filter(col("is_current") == True)
        .groupBy("customer_id").count()
        .filter(col("count") != 1)
        .count()
    )
    check("still exactly one current row per customer_id", violations == 0,
          f"keys with != 1 current row: {violations}")

# COMMAND ----------

finalize("SCD2_CHANGE")
dbutils.notebook.exit("SCD2_CHANGE_ASSERTIONS_PASSED")
