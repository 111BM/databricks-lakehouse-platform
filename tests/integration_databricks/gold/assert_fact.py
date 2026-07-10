# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — Gold FACT assertions
# MAGIC
# MAGIC Validates the fact framework output in the isolated `integration_test_gold`
# MAGIC schema: facts exist and are non-empty, grain is unique, and every foreign
# MAGIC key resolves to a real dimension row (referential integrity). SCD2 checks
# MAGIC live in `assert_dim`.

# COMMAND ----------

import sys
# Deployed _helpers location is passed in by the job (bundle-relative).
sys.path.append(dbutils.widgets.get("helpers_path"))
from assertion_helpers import check, count, count_or_zero, columns, table_exists, finalize, CATALOG, ENV, BRONZE, SILVER, GOLD, QUARANTINE, AUDIT, METRICS, SEED_ROW_COUNT, BUSINESS_KEYS

from pyspark.sql.functions import col

# fact -> grain (unique key set)
FACTS = {
    "facts_orders": ["order_id"],
    "facts_sales": ["order_id", "product_id"],
}

# COMMAND ----------

# DBTITLE 1,Facts exist, non-empty, and grain is unique
for fact, grain in FACTS.items():
    fqn = f"{GOLD}.{fact}"
    if not check(f"{fqn} exists", table_exists(fqn)):
        continue
    df = spark.table(fqn)
    total = df.count()
    check(f"{fact} is non-empty", total > 0, f"rows={total}")
    if set(grain).issubset(df.columns):
        distinct = df.select(*grain).distinct().count()
        check(f"{fact} grain {grain} is unique", total == distinct,
              f"rows={total}, distinct={distinct}")

# COMMAND ----------

# DBTITLE 1,Referential integrity — every fact FK resolves to a dimension row
def orphan_check(fact, fact_key, dim, dim_key):
    fact_fqn, dim_fqn = f"{GOLD}.{fact}", f"{GOLD}.{dim}"
    if not (table_exists(fact_fqn) and table_exists(dim_fqn)):
        return
    fdf, ddf = spark.table(fact_fqn), spark.table(dim_fqn)
    if fact_key not in fdf.columns or dim_key not in ddf.columns:
        check(f"{fact}.{fact_key} / {dim}.{dim_key} present for FK check", False,
              "column missing — skipped")
        return
    orphans = (
        fdf.select(fact_key).distinct()
        .join(ddf.select(col(dim_key).alias(fact_key)).distinct(),
              on=fact_key, how="left_anti")
        .count()
    )
    check(f"referential integrity: {fact}.{fact_key} -> {dim}.{dim_key} (no orphans)",
          orphans == 0, f"orphan keys={orphans}")

orphan_check("facts_orders", "customer_id", "dim_customers", "customer_id")
orphan_check("facts_sales", "product_id", "dim_products", "product_id")

# COMMAND ----------

# DBTITLE 1,Gold fact metrics recorded for this run
m = f"{METRICS}.gold_layer_metrics"
if table_exists(m):
    mdf = spark.table(m)
    for fact in FACTS:
        got = mdf.filter(col("target_table").endswith(f"{ENV}_gold.{fact}")).count()
        check(f"gold metrics recorded for {fact}", got >= 1, f"rows={got}")

# COMMAND ----------

finalize("GOLD_FACT")
dbutils.notebook.exit("GOLD_FACT_ASSERTIONS_PASSED")
