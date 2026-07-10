# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — Silver layer assertions
# MAGIC
# MAGIC Verifies the Bronze -> Silver transition on the isolated schema:
# MAGIC data-quality routing (the RIGHT rows quarantined), reconciliation
# MAGIC (nothing lost), deduplication (latest wins), hash integrity, and metrics.

# COMMAND ----------

import sys
# Deployed _helpers location is passed in by the job (bundle-relative).
sys.path.append(dbutils.widgets.get("helpers_path"))
from assertion_helpers import check, count, count_or_zero, columns, table_exists, finalize, CATALOG, ENV, BRONZE, SILVER, GOLD, QUARANTINE, AUDIT, METRICS, SEED_ROW_COUNT, BUSINESS_KEYS

from pyspark.sql.functions import col, array_contains



# DBTITLE 1,Silver tables exist, non-empty, no null business keys
for entity, keys in BUSINESS_KEYS.items():
    fqn = f"{SILVER}.{entity}"
    if not check(f"silver.{entity} exists", table_exists(fqn)):
        continue
    df = spark.table(fqn)
    check(f"silver.{entity} is non-empty", df.count() > 0)
    for k in keys:
        nulls = df.filter(col(k).isNull()).count()
        check(f"silver.{entity} has no null '{k}'", nulls == 0, f"nulls={nulls}")

# COMMAND ----------

# DBTITLE 1,Business keys are unique in Silver (dedup collapsed duplicates)
for entity, keys in BUSINESS_KEYS.items():
    fqn = f"{SILVER}.{entity}"
    if not table_exists(fqn):
        continue
    df = spark.table(fqn)
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    check(f"silver.{entity} business key is unique", total == distinct,
          f"rows={total}, distinct_keys={distinct}")

# COMMAND ----------

# DBTITLE 1,Reconciliation — nothing lost: bronze_entity == silver + quarantine + audit
for entity in BUSINESS_KEYS:
    bronze_c = count_or_zero(f"{BRONZE}.{entity}")
    silver_c = count_or_zero(f"{SILVER}.{entity}")
    quar_c = count_or_zero(f"{QUARANTINE}.{entity}_dirty")
    audit_c = count_or_zero(f"{AUDIT}.{entity}_duplicates")
    total = silver_c + quar_c + audit_c
    check(f"reconciliation {entity}: bronze == silver + quarantine + audit",
          bronze_c == total,
          f"bronze={bronze_c}, silver={silver_c}, quarantine={quar_c}, audit={audit_c}, sum={total}")

# COMMAND ----------

# DBTITLE 1,Routing identity — the specific dirty rows landed in quarantine
# null business key -> customers_dirty
q_cust = f"{QUARANTINE}.customers_dirty"
if check(f"{q_cust} exists", table_exists(q_cust)):
    qc = spark.table(q_cust)
    check("quarantine caught the null Customer ID row",
          qc.filter(col("customer_id").isNull()).count() >= 1)
    check("quarantine caught the invalid segment 'Premium'",
          qc.filter(col("segment") == "Premium").count() >= 1)
    # error_columns should name the failing rule
    if "error_columns" in qc.columns:
        check("quarantine error_columns names the segment rule",
              qc.filter(array_contains(col("error_columns"), "segment")).count() >= 1)

# ship_date < order_date -> orders_dirty
q_ord = f"{QUARANTINE}.orders_dirty"
if check(f"{q_ord} exists", table_exists(q_ord)):
    check("quarantine caught ship_date < order_date (order CA-2026-0005)",
          spark.table(q_ord).filter(col("order_id") == "CA-2026-0005").count() >= 1)

# COMMAND ----------

# DBTITLE 1,Deduplication — latest record wins, loser audited
sc = f"{SILVER}.customers"
if table_exists(sc):
    df = spark.table(sc)
    cg = df.filter(col("customer_id") == "CG-12520")
    check("silver.customers deduped CG-12520 to a single row", cg.count() == 1,
          f"rows={cg.count()}")
    # the surviving row is the newest (name carries 'UPDATED' in the seed's latest dup)
    if cg.count() == 1 and "customer_name" in df.columns:
        name = cg.first()["customer_name"]
        check("silver kept the LATEST CG-12520 version", "UPDATED" in (name or ""),
              f"customer_name={name}")

a_cust = f"{AUDIT}.customers_duplicates"
check("audit.customers_duplicates captured the losing duplicate(s)",
      count_or_zero(a_cust) >= 1, f"audit rows={count_or_zero(a_cust)}")

# COMMAND ----------

# DBTITLE 1,Hash integrity — silver hash id present, non-null, 64-char
for entity in BUSINESS_KEYS:
    fqn = f"{SILVER}.{entity}"
    if not table_exists(fqn):
        continue
    hash_col = f"silver_{entity}_hash_id"
    cols = columns(fqn)
    if hash_col in cols:
        df = spark.table(fqn)
        nulls = df.filter(col(hash_col).isNull()).count()
        check(f"silver.{entity} {hash_col} has no nulls", nulls == 0, f"nulls={nulls}")
        bad_len = df.filter("length(" + hash_col + ") != 64").count()
        check(f"silver.{entity} {hash_col} is 64-char SHA-256", bad_len == 0)

# COMMAND ----------

# DBTITLE 1,Metrics accuracy — a metrics row exists per entity for this run
m = f"{METRICS}.silver_layer_metrics"
if check(f"{m} exists", table_exists(m)):
    mdf = spark.table(m)
    for entity in BUSINESS_KEYS:
        got = mdf.filter(col("target_table").endswith(f"{ENV}_silver.{entity}")).count()
        check(f"silver metrics recorded for {entity}", got >= 1, f"rows={got}")

# COMMAND ----------

finalize("SILVER")
dbutils.notebook.exit("SILVER_ASSERTIONS_PASSED")
