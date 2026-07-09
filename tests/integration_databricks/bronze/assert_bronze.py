# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — Bronze layer assertions
# MAGIC
# MAGIC Bronze runs as a single pipeline task but has two modules; this notebook
# MAGIC validates both:
# MAGIC
# MAGIC - **Module 01 — Ingestion**: source CSV → `superstore_raw` (OBT).
# MAGIC   Lossless ingest, column renames, metadata enrichment.
# MAGIC - **Module 02 — Entity split**: `superstore_raw` → entity tables.
# MAGIC   Column contract, provenance, row-count preservation.
# MAGIC
# MAGIC Runs against the isolated `integration_test_bronze` schema after the real
# MAGIC pipeline has executed.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/tests/integration_databricks/_helpers")
from assertion_helpers import check, count, count_or_zero, columns, table_exists, finalize, CATALOG, ENV, BRONZE, SILVER, GOLD, QUARANTINE, AUDIT, METRICS, SEED_ROW_COUNT, BUSINESS_KEYS
from pyspark.sql.functions import col

RAW = f"{BRONZE}.superstore_raw"

# Configured entity columns (must match configs/.../superstore_bronze_config.yaml)
ENTITY_COLUMNS = {
    "customers": ["customer_id", "customer_name", "segment", "country",
                  "state", "city", "postal_code", "region"],
    "products": ["product_id", "product_name", "category", "sub_category"],
    "orders": ["order_id", "customer_id", "order_date", "ship_date", "ship_mode"],
    "sales": ["order_id", "product_id", "sales", "quantity", "discount", "profit"],
}

# A column that must NOT appear in each entity (guards against over-selection)
FOREIGN_COLUMN = {
    "customers": "sales",
    "products": "order_date",
    "orders": "profit",
    "sales": "segment",
}

METADATA_COLUMNS = ["bronze_ingestion_ts", "ingestion_date", "source_file_name"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 01 — Ingestion (`superstore_raw`)

# COMMAND ----------

# DBTITLE 1,Raw ingestion is lossless
raw_rows = None
if check(f"{RAW} exists", table_exists(RAW)):
    raw_rows = count(RAW)
    check("raw row count equals seed row count (nothing lost/duplicated on ingest)",
          raw_rows == SEED_ROW_COUNT, f"raw={raw_rows}, seed={SEED_ROW_COUNT}")

# COMMAND ----------

# DBTITLE 1,Column renames applied (Customer ID -> customer_id, etc.)
raw_cols = columns(RAW)
for expected in ["customer_id", "order_id", "product_id", "order_date", "ship_date"]:
    check(f"raw has renamed column '{expected}'", expected in raw_cols)
for original in ["Customer ID", "Order ID", "Product ID"]:
    check(f"raw does not keep original name '{original}'", original not in raw_cols)

# COMMAND ----------

# DBTITLE 1,Metadata enrichment present and populated
for meta in METADATA_COLUMNS:
    check(f"raw has metadata column '{meta}'", meta in raw_cols)
if table_exists(RAW):
    for meta in METADATA_COLUMNS:
        if meta in raw_cols:
            nulls = spark.table(RAW).filter(col(meta).isNull()).count()
            check(f"raw metadata '{meta}' has no nulls", nulls == 0, f"null rows={nulls}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 02 — Entity split (entity tables)

# COMMAND ----------

# DBTITLE 1,Entity tables exist and are non-empty
for entity in ENTITY_COLUMNS:
    fqn = f"{BRONZE}.{entity}"
    if check(f"entity '{entity}' table exists", table_exists(fqn)):
        c = count(fqn)
        check(f"entity '{entity}' is non-empty", bool(c), f"rows={c}")

# COMMAND ----------

# DBTITLE 1,Column contract — each entity has its configured columns, and no foreign ones
for entity, expected_cols in ENTITY_COLUMNS.items():
    fqn = f"{BRONZE}.{entity}"
    if not table_exists(fqn):
        continue
    cols = columns(fqn)
    missing = [c for c in expected_cols if c not in cols]
    check(f"entity '{entity}' has all configured columns", not missing,
          f"missing={missing}")
    # over-selection guard
    foreign = FOREIGN_COLUMN[entity]
    check(f"entity '{entity}' did NOT pull foreign column '{foreign}'",
          foreign not in cols)

# COMMAND ----------

# DBTITLE 1,Provenance — entity rows carry source metadata (traceable to raw)
for entity in ENTITY_COLUMNS:
    fqn = f"{BRONZE}.{entity}"
    if not table_exists(fqn):
        continue
    cols = columns(fqn)
    check(f"entity '{entity}' carries provenance (source_file_name)",
          "source_file_name" in cols)

# COMMAND ----------

# DBTITLE 1,Row-count preservation — entity split neither drops nor duplicates
# Bronze does NO data-quality filtering (that's Silver), so each entity table
# should carry the same grain/row count as the raw OBT.
if raw_rows is not None:
    for entity in ENTITY_COLUMNS:
        fqn = f"{BRONZE}.{entity}"
        if not table_exists(fqn):
            continue
        ec = count(fqn)
        check(f"entity '{entity}' row count preserved from raw", ec == raw_rows,
              f"entity={ec}, raw={raw_rows}")

# COMMAND ----------

finalize("BRONZE")
dbutils.notebook.exit("BRONZE_ASSERTIONS_PASSED")
