# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — Step 4: Cleanup
# MAGIC
# MAGIC Drops the isolated `integration_test_*` schemas and removes the test
# MAGIC volume so each run starts clean and nothing lingers. Runs even if the
# MAGIC assertions failed (configured with `run_if: ALL_DONE` in the job).

# COMMAND ----------

CATALOG = "superstore_catalog"   # CONFIRM: matches var.catalog
ENV = "integration_test"

SCHEMAS = ["bronze", "silver", "gold", "metrics", "quarantine", "audit", "mart"]
VOLUME_BASE = "/Volumes/workspace/default/my_filestore_integration_test/"

# COMMAND ----------

for s in SCHEMAS:
    schema = f"{CATALOG}.{ENV}_{s}"
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        print(f"dropped {schema}")
    except Exception as e:
        print(f"(ok) could not drop {schema}: {e}")

# COMMAND ----------

try:
    dbutils.fs.rm(VOLUME_BASE, recurse=True)
    print(f"removed volume {VOLUME_BASE}")
except Exception as e:
    print(f"(ok) could not remove {VOLUME_BASE}: {e}")

# COMMAND ----------

dbutils.notebook.exit("CLEANUP_DONE")
