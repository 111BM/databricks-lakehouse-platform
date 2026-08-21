# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — environment reset
# MAGIC
# MAGIC Drops the isolated `integration_test_*` schemas and removes the test
# MAGIC volume so each run starts clean.
# MAGIC
# MAGIC Wired as `reset_environment`, the FIRST task in the suite — not the
# MAGIC last. There is no end-of-run cleanup: enforcing isolation at the start
# MAGIC gives the same guarantee while leaving the previous run's tables
# MAGIC available to query. A suite that deletes its own evidence can only be
# MAGIC debugged from whatever was anticipated in an exit string.
# MAGIC
# MAGIC The filename keeps its `04_` prefix so the git history stays traceable;
# MAGIC the number no longer reflects its position in the DAG.

# COMMAND ----------

CATALOG = "superstore_catalog"   # CONFIRM: matches var.catalog
ENV = "integration_test"

# Every schema the pipeline creates for an environment. Two were missing here
# (features, semantic_layer), so integration_test_features and
# integration_test_semantic_layer survived every run and accumulated in the
# catalog.
#
# Harmless while nothing asserts on them, and a trap the moment something does:
# a features assertion would read the PREVIOUS run's output rather than this
# run's seed, and pass for the wrong reason. That is the same failure this test
# suite exists to catch, hiding in the test suite's own cleanup.
#
# Keep in step with the schema list in superstore_catalog_and_schemas_init --
# a schema created there and not dropped here leaks the same way.
SCHEMAS = [
    "bronze",
    "silver",
    "gold",
    "metrics",
    "quarantine",
    "audit",
    "mart",
    "features",
    "semantic_layer",
]
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
