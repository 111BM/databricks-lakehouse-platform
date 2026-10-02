# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — environment reset
# MAGIC
# MAGIC Empties the isolated `integration_test_*` schemas and removes the test
# MAGIC volume so each run starts clean.
# MAGIC
# MAGIC It drops the **tables and views inside** the schemas, not the schemas
# MAGIC themselves. Dropping the schemas also destroyed every grant on them, so
# MAGIC read access for a person had to be re-granted after every run. Schemas
# MAGIC are long-lived containers that hold grants; tables are what tests create
# MAGIC and destroy.
# MAGIC
# MAGIC Every failure **raises**. This used to print "(ok) could not drop ..."
# MAGIC and carry on, so a reset that could not clean up -- after an ownership
# MAGIC change, say -- would have let the suite run against the previous run's
# MAGIC tables and pass for the wrong reason.
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

dropped = 0
for s in SCHEMAS:
    schema = f"{CATALOG}.{ENV}_{s}"

    # A missing schema is fine: first run ever, or after a manual drop. The
    # pipeline's init task creates it.
    if not spark.catalog.databaseExists(schema):
        print(f"{schema}: absent, nothing to empty")
        continue

    # Views first: a view can depend on a table in the same schema.
    # isTemporary: session temp views are listed too, but are not in the schema.
    objects = [o for o in spark.catalog.listTables(schema) if not o.isTemporary]
    for obj in sorted(objects, key=lambda o: o.tableType != "VIEW"):
        kind = "VIEW" if obj.tableType == "VIEW" else "TABLE"
        spark.sql(f"DROP {kind} IF EXISTS {schema}.`{obj.name}`")   # raises on failure
        dropped += 1

    # Prove it, rather than trust the loop: any object type the loop does not
    # know about would otherwise survive and leak into the next run.
    left = [o.name for o in spark.catalog.listTables(schema) if not o.isTemporary]
    if left:
        raise RuntimeError(f"{schema} is not empty after reset: {left}")
    print(f"{schema}: emptied ({len(objects)} objects)")

# COMMAND ----------

# The whole volume tree: raw seed files, Auto Loader schema and checkpoint
# state. An absent path is fine; any other failure stops the suite.
try:
    dbutils.fs.rm(VOLUME_BASE, recurse=True)
    print(f"removed volume contents {VOLUME_BASE}")
except Exception as e:
    if "FileNotFoundException" not in str(e) and "does not exist" not in str(e).lower():
        raise

# COMMAND ----------

# The exit string is the only thing the Jobs API returns for a notebook task,
# so the count goes there rather than only in print output.
dbutils.notebook.exit(f"CLEANUP_DONE objects_dropped={dropped}")
