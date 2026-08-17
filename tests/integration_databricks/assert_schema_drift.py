# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — schema drift assertions
# MAGIC
# MAGIC The schema-drift detector reports `SCHEMA_STABLE` in every environment,
# MAGIC because no column has ever appeared or vanished — the source is a static
# MAGIC CSV. A detector that has only ever said "nothing to report" is
# MAGIC indistinguishable from one that *cannot* report, and this codebase has
# MAGIC already shipped that exact thing twice:
# MAGIC
# MAGIC - the freshness alert evaluated daily for days while structurally unable
# MAGIC   to return a value that could breach its own threshold
# MAGIC - the orphaned-fact counters read 0 permanently once severity tiers made
# MAGIC   the condition impossible, and nobody noticed they had gone blind
# MAGIC
# MAGIC So this suite makes the event happen. `02_seed_scd2_change` sends a
# MAGIC `Discount Reason` column the first seed did not, and these assertions
# MAGIC require the detector to have noticed.
# MAGIC
# MAGIC ## What each check would catch
# MAGIC
# MAGIC - **heartbeat**: one `RESCUED` row per pipeline run. A run missing its row
# MAGIC   means the detector did not execute — the failure a monitor is least
# MAGIC   likely to notice about itself, and the reason `record_schema_drift` is
# MAGIC   allowed to swallow its own exceptions.
# MAGIC - **fires**: `discount_reason` recorded as `NEW`. Without this the whole
# MAGIC   feature is unfalsifiable.
# MAGIC - **no false positives**: `row_id` must NOT be reported. It arrives in
# MAGIC   every file and no entity declares it, so before `ignored_source_columns`
# MAGIC   existed it would have been flagged on every run forever.
# MAGIC - **still dropped**: the new column must NOT have reached the entity
# MAGIC   tables. Detection was explicitly not supposed to change behaviour, and a
# MAGIC   detector that silently started widening the contract would be worse than
# MAGIC   none.

# COMMAND ----------

import os
import sys

NOTEBOOK_DIR = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
BUNDLE_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(NOTEBOOK_DIR))
sys.path.append(f"{BUNDLE_ROOT}/src/superstore_shared_utilities")

from superstore_platform_config import get_bronze_schema, get_metrics_schema, table

DRIFT_TABLE = table(get_metrics_schema(), "schema_drift")
RAW_TABLE = table(get_bronze_schema(), "superstore_raw")

print(f"drift table: {DRIFT_TABLE}")

failures = []


def check(condition, message):
    """Collect rather than raise, so one run reports every failure at once."""
    if condition:
        print(f"PASS  {message}")
    else:
        print(f"FAIL  {message}")
        failures.append(message)


# COMMAND ----------

drift = spark.table(DRIFT_TABLE)
drift.orderBy("detected_at").show(50, truncate=False)

rows = drift.collect()
by_status = {}
for r in rows:
    by_status.setdefault(r["drift_status"], []).append(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Heartbeat — the detector ran on every load

rescued = by_status.get("RESCUED", [])
distinct_runs = {r["master_run_id"] for r in rescued}

check(
    len(rescued) >= 2,
    f"RESCUED heartbeat written for at least the two loads (found {len(rescued)})",
)
check(
    len(distinct_runs) == len(rescued),
    f"one heartbeat per run, not several (runs={len(distinct_runs)}, rows={len(rescued)})",
)
check(
    all(r["row_count"] == 0 for r in rescued),
    "no rows were rescued — the seed types are all parseable, so a non-zero "
    "count here would mean a type problem the suite did not intend",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. It fires — `discount_reason` was detected as NEW
# MAGIC
# MAGIC The check the entire feature rests on. Everything else only proves the
# MAGIC detector stays quiet, which is also what a broken one does.

new_rows = by_status.get("NEW", [])
new_columns = {r["column_name"] for r in new_rows}

check(
    "discount_reason" in new_columns,
    f"discount_reason recorded as NEW (found: {sorted(new_columns)})",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. No false positives — `row_id` stays silent
# MAGIC
# MAGIC It arrives in every file and no entity declares it. Without the
# MAGIC `ignored_source_columns` decision it would be reported on every run
# MAGIC forever, and a permanently red monitor is a muted one.

check(
    "row_id" not in new_columns,
    "row_id NOT reported as drift — it is in ignored_source_columns",
)
check(
    not by_status.get("MISSING"),
    f"no MISSING columns (found: {[r['column_name'] for r in by_status.get('MISSING', [])]})",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Behaviour unchanged — the column was still dropped
# MAGIC
# MAGIC Detection was explicitly not meant to alter what enters the model. A
# MAGIC detector that quietly started widening the contract would be worse than
# MAGIC no detector at all.

raw_columns = set(spark.table(RAW_TABLE).columns)
check(
    "discount_reason" in raw_columns,
    "discount_reason IS in superstore_raw — Bronze keeps everything that arrives",
)

for entity in ("customers", "products", "orders", "sales"):
    entity_columns = set(spark.table(table(get_bronze_schema(), entity)).columns)
    check(
        "discount_reason" not in entity_columns,
        f"discount_reason NOT in {entity} — the allowlist still governs the split",
    )

# COMMAND ----------

if failures:
    raise AssertionError(
        f"{len(failures)} schema-drift assertion(s) failed:\n  - "
        + "\n  - ".join(failures)
    )

dbutils.notebook.exit(f"SCHEMA_DRIFT_ASSERTIONS_OK ({len(rows)} drift row(s))")
