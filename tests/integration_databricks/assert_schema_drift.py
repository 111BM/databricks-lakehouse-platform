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

import sys

# Table names come from assertion_helpers, which hardcodes ENV="integration_test",
# NOT from superstore_platform_config.
#
# The first version of this notebook used get_metrics_schema(), and that reads
# os.getenv("SUPERSTORE_ENV", "dev") -- the env var is set for pipeline tasks but
# NOT for this assertion task, so it silently resolved to dev_metrics and
# dev_bronze. Every run of this notebook was asserting against DEV.
#
# It went unnoticed because dev happened to contain plausible values: a run
# reported SCHEMA_DRIFT_ASSERTIONS_OK with rows=3 and was presented as proof the
# detector fires in CI, when it was reading a dev table populated by a manual
# dev experiment. The "unexplained qa discrepancy" chased for hours was the same
# thing -- dev's row count at different moments.
#
# A wrong-but-plausible table is worse than a missing one: a missing table fails
# loudly, a wrong one passes.
sys.path.append(dbutils.widgets.get("helpers_path"))
from assertion_helpers import BRONZE, ENV, METRICS

DRIFT_TABLE = f"{METRICS}.schema_drift"
RAW_TABLE = f"{BRONZE}.superstore_raw"

print(f"env={ENV} drift table={DRIFT_TABLE} raw table={RAW_TABLE}")

failures = []
checks_run = []

# Every check below must execute. The first version of this notebook put the
# `# MAGIC %md` headings in the SAME cell as the code beneath them, which makes
# Databricks treat the whole cell as markdown -- so not one assertion ran, and
# the notebook exited "OK" having verified nothing.
#
# A green run proving nothing is the exact failure this suite exists to catch,
# so the count is asserted rather than trusted.
EXPECTED_CHECKS = 13


def check(condition, message):
    """Collect rather than raise, so one run reports every failure at once."""
    checks_run.append(message)
    if condition:
        print(f"PASS  {message}")
    else:
        print(f"FAIL  {message}")
        failures.append(message)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. The test is pointed at the right environment
# MAGIC
# MAGIC Checked first, and checked at all, because this notebook spent several
# MAGIC runs asserting against `dev` — passing, failing, and producing a
# MAGIC discrepancy that took hours to chase. Every one of those outcomes was
# MAGIC about dev's data.
# MAGIC
# MAGIC A test reading the wrong table does not fail; it reports confidently
# MAGIC about something nobody asked. That is worse than a missing table, which
# MAGIC at least fails loudly.

# COMMAND ----------

check(
    "integration_test" in DRIFT_TABLE,
    f"drift table belongs to integration_test, not another env ({DRIFT_TABLE})",
)
check(
    "integration_test" in RAW_TABLE,
    f"raw table belongs to integration_test, not another env ({RAW_TABLE})",
)

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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

raw_columns = set(spark.table(RAW_TABLE).columns)
check(
    "discount_reason" in raw_columns,
    "discount_reason IS in superstore_raw — Bronze keeps everything that arrives",
)

for entity in ("customers", "products", "orders", "sales"):
    entity_columns = set(spark.table(f"{BRONZE}.{entity}").columns)
    check(
        "discount_reason" not in entity_columns,
        f"discount_reason NOT in {entity} — the allowlist still governs the split",
    )

# COMMAND ----------

summary = {status: len(v) for status, v in sorted(by_status.items())}

# Forensics for the qa/dev discrepancy.
#
# The identical operation produced opposite outcomes: in dev the retry ADDED
# discount_reason (28 -> 29 columns, 0 rescued) and schema_drift gained a NEW
# row; in integration_test the column never appeared and only one drift row was
# written. Same code, same Auto Loader settings.
#
# `cleanup` drops integration_test_* the moment this task finishes, so the
# tables cannot be inspected afterwards -- which is why every question so far has
# had to be answered by guessing. These four facts travel out in the exit string
# instead, and settle it in one run:
#
#   raw_cols / raw_has_dr  did the retry add the column, or not
#   raw_rescued            or did the value go into col__rescued_data
#   drift_runs             how many distinct pipeline runs wrote a heartbeat
rescued_in_raw = spark.table(RAW_TABLE).where("col__rescued_data IS NOT NULL").count()
drift_runs = sorted({r["master_run_id"] for r in rows})

forensics = (
    f"raw_cols={len(raw_columns)} "
    f"raw_has_dr={'discount_reason' in raw_columns} "
    f"raw_rescued={rescued_in_raw} "
    f"drift_runs={len(drift_runs)}"
)

# The diagnostic goes in the EXIT STRING, not a print(). The Databricks Jobs API
# returns only `notebook_output` for a notebook task -- cell output is
# unreachable, and `export-run` returns an HTML shell that loads content via
# JavaScript. A print() here is visible solely to a human opening the run in a
# browser, which is how the previous no-op version looked healthy to every
# automated check.
state = (
    f"rows={len(rows)} by_status={summary} checks_run={len(checks_run)} {forensics}"
)

if len(checks_run) != EXPECTED_CHECKS:
    raise AssertionError(
        f"only {len(checks_run)} of {EXPECTED_CHECKS} assertions executed -- "
        f"the notebook is not running the checks it appears to contain "
        f"(a `# MAGIC %md` heading sharing a cell with code will do this). {state}"
    )

if failures:
    raise AssertionError(
        f"{len(failures)} schema-drift assertion(s) failed. {state}\n  - "
        + "\n  - ".join(failures)
    )

dbutils.notebook.exit(f"SCHEMA_DRIFT_ASSERTIONS_OK {state}")
