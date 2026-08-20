# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — Step 3 snapshot: state before the replay leg
# MAGIC
# MAGIC The suite previously exercised `incremental` only. Every windowed-mode
# MAGIC defect found on 2026-08-13 was therefore invisible to it:
# MAGIC
# MAGIC - quarantine and audit were appended to, so a replay silently added a
# MAGIC   second copy of every dirty row (docs/RUN_MODE_IDEMPOTENCY.md)
# MAGIC - Gold dimensions windowed on `silver_ingestion_ts` while Silver windowed
# MAGIC   on `bronze_ingestion_ts`, so a replay selected ZERO dimension rows and
# MAGIC   reported success (docs/GOLD_WINDOW_ALIGNMENT.md)
# MAGIC
# MAGIC Both ran green in CI the whole time. This notebook captures the state a
# MAGIC correct replay must reproduce exactly, and derives the replay window from
# MAGIC the data rather than from the clock — the seed lands "now", so a
# MAGIC hard-coded or job-start date would silently miss the window if a run
# MAGIC crossed midnight UTC, which is precisely the sort of quiet miss this leg
# MAGIC exists to catch.

# COMMAND ----------

import sys

sys.path.append(dbutils.widgets.get("helpers_path"))
from assertion_helpers import BRONZE, SILVER, QUARANTINE, AUDIT, GOLD

# Which point in the suite this snapshot represents. Idempotency is a property
# of replay-vs-replay, NOT of replay-vs-incremental: a replay processes the whole
# window as one batch while incremental loads process several, so a row that is a
# duplicate within one batch is not one across two. A replay therefore reclassifies
# rows between Silver and audit legitimately, preserving their sum. Comparing a
# replay against the incremental state would fail on correct behaviour.
dbutils.widgets.text("phase", "incremental")
PHASE = dbutils.widgets.get("phase")

from pyspark.sql.functions import col, min as spark_min, max as spark_max

# COMMAND ----------

# DBTITLE 1,Derive the replay window from the ingested data
bounds = (
    spark.table(f"{BRONZE}.customers")
    .agg(
        spark_min(col("ingestion_date")).alias("lo"),
        spark_max(col("ingestion_date")).alias("hi"),
    )
    .first()
)

if bounds["lo"] is None:
    raise AssertionError(
        "Bronze customers has no ingestion_date — cannot derive a replay window. "
        "The initial load must run before this task."
    )

replay_start = bounds["lo"].strftime("%Y-%m-%d")
replay_end = bounds["hi"].strftime("%Y-%m-%d")

print(f"replay window derived from data: {replay_start} .. {replay_end}")

dbutils.jobs.taskValues.set(key="replay_start", value=replay_start)
dbutils.jobs.taskValues.set(key="replay_end", value=replay_end)

# COMMAND ----------

# DBTITLE 1,Snapshot the counts a correct replay must leave unchanged
# Written to a table rather than task values: assert_replay needs to compare
# against it, and a table keeps the comparison inspectable after a failure.
SNAPSHOT = f"{GOLD}.replay_baseline"

rows = []
for entity in ("customers", "products", "orders", "sales"):
    rows.append(
        (
            entity,
            spark.table(f"{BRONZE}.{entity}").count(),
            spark.table(f"{SILVER}.{entity}").count(),
            spark.table(f"{QUARANTINE}.{entity}_dirty").count()
            if spark.catalog.tableExists(f"{QUARANTINE}.{entity}_dirty") else 0,
            spark.table(f"{AUDIT}.{entity}_duplicates").count()
            if spark.catalog.tableExists(f"{AUDIT}.{entity}_duplicates") else 0,
        )
    )

for dim in ("dim_customers", "dim_products"):
    current = spark.table(f"{GOLD}.{dim}").filter(col("is_current") == True).count()
    total = spark.table(f"{GOLD}.{dim}").count()
    rows.append((dim, 0, current, total, 0))

# Facts, recorded for the same reason dimensions are: a replay re-derives them,
# and nothing was checking whether that is idempotent.
#
# merge_fact_into_gold merges on natural keys, so re-deriving identical rows
# SHOULD update in place rather than append -- but "should" is exactly the word
# that preceded the backfill defect, where a mechanism assumed idempotent
# duplicated 505 Bronze rows and every downstream count stayed plausible.
#
# Stored as (total_rows, distinct_grain) in the silver_rows / quarantine_rows
# columns, following the same positional convention the dimensions above use.
# Duplication shows up as either number moving, or as the two diverging.
for fact, grain in (("facts_orders", ["order_id"]), ("facts_sales", ["order_id", "product_id"])):
    f = spark.table(f"{GOLD}.{fact}")
    rows.append((fact, 0, f.count(), f.select(*grain).distinct().count(), 0))

snapshot_df = spark.createDataFrame(
    [(PHASE,) + r for r in rows],
    ["phase", "entity", "bronze_rows", "silver_rows", "quarantine_rows", "audit_rows"],
)

# append: each phase adds its own rows so assert_replay can diff them
mode = "overwrite" if PHASE == "incremental" else "append"
writer = snapshot_df.write.format("delta").mode(mode)
if mode == "overwrite":
    writer = writer.option("overwriteSchema", "true")
writer.saveAsTable(SNAPSHOT)

print(f"snapshot phase={PHASE} written to {SNAPSHOT}")
snapshot_df.show(truncate=False)

# COMMAND ----------

dbutils.notebook.exit("SNAPSHOT_OK")
