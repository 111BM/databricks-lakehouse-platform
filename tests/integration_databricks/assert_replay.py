# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — replay assertions
# MAGIC
# MAGIC The suite exercised `incremental` only, so every windowed-mode defect was
# MAGIC invisible to it — and both ran green in CI for as long as they existed:
# MAGIC
# MAGIC - quarantine and audit were appended to rather than replaced in scope, so
# MAGIC   every replay added a second copy of each dirty and duplicate row and the
# MAGIC   `bronze == silver + quarantine + audit` invariant over-counted
# MAGIC   (docs/RUN_MODE_IDEMPOTENCY.md)
# MAGIC - Gold dimensions derived their window from `silver_ingestion_ts` while
# MAGIC   Silver used `bronze_ingestion_ts`. A replay rewrites Silver stamped *now*,
# MAGIC   which can never fall inside a historical window, so Gold selected zero
# MAGIC   rows and reported success (docs/GOLD_WINDOW_ALIGNMENT.md)
# MAGIC
# MAGIC ## What idempotency means here, and what it does not
# MAGIC
# MAGIC It is a property of **replay vs replay**, not of replay vs incremental.
# MAGIC
# MAGIC A replay processes the whole window as ONE batch; the incremental loads
# MAGIC processed it as several. A row that is a duplicate within one batch is not
# MAGIC a duplicate across two, so a replay legitimately moves rows from Silver
# MAGIC into audit — the partition changes, the sum does not. The first version of
# MAGIC this notebook asserted `audit unchanged` across that boundary and failed on
# MAGIC correct behaviour (2 -> 4 on customers and products).
# MAGIC
# MAGIC So the checks are split three ways:
# MAGIC   - **replay -> replay**: nothing at all may change. This is idempotency.
# MAGIC   - **after a replay**: reconciliation is EXACT, `sum == bronze`.
# MAGIC   - **incremental**: the sum is only bounded ABOVE. It can legitimately be
# MAGIC     short, because Silver's MERGE updates a row in place when an entity
# MAGIC     re-appears changed, leaving the superseded version in no bucket. Seen
# MAGIC     here as 7 of 9 after the second seed, and as a 416-row shortfall on
# MAGIC     customers at ~1M rows in dev. Over-counting is the thing that must
# MAGIC     never happen — that was the append-only quarantine defect.
# MAGIC
# MAGIC ## Why counts alone are not enough
# MAGIC
# MAGIC A replay that selects *nothing* also leaves every count unchanged. Passing
# MAGIC on counts alone would give a test that goes green exactly when the Gold
# MAGIC windowing bug is present. The `REPLAY` load_type checks are what separate
# MAGIC "replayed correctly" from "did not run".

# COMMAND ----------

import sys

sys.path.append(dbutils.widgets.get("helpers_path"))
from assertion_helpers import (
    check, table_exists, finalize,
    BRONZE, SILVER, GOLD, QUARANTINE, AUDIT, METRICS,
)

from pyspark.sql.functions import col

SNAPSHOT = f"{GOLD}.replay_baseline"
ENTITIES = ("customers", "products", "orders", "sales")

# COMMAND ----------

# DBTITLE 1,The replay actually ran — not a silent no-op
# Zero REPLAY rows in gold metrics is the signature of the windowing defect:
# Silver updated, Gold selected nothing, every task green.
for layer, tbl in (("silver", "silver_layer_metrics"), ("gold", "gold_layer_metrics")):
    m = f"{METRICS}.{tbl}"
    if check(f"{m} exists", table_exists(m)):
        replays = spark.table(m).filter(col("load_type") == "REPLAY").count()
        check(f"{layer} metrics recorded a REPLAY load_type", replays >= 1,
              f"replay rows={replays} — 0 means {layer} skipped the replay entirely")

m = f"{METRICS}.silver_layer_metrics"
if table_exists(m):
    failed = spark.table(m).filter(col("run_status").isin("failure", "FAILURE", "failed")).count()
    check("no Silver entity reported failure anywhere in the suite", failed == 0,
          f"failure rows={failed}")

# COMMAND ----------

# DBTITLE 1,Idempotency — the SECOND replay changed nothing at all
# This is the direct regression test for the append-only quarantine/audit: with
# that defect both tables double on every replay.
if check(f"{SNAPSHOT} exists", table_exists(SNAPSHOT)):
    snap = spark.table(SNAPSHOT)
    first = {r["entity"]: r for r in snap.filter(col("phase") == "replay_1").collect()}

    if check("snapshot captured the first replay", len(first) > 0,
             "phase 'replay_1' missing — did snapshot_after_replay_1 run?"):
        for entity in ENTITIES:
            b = first[entity]

            silver_now = spark.table(f"{SILVER}.{entity}").count()
            check(f"second replay left silver.{entity} unchanged",
                  silver_now == b["silver_rows"],
                  f"after first replay={b['silver_rows']}, after second={silver_now}")

            q = f"{QUARANTINE}.{entity}_dirty"
            if table_exists(q):
                now = spark.table(q).count()
                check(f"second replay left quarantine.{entity}_dirty unchanged",
                      now == b["quarantine_rows"],
                      f"after first={b['quarantine_rows']}, after second={now}")

            a = f"{AUDIT}.{entity}_duplicates"
            if table_exists(a):
                now = spark.table(a).count()
                check(f"second replay left audit.{entity}_duplicates unchanged",
                      now == b["audit_rows"],
                      f"after first={b['audit_rows']}, after second={now}")

        for dim in ("dim_customers", "dim_products"):
            b = first[dim]
            d = spark.table(f"{GOLD}.{dim}")
            check(f"second replay left {dim} current rows unchanged",
                  d.filter(col("is_current") == True).count() == b["silver_rows"],
                  f"after first={b['silver_rows']}, after second={d.filter(col('is_current') == True).count()}")
            # A replay must not manufacture SCD2 history: hash-based change
            # detection means re-deriving identical rows creates no new version.
            check(f"second replay created no spurious {dim} SCD2 versions",
                  d.count() == b["quarantine_rows"],
                  f"after first={b['quarantine_rows']}, after second={d.count()}")

        # Facts. Previously unchecked after a replay, while the replay legs ran
        # the Gold facts orchestrator anyway -- so the suite was paying to
        # produce output nothing looked at.
        #
        # merge_fact_into_gold merges on natural keys, so a replay SHOULD update
        # in place. That word is doing a lot of work: the backfill defect was a
        # mechanism assumed idempotent that appended 505 duplicate Bronze rows,
        # and every downstream count stayed plausible because Silver's dedup
        # absorbed them. Facts have no such absorber -- a duplicate here lands
        # directly in the marts.
        #
        # assert_gold_fact checks grain uniqueness, but only after the initial
        # load. These two checks are the same property after a replay.
        for fact, grain in (("facts_orders", ["order_id"]),
                            ("facts_sales", ["order_id", "product_id"])):
            b = first[fact]
            f = spark.table(f"{GOLD}.{fact}")
            total = f.count()
            distinct = f.select(*grain).distinct().count()

            check(f"second replay left {fact} row count unchanged",
                  total == b["silver_rows"],
                  f"after first={b['silver_rows']}, after second={total} "
                  f"— a merge on {grain} should update in place, not append")
            check(f"{fact} grain {grain} still unique after replay",
                  total == distinct,
                  f"rows={total}, distinct grain={distinct} — the replay "
                  f"duplicated fact rows")

# COMMAND ----------

# DBTITLE 1,Reconciliation holds — the only invariant that spans the mode change
# Rows may move between Silver and audit when batching changes, so the sum is
# what must be preserved across incremental -> replay, not the individual counts.
for entity in ENTITIES:
    bronze_c = spark.table(f"{BRONZE}.{entity}").count()
    silver_c = spark.table(f"{SILVER}.{entity}").count()
    quar_c = spark.table(f"{QUARANTINE}.{entity}_dirty").count() if table_exists(f"{QUARANTINE}.{entity}_dirty") else 0
    audit_c = spark.table(f"{AUDIT}.{entity}_duplicates").count() if table_exists(f"{AUDIT}.{entity}_duplicates") else 0
    total = silver_c + quar_c + audit_c
    check(f"reconciliation holds for {entity} after replay", total == bronze_c,
          f"bronze={bronze_c}, silver={silver_c}, quarantine={quar_c}, audit={audit_c}, sum={total}")

# COMMAND ----------

# DBTITLE 1,Incremental never OVER-counts (the append-duplication regression)
# The incremental state can legitimately be SHORT of bronze, and is: Silver's
# MERGE updates a row in place when an entity re-appears with changed values, so
# the superseded version lands in no bucket -- not Silver (overwritten), not
# quarantine (it was valid), not audit (that only receives intra-batch dedup
# losers). Measured here as 7 accounted-for against 9 in bronze after the second
# seed, and at ~1M rows in dev as a 416-row shortfall on customers.
#
# A replay repairs it by processing the window as one batch, where both versions
# are present and the older is correctly audited. That is why the reconciliation
# check above asserts equality only AFTER the replay.
#
# What must never happen in either mode is over-counting: that is the signature
# of the append-only quarantine/audit defect, where a replay added a second copy
# of every dirty row. So the incremental sum is bounded above, not pinned.
if table_exists(SNAPSHOT):
    snap = spark.table(SNAPSHOT)
    inc = {r["entity"]: r for r in snap.filter(col("phase") == "incremental").collect()}
    if inc:
        for entity in ENTITIES:
            b = inc[entity]
            inc_sum = b["silver_rows"] + b["quarantine_rows"] + b["audit_rows"]
            check(f"{entity}: incremental never accounts for MORE rows than bronze",
                  inc_sum <= b["bronze_rows"],
                  f"incremental sum={inc_sum}, bronze={b['bronze_rows']}")

# COMMAND ----------

finalize("REPLAY")
dbutils.notebook.exit("REPLAY_ASSERTIONS_PASSED")
