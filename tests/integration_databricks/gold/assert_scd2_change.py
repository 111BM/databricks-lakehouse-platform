# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — SCD2 change detection & idempotency (second run)
# MAGIC
# MAGIC Runs after the SECOND incremental load (seed_v2). Verifies the dimension
# MAGIC framework correctly historizes a real change and does NOT churn on
# MAGIC unchanged data.
# MAGIC
# MAGIC - `CG-12520` (city changed Henderson -> Oakland): a new current version
# MAGIC   exists with the new city, and the previous version is closed.
# MAGIC - `DV-13045` (unchanged): still a single version — no phantom SCD2 row.
# MAGIC - Global: still exactly one current row per customer.

# COMMAND ----------

import sys
# Deployed _helpers location is passed in by the job (bundle-relative).
sys.path.append(dbutils.widgets.get("helpers_path"))
from assertion_helpers import check, count, count_or_zero, columns, table_exists, finalize, CATALOG, ENV, BRONZE, SILVER, GOLD, QUARANTINE, AUDIT, METRICS, SEED_ROW_COUNT, BUSINESS_KEYS

from pyspark.sql.functions import col

DIM = f"{GOLD}.dim_customers"

# COMMAND ----------

# DBTITLE 1,Changed customer (CG-12520) — new version opened, old version closed
if check(f"{DIM} exists", table_exists(DIM)):
    dim = spark.table(DIM)
    cg = dim.filter(col("customer_id") == "CG-12520")

    total_versions = cg.count()
    check("CG-12520 now has >= 2 versions (history retained)", total_versions >= 2,
          f"versions={total_versions}")

    current = cg.filter(col("is_current") == True)
    check("CG-12520 has exactly one current version", current.count() == 1,
          f"current={current.count()}")

    if current.count() == 1 and "city" in dim.columns:
        cur_city = current.first()["city"]
        check("CG-12520 current version reflects the CHANGE (city = Oakland)",
              cur_city == "Oakland", f"city={cur_city}")

    # at least one closed (historical) version with effective_to populated
    closed = cg.filter((col("is_current") == False) & col("effective_to").isNotNull())
    check("CG-12520 previous version was closed (is_current=false, effective_to set)",
          closed.count() >= 1, f"closed={closed.count()}")

    # A closed version must cover a real span of time.
    #
    # Checking only that effective_to is populated is not enough, and that gap
    # is how an inverted timeline survived: the merge closes a row with
    # "src.effective_from - 1 SECOND", so if effective_from does not advance
    # between an entity's versions, the closed row ends BEFORE it starts. It is
    # closed, is_current is false, effective_to is set - and no point-in-time
    # query can ever land inside it, which is the entire purpose of SCD2.
    inverted = closed.filter(col("effective_to") <= col("effective_from"))
    check("CG-12520 closed version covers a valid interval (effective_to > effective_from)",
          inverted.count() == 0, f"inverted intervals={inverted.count()}")

# COMMAND ----------

# DBTITLE 1,Unchanged customer (DV-13045) — NO phantom version (idempotency)
if table_exists(DIM):
    dv = spark.table(DIM).filter(col("customer_id") == "DV-13045")
    check("DV-13045 has exactly one version (unchanged data made no new version)",
          dv.count() == 1, f"versions={dv.count()}")
    check("DV-13045 remains current", dv.filter(col("is_current") == True).count() == 1)

# COMMAND ----------

# DBTITLE 1,Global SCD2 invariant still holds after the second run
if table_exists(DIM):
    violations = (
        spark.table(DIM)
        .filter(col("is_current") == True)
        .groupBy("customer_id").count()
        .filter(col("count") != 1)
        .count()
    )
    check("still exactly one current row per customer_id", violations == 0,
          f"keys with != 1 current row: {violations}")

# COMMAND ----------

# DBTITLE 1,Reconciliation under INCREMENTAL loading — the four-term invariant
# This task runs while the state is still incremental (two separate loads), and
# that is the only point in the suite where the shortfall exists. After the
# replay leg it is repaired, so a check placed later would verify the formula
# only where it returns zero — which proves it is not wrong, not that it is
# right. The same trap that let an invalid-value defect ship earlier.
#
# `bronze == silver + quarantine + audit` is SHORT here by construction. Seed 2
# re-sends CG-12520 (changed) and DV-13045 (unchanged); Silver's MERGE updates
# both rows in place, so each superseded version lands in no bucket — not Silver
# (overwritten), not quarantine (valid), not audit (which only receives
# intra-batch deduplication losers). Expected shortfall: exactly 2 on customers.
#
# The fourth term derives those rows from Bronze rather than storing them, since
# Bronze already retains every arrival. See docs/RECONCILIATION_INVARIANT.md.
# The reconciliation helper is production code, not a test helper, so it lives
# under src/ and needs its own path. The job passes it as shared_utils_path.
dbutils.widgets.text("shared_utils_path", "")
_shared = dbutils.widgets.get("shared_utils_path")
if _shared:
    sys.path.append(_shared)

try:
    from superstore_reconciliation import reconciliation_sql
    _recon_available = True
except ImportError as exc:
    _recon_available = False
    check("superstore_reconciliation is importable", False,
          f"{exc} — shared_utils_path={_shared!r}")

if _recon_available:
    for entity, keys in BUSINESS_KEYS.items():
        # Quarantine and audit tables only exist once they receive a row; the
        # seed dirties no products, so products_dirty is legitimately absent.
        r = spark.sql(
            reconciliation_sql(
                CATALOG, f"{ENV}_bronze", f"{ENV}_silver",
                f"{ENV}_quarantine", f"{ENV}_audit", entity, keys,
                quarantine_exists=table_exists(f"{QUARANTINE}.{entity}_dirty"),
                audit_exists=table_exists(f"{AUDIT}.{entity}_duplicates"),
            )
        ).first()

        three_term = r["silver_rows"] + r["quarantine_rows"] + r["audit_rows"]

        # The four-term invariant must hold even here, where three terms do not.
        check(f"{entity}: four-term reconciliation balances under incremental",
              bool(r["balanced"]),
              f"bronze={r['bronze_rows']}, silver={r['silver_rows']}, "
              f"quarantine={r['quarantine_rows']}, audit={r['audit_rows']}, "
              f"superseded={r['superseded_rows']}, accounted={r['accounted_rows']}")

        # superseded must equal the actual gap, not merely make the sum work.
        # Pinning it against an independently computed residual is what stops
        # the term becoming a fudge factor that balances by definition.
        check(f"{entity}: superseded equals the observed three-term shortfall",
              r["superseded_rows"] == r["bronze_rows"] - three_term,
              f"superseded={r['superseded_rows']}, "
              f"observed shortfall={r['bronze_rows'] - three_term}")

    # And the headline: customers is short by exactly the two re-sent rows.
    rc = spark.sql(
        reconciliation_sql(CATALOG, f"{ENV}_bronze", f"{ENV}_silver",
                           f"{ENV}_quarantine", f"{ENV}_audit",
                           "customers", BUSINESS_KEYS["customers"],
                           quarantine_exists=table_exists(f"{QUARANTINE}.customers_dirty"),
                           audit_exists=table_exists(f"{AUDIT}.customers_duplicates"))
    ).first()
    check("customers: exactly 2 superseded rows after the second seed",
          rc["superseded_rows"] == 2,
          f"superseded={rc['superseded_rows']} — seed 2 re-sends CG-12520 "
          f"(changed) and DV-13045 (unchanged), so the shortfall must be 2")

# COMMAND ----------

finalize("SCD2_CHANGE")
dbutils.notebook.exit("SCD2_CHANGE_ASSERTIONS_PASSED")
