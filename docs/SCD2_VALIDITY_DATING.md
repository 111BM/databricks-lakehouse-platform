# SCD2 Validity Dating — why closed versions covered no time

## Summary

Every closed version in `dim_customers` carried an **inverted validity interval**:
`effective_to` fell one second *before* its own `effective_from`. The rows looked correct —
`is_current = false`, `effective_to` populated — but no point-in-time query could ever match
them, which is the single thing SCD2 exists to support.

The cause was that `effective_from` was derived from a value that is **constant across an
entity's versions**. The fix is one line. The reason it survived so long is more interesting
than the fix, and is the point of this document.

---

## The mechanism

`merge_into_gold_table_scd2` closes an old version using the incoming row's timestamp:

```python
.whenMatchedUpdate(
    condition=f"NOT (tgt.{hash_column} <=> src.{hash_column})",
    set={
        "effective_to": expr("src.effective_from - INTERVAL 1 SECOND"),
        "is_current": expr("false"),
    }
)
```

That gives `effective_from` exactly one load-bearing property: **it must advance between
versions of the same entity.**

`prepare_scd2_columns` previously did this:

```python
if dim_type == "customers":
    first_order_df = silver_orders_df.groupBy("customer_id") \
        .agg(min("order_date").alias("first_order_date"))
    df = silver_df.join(first_order_df, on="customer_id", how="left")
    df = df.withColumn("effective_from", col("first_order_date"))
else:
    df = silver_df.withColumn("effective_from", col("silver_ingestion_ts"))
```

The customer's **first order date** is a business date, but it is the same value for every
version of that customer. So closing a version produced:

| Row | effective_from | effective_to | is_current |
|---|---|---|---|
| v1 | 2024-01-15 | **2024-01-14 23:59:59** | false |
| v2 | 2024-01-15 | null | true |

```sql
-- returns nothing, for any date
WHERE '2024-03-01' BETWEEN effective_from AND effective_to
```

`dim_products` used `silver_ingestion_ts` and was always coherent. Only the customers
dimension was affected.

---

## Why the tests did not catch it

Two checks existed, and both passed against broken data.

**`assert_scd2_change.py`** asserted that a closed version exists and has `effective_to`
populated:

```python
closed = cg.filter((col("is_current") == False) & col("effective_to").isNotNull())
```

Both true. Nothing compared the two dates.

**`assert_dim.py`** asserted no *overlapping* validity ranges:

```python
.filter(col("prev_to").isNotNull() & (col("effective_from") <= col("prev_to")))
```

An inverted interval covers no time at all, so it cannot overlap anything. The check passed
precisely *because* the data was broken.

That is the general lesson: **an assertion on the presence of a value is not an assertion on
its meaning.** Both checks tested that the SCD2 machinery ran, not that it produced a usable
timeline.

---

## The fix

```python
# one rule for every dimension
df = silver_df.withColumn("effective_from", col("silver_ingestion_ts"))
```

`first_order_date` is no longer computed here; the join and the `silver_orders_tbl` config key
that fed it are gone.

### It is processing time, and that is deliberate

`silver_ingestion_ts` records **when the pipeline observed the state**, not when the state
changed. If a customer's segment changed on the 3rd and the pipeline saw it on the 8th, the
history says the 8th.

That is the most precise honest answer available here: the Superstore source is a set of CSV
snapshots with **no change timestamp**. Nothing in the data says when an attribute changed.
The options were:

| Source of truth | Verdict |
|---|---|
| `silver_ingestion_ts` (processing time) | chosen — always present, always advances |
| `source_file_modification_time` (snapshot as-of date) | more accurate, but the column stops at `bronze_superstore`; it would need propagating through the entity split and Silver first |
| A `last_modified` column in the source | would mean authoring our own source data |
| CDC commit timestamp | correct, and the right answer with a real source |

With a CDC source, `effective_from` would come from the commit timestamp and this becomes
business-time dating for free.

### What it does not do

The pipeline **cannot place a retroactive change into the middle of an existing timeline**.
The merge appends: it closes the current version and inserts a new one. Inserting a version
between two existing ones would require recomputing the neighbours' `effective_to`, which it
does not do.

There is a deeper blocker than the merge. Silver deduplicates to one row per business key, so
a historical version of a customer is classified as a duplicate and routed to the audit table
**before Gold ever sees it**. Retroactive dimension history would require Silver to retain
versions — a change to the reconciliation invariant, not a change to this function.

---

## Guards added

**Unit** — `tests/unit/gold/test_scd2_effective_from.py` asserts the property directly:
`effective_from` resolves to `silver_ingestion_ts`, resolves identically for every
`dim_type`, advances between versions, and that closing a version with
`effective_from - 1 SECOND` yields an interval that is not inverted.

**Integration** — both dimension assertions now check interval validity:

```python
inverted = closed.filter(col("effective_to") <= col("effective_from"))
check("closed version covers a valid interval", inverted.count() == 0)
```

---

## Repairing existing rows

The fix corrects new versions. It does not rewrite history, so rows already closed keep their
inverted intervals. Behaviour going forward:

| Rows | After the fix |
|---|---|
| Already closed | Still inverted — needs a rebuild |
| Currently open, closed later | Valid, but **wide**: `effective_from` is still the old first-order date, so the interval claims the state held since that date |
| New versions | Precise processing-time intervals |

Full correction means rebuilding `dim_customers` from Silver — `run_mode=full_refresh`, which
is gated behind `allow_full_refresh` in the orchestrators and therefore takes a deliberate
code change. Until that is run, the dimension holds two dating regimes, which is worth knowing
before querying its history.

---

## The general lesson

`effective_to` and `is_current` were derived from `effective_from` without anything ever
checking that `effective_from` meant what the derivation assumed. The column was populated,
the merge ran, the metrics counted rows, and two integration assertions passed — while the
history was unusable. Assertions that check a value exists are cheap; assertions that check a
value is *coherent* are the ones that catch this class of bug.
