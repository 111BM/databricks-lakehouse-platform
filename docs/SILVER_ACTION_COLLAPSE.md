# Optimization — Collapsing Repeated Silver Passes

> Sequel to [PERFORMANCE_INVESTIGATION.md](PERFORMANCE_INVESTIGATION.md), which fixed the
> date-parsing bottleneck. That fix made this one visible, and also revises one of its
> conclusions (see [What this revises](#what-this-revises)).

## Summary

`bronze_to_silver_prod` triggered **eleven separate Spark actions per entity** purely to
produce metrics and emptiness probes. Because Databricks Serverless forbids `cache()` and
`persist()`, every one of those actions replayed the entire upstream lineage — bronze scan
→ `clean_string_columns` → `add_error_columns` → multi-format date parse.

Collapsing them into **two fused aggregations** removed eight full lineage replays and
three short-circuit probes per entity, with no loss of any metric.

**Status: implemented and correctness-verified; runtime gain not yet measured.** See
[Verification](#verification) for exactly what was and was not proven, and
[How to measure](#how-to-measure) to close that gap.

---

## The constraint that shapes the fix

The obvious fix for "the same DataFrame is scanned ten times" is to cache it. That is not
available here. Per the Databricks documentation:

> Dataframe and SQL cache APIs are not supported on serverless compute. Using any of these
> APIs or SQL commands results in an exception.

Serverless exposes only Spark Connect APIs, not RDD APIs. That rules out `.cache()`,
`.persist()`, `CACHE TABLE`, and also `.checkpoint()` / `.localCheckpoint()` — the last two
are RDD-backed, so they are not a substitute. The module header of
`superstore_silver_module.py` already documented the sibling half of this constraint
("entirely DataFrame-based (no RDD usage)").

Databricks' recommended replacement is to materialize intermediates to a Delta table. That
is a real option (see [Deferred](#deferred-materializing-to-a-staging-table)), but it costs
an extra write. **Collapsing the actions is strictly better where it applies: it eliminates
the work rather than storing it.**

One clarification worth keeping straight, because it explains the whole problem: serverless
*does* cache automatically, but it disk-caches **Delta file reads**, not **computation**.
Re-reading bronze is cheap. Re-evaluating the DQ expressions and
`coalesce(try_to_date(...))` over seven formats is not.

---

## How the problem was found

Not by profiling — by auditing the actions. Every `.count()`, `.head(1)`, `.limit(1).count()`
and `.collect()` inside `bronze_to_silver_prod` is a separate Spark job over the same lineage:

| # | Action | Purpose | Cost |
|---|---|---|---|
| 1 | `df.count()` | `read_rows` | full pass |
| 2 | `good_rows_df.count()` | `good_rows` | full pass |
| 3 | `dirty_rows_df.count()` | `dirty_rows` | full pass |
| 4 | `dirty_rows_df.head(1)` | quarantine branch guard | probe |
| 5 | `silver_cast_df` … `groupBy(partition_id).count().collect()` | `skew_ratio` | full pass |
| 6 | `silver_dedup_df.limit(1).count()` | merge branch guard | probe |
| 7 | `silver_dedup_df.count()` | `dedup_rows` | full pass |
| 8 | `silver_dup_df.limit(1).count()` | audit branch guard | probe |
| 9 | `silver_df.count()` | `total` | full pass |
| 10 | `silver_dedup_df.count()` | `deduplicated_rows` | full pass |
| 11 | `silver_dup_df.count()` | `duplicate_rows` | full pass |

**8 full passes + 3 probes**, none of which write anything. Actions 7/9/10/11 additionally
re-evaluated the `row_number()` dedup window, which carries its own shuffle.

Note that Spark's column pruning could *not* rescue actions 2 and 3 for the `orders` entity:
`is_valid` derives from `error_columns`, which for `orders` depends on the parsed dates. So
those counts paid full price for the date parse. Entities without a date rule (`sales`,
`customers`, `products`) prune more, which is why the expected gain is uneven.

---

## What this revises

[PERFORMANCE_INVESTIGATION.md](PERFORMANCE_INVESTIGATION.md) closes with a deferred item,
"parse dates once", reasoning that `order_date` / `ship_date` are parsed **twice** (once in
the DQ step, once in the casting step) and estimating the remaining upside at ~20 s, or ~3%.

That estimate was too low, because the parse is not in the code path twice — it is in the
*lineage*, and therefore re-executed **once per action**. With eleven actions, the two
call sites were evaluated far more than twice.

This is also the better explanation for that investigation's "unexpected part": the reorder
was predicted to give ~2× and delivered **5×**. The original write-up attributes this
entirely to failed `try_to_date` calls being more expensive than successful ones (which is
true and does contribute). But the multiplier also comes from the parse re-running on every
one of eleven actions rather than twice.

---

## The change

### 1. One pass for the DQ split (replaces actions 1, 2, 3, 4)

```python
# Before: three full passes plus a probe
read_rows    = df.count()
good_count   = good_rows_df.count()
dirty_count  = dirty_rows_df.count()
if dirty_rows_df.head(1):
    ...

# After: one pass; read_rows and the branch guard are derived
valid_counts = {
    row["is_valid"]: row["count"]
    for row in dq_df.groupBy("is_valid").count().collect()
}
good_count  = valid_counts.get(True, 0)
dirty_count = valid_counts.get(False, 0)
read_rows   = good_count + dirty_count
if dirty_count > 0:
    ...
```

### 2. One pass for the dedup metrics (replaces actions 5, 6, 7, 8, 9, 10, 11)

The dedup window is expressed once, via a new pure function `classify_duplicates()`, and the
metrics plus both output branches are all derived from that single frame:

```python
classified_df = classify_duplicates(silver_df, business_keys, "bronze_ingestion_ts")

partition_stats = (
    classified_df.groupBy(spark_partition_id().alias("_partition_id"))
    .agg(
        spark_count(lit(1)).alias("row_count"),
        spark_sum(when(col("row_num") == 1, 1).otherwise(0)).alias("winner_count"),
        spark_sum(when(col("row_num") > 1, 1).otherwise(0)).alias("loser_count"),
    )
    .collect()
)

total        = sum(r["row_count"]    for r in partition_stats)
dedups_count = sum(r["winner_count"] for r in partition_stats)
dups_count   = sum(r["loser_count"]  for r in partition_stats)
# skew_ratio comes from the same row_count distribution

silver_dedup_df = classified_df.filter(col("row_num") == 1).drop("row_num")
silver_dup_df   = classified_df.filter(col("row_num") > 1).drop("row_num")
```

Grouping by `spark_partition_id()` is what lets one aggregation produce both the row counts
*and* the skew distribution.

### 3. Keeping the pure functions honest

`classify_duplicates()` (in `superstore_silver_transformations.py`) now owns the dedup
window as the single source of truth. `deduplicate_latest_wins()` became a thin wrapper over
it.

This matters for the testing story. Production now calls `classify_duplicates()` directly,
and the existing `deduplicate_latest_wins` tests still exercise the same window logic
through the wrapper. Had the window been re-typed inline in the module, the unit tests would
have silently stopped protecting production — the exact failure mode called out in
`tests/unit/silver/test_silver_dedup.py`'s own docstring.

### Net effect

| | Before | After |
|---|---|---|
| Full lineage passes (metrics/probes) | 8 | **2** |
| Short-circuit probes | 3 | **0** |
| Dedup-window evaluations for metrics | 4 | **1** |
| Metrics lost | — | **none** |

---

## The new invariants, and the tests that pin them

The refactor converts two previously *measured* numbers into *derived* ones. A derivation
that silently breaks produces a wrong metrics table rather than a failure, so both
invariants are now under test.

**1. `is_valid` is exhaustive** — required for `read_rows = good_count + dirty_count`.

`add_is_valid()` computes `size(filter(error_columns, x -> x is not null)) == 0`.
`error_columns` is always a non-null array (`add_error_columns` builds it with `array(...)`),
so `size()` never returns null and `is_valid` is strictly `True`/`False`. Two buckets,
partitioning the frame.

- `test_is_valid_is_never_null_so_the_split_is_exhaustive`
- `test_all_clean_rows_still_yield_a_countable_split` — the single-bucket case, where
  `dirty_count` must fall back to 0 rather than raise `KeyError`

**2. The `row_num` split is exhaustive** — required for `total = dedups_count + dups_count`.

Every row is either a winner (`row_num == 1`) or a duplicate (`row_num > 1`), never neither.

- `test_split_is_exhaustive`
- `test_row_num_ranks_latest_first`, `test_winner_is_row_num_one_per_group`,
  `test_row_num_is_never_null`, `test_composite_key_row_num`
- `test_agrees_with_deduplicate_latest_wins` — guards against the wrapper drifting from the
  function production actually calls

---

## Verification

### Proven

- **87 unit tests pass** (up from 79). The pre-existing `deduplicate_latest_wins` tests pass
  unchanged, confirming the wrapper refactor is behaviour-identical.
- **Old-vs-new numerical equivalence.** A synthetic 1,250-row frame with uneven duplicate
  counts, null business keys and invalid categoricals produced identical
  `total` / `deduplicated_rows` / `duplicate_rows` from the old three-count approach and the
  new fused aggregation, at **1, 3, 8 and 50 shuffle partitions**. The `is_valid` split
  matched the old two-count approach exactly. The empty-frame path returns zero rows from
  the aggregation and hits the `skew_ratio` guard without raising.

### Not proven

**The runtime improvement.** No run at scale has been performed. The mechanism is sound and
the direction is not in doubt — eight Spark jobs over an expensive lineage are gone and
nothing was added except two conditional sums inside an aggregation that already had to scan
— but the magnitude is unmeasured, and no number is claimed here.

### Why no prediction is offered

The decomposition in [PERFORMANCE_INVESTIGATION.md](PERFORMANCE_INVESTIGATION.md) cannot
support one. It reports ~464 s of Silver entity time at 3M rows (158 + 117 + 116 + 73), a
698 s total run, and ~420 s of fixed task-startup overhead. Those cannot all hold —
464 + 420 > 698. The "~93 s per million rows + ~7 minutes overhead" figure is a two-point
linear fit, not a decomposition, so the orchestration/data split should be treated as
approximate. Predicting from it would be false precision.

---

## Two behaviour changes

**1. `skew_ratio` changed meaning.** It is now measured **after** the repartition by
`business_keys`, not before. This is the skew the dedup window actually experiences, so a
hot business key shows up where the old placement could only see the bronze read layout —
more useful, but **not comparable to historical values** in `silver_layer_metrics`.

**2. The metrics aggregation can now fail the ETL.** The old skew block was wrapped in
`try/except` that swallowed everything and defaulted `skew_ratio` to 0.0. The fused
aggregation also produces the counts that gate the merge and audit branches, so it is
load-bearing and deliberately not guarded — a failure there must fail the run rather than
silently produce zero counts.

---

## How to measure

```sql
SELECT target_table,
       ROUND(duration_secs,1) AS secs,
       read_rows, inserted_rows,
       ROUND(throughput_rows_per_sec,0) AS rows_per_sec
FROM superstore_catalog.dev_metrics.silver_layer_metrics
WHERE master_run_id = (SELECT master_run_id
                       FROM superstore_catalog.dev_metrics.silver_layer_metrics
                       ORDER BY load_timestamp DESC LIMIT 1)
ORDER BY duration_secs DESC;
```

Validity conditions:

- `read_rows` **and** `inserted_rows` must match the baseline run, or an incremental is being
  compared against a full load.
- **Ignore `skew_ratio`** in any before/after comparison — its measurement point moved.

### Falsifiable prediction

`orders` should improve most in absolute terms, because its `is_valid` depends on the parsed
dates and therefore could not be column-pruned out of the removed counts. `products`
(~1,862 rows) should improve least in absolute terms — but not zero, since each removed
action also removes a Spark job's scheduling and shuffle setup against
`shuffle_partitions=200`, which is why 1,862 rows cost 73 s to begin with.

**If `orders` does not move, the pruning reasoning above is wrong** and the next step is to
compare Spark UI job counts per entity before and after.

### Results

| Silver entity | Before (s) | After (s) | Δ |
|---|---|---|---|
| orders | 158 | _to be measured_ | |
| sales | 117 | _to be measured_ | |
| customers | 116 | _to be measured_ | |
| products | 73 | _to be measured_ | |
| **Whole pipeline @ 3M** | **11 min 38 s** | _to be measured_ | |

---

## Deferred: materializing to a staging table

Three lineage replays remain and are **not** removable by collapsing, because they are
writes: the quarantine append, the Silver merge, and the audit append. Each re-evaluates
`clean → DQ → date parse`.

Databricks' recommended `cache()` substitute would address these:

```python
dq_df.write.mode("overwrite").saveAsTable(f"{staging_schema}.dq_{entity}")
dq_df = spark.table(f"{staging_schema}.dq_{entity}")
```

**Not done, deliberately.** It trades an extra full write for cheaper subsequent reads, so it
only pays off where the lineage is expensive relative to the write. That is plausibly true
for `orders` at 3M rows and almost certainly false for `products` at ~1,862. Doing it
blanket would slow the small entities down. It also introduces staging tables that need a
lifecycle (creation, cleanup, isolation per environment and per `master_run_id`) — real
surface area for a gain that should be measured first.

The right sequence is: measure this change, then decide whether the remaining write-side
replays justify staging tables for the large entities only.

---

## Related levers, not addressed here

Found during this work, deliberately out of scope:

1. **`shuffle_partitions` defaults to 200 for every entity.** Nothing in
   `superstore_silver_config.yaml` sets it, so `products` (~1,862 rows) is repartitioned into
   200 partitions of ~9 rows, three times. The equivalence check incidentally demonstrated
   the effect: measured skew on the same 1,025-row frame rose from 1.0 at one partition to
   1.9 at fifty.
2. **A duplicate `repartition`.** `silver_dedup_df` is repartitioned on the hash column
   twice, back to back, in `superstore_silver_module.py`.
3. **Entity-level parallelism.** The Silver orchestrator processes the four entities in a
   sequential Python loop (158 + 117 + 116 + 73 = 464 s serial). They are independent.
   Prerequisite: the loop's `except Exception` currently logs and continues, so an entity can
   fail while the task still reports success — that must be fixed first, and is worth fixing
   regardless.
4. **`Step 6a` evaluates `try_cast` twice per column** (`filter(try_cast IS NOT NULL)` then
   `.cast()`), and silently drops rows that pass the regex but overflow the target type
   (e.g. `99999999999.99` into `decimal(10,2)`) — which breaks the
   `bronze == silver + quarantine + audit` reconciliation invariant. Folding this into the DQ
   step as a quarantine rule would be both faster and more correct.
5. **Task consolidation.** The pipeline remains orchestration-bound: 11 of 18 tasks are
   post-gold serving notebooks, each paying a serverless cold start. This change touches only
   the data half of one task, so task consolidation remains the larger lever — as
   [PERFORMANCE_INVESTIGATION.md](PERFORMANCE_INVESTIGATION.md) already concluded.

---

## Files changed

| File | Change |
|---|---|
| `src/superstore_silver/superstore_silver_transformations.py` | Added `classify_duplicates()`; `deduplicate_latest_wins()` became a wrapper over it |
| `src/superstore_silver/superstore_silver_module.py` | Two fused aggregations replace eight passes and three probes; `read_rows` derived; skew relocated |
| `tests/unit/silver/test_silver_dedup.py` | `TestClassifyDuplicates` — row_num contract and exhaustiveness (+6 tests) |
| `tests/unit/silver/test_silver_dq.py` | `is_valid` exhaustiveness and single-bucket fallback (+2 tests) |

## References

- [Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
- [Migrate from classic compute to serverless compute](https://docs.databricks.com/aws/en/compute/serverless/migration)
