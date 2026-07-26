# Performance Investigation — Silver Date Parsing

## Summary

A profiling pass at 3M source rows found the Silver layer consuming **91% of all
data-processing time**, and within it a single entity (`orders`) running ~10× slower
per row than its siblings despite writing *fewer* rows. The cause was a 7-format
`coalesce(try_to_date(...))` where the actual source format sat second in the list,
so every row paid for a failed parse attempt first — in two separate steps.

Reordering the format list cut that entity from **803 s to 158 s** and **halved total
pipeline runtime** at 3M rows.

---

## How the bottleneck was found

The pipeline already writes per-entity metrics (`duration_secs`, `read_rows`,
`throughput_rows_per_sec`) to `{env}_metrics.*` for every layer, so the investigation
was a query rather than a guess.

### Step 1 — Which layer?

```sql
WITH m AS (
  SELECT 'bronze' AS layer, master_run_id, duration_secs, load_timestamp
    FROM superstore_catalog.dev_metrics.bronze_layer_entity_metrics
  UNION ALL
  SELECT 'silver', master_run_id, duration_secs, load_timestamp
    FROM superstore_catalog.dev_metrics.silver_layer_metrics
  UNION ALL
  SELECT 'gold', master_run_id, duration_secs, load_timestamp
    FROM superstore_catalog.dev_metrics.gold_layer_metrics
)
SELECT layer, ROUND(SUM(duration_secs),1) AS total_secs
FROM m
WHERE master_run_id = (SELECT master_run_id FROM m ORDER BY load_timestamp DESC LIMIT 1)
GROUP BY layer ORDER BY total_secs DESC;
```

| Layer | Total | vs bronze |
|---|---|---|
| **silver** | **1109 s** | **28×** |
| gold | 72 s | 1.8× |
| bronze | 39 s | — |

Silver dominating is expected — it owns the MERGEs, the dedup shuffles, and the DQ
rules. The useful signal was one level deeper.

### Step 2 — Which entity, and is it volume?

| Silver table | Time | Rows written | Throughput |
|---|---|---|---|
| **orders** | **803 s** | 2,524,909 | **20,896 rows/s** |
| sales | 117 s | 2,982,494 | 189,368 rows/s |
| customers | 116 s | 90,784 unique | 201,992 rows/s |
| products | 73 s | ~1,862 | 275,444 rows/s |

This ruled out the obvious explanations. `sales` wrote **more** rows in 117 s.
`customers` collapsed 2.9M duplicates and still finished in 116 s. So the cost was not
row count, not MERGE volume, and not deduplication — it was something specific to
`orders`.

### Step 3 — What is unique about `orders`?

It is the only entity that parses dates. `superstore_silver_module` built
`order_date_dt` / `ship_date_dt` for the `ship_date < order_date` business rule using:

```python
date_formats = ["d/M/yyyy", "dd-MM-yyyy", "yyyy-MM-dd", "dd/MM/yyyy", ...]  # 7 formats
coalesce(*[try_to_date(col(c), fmt) for fmt in date_formats])
```

The same list was **duplicated** in the casting step (Step 6b), which re-parsed the
same two columns from scratch.

Source dates are `dd-MM-yyyy` — **second** in the list. `coalesce` short-circuits on
the first non-null, so every row attempted `d/M/yyyy`, failed, then succeeded on the
second format. Across 3M rows × 2 columns × 2 steps that is roughly **12 million parse
attempts, a quarter of them guaranteed failures**.

---

## The fix

A single `DATE_FORMATS` constant, with the actual source format first, referenced from
both call sites (`src/superstore_silver/superstore_silver_module.py`):

```python
# Source dates are dd-MM-yyyy. That format is listed FIRST so the common case
# short-circuits the coalesce instead of failing a parse attempt on every row.
DATE_FORMATS = [
    "dd-MM-yyyy",
    "yyyy-MM-dd",
    "d/M/yyyy",
    "dd/MM/yyyy",
    "yyyy/MMM/d",
    "yyyy MMM d",
    "d MMMM yyyy",
]
```

**Why reordering is safe here:** changing the order of a `coalesce` of parsers changes
results whenever two formats can match the same string with *different* meanings. In
this set the dash formats (`dd-MM-yyyy`, `yyyy-MM-dd`) and slash formats (`d/M/yyyy`,
`dd/MM/yyyy`) are separated by their delimiter, and the 2-vs-4-digit groups keep the
dash pair disjoint — so no input can match two formats with different results. **This
assumption must be rechecked if a format is added.**

---

## Results

Measured on identical input (`read_rows` and `inserted_rows` identical across runs, so
the comparison is like-for-like):

### Silver `orders`

| | Before | After | |
|---|---|---|---|
| duration | 803 s | **158 s** | **5.1× faster** |
| throughput | 20,896 rows/s | **104,479 rows/s** | **5.0×** |
| read_rows | 3,029,880 | 3,029,880 | identical |
| inserted_rows | 2,524,909 | 2,524,909 | identical |

### Whole pipeline

| Source rows | Before | After | |
|---|---|---|---|
| 1,000,000 | 12 min 00 s | 8 min 32 s | −29% |
| 3,000,000 | 23 min 53 s | **11 min 38 s** | **−51%** |

### The unexpected part

The predicted gain was ~2× on date work: the change removes one of two parse attempts.
The actual gain was **5×**, because a *failed* `try_to_date` is far more expensive than
a successful one — the JVM's `DateTimeFormatter` constructs and throws an exception
internally on failure. The fix did not eliminate half the work; it eliminated the
expensive half.

---

## Where the time goes now

From the two post-fix data points (512 s at 1M rows, 698 s at 3M):

- **~93 s per million rows** of actual data processing
- **~7 minutes fixed overhead** — 17 serverless task cold starts

At 3M rows that is roughly **60% orchestration, 40% data**, and scaling is now **linear**
rather than superlinear. The pipeline is orchestration-bound again, which is the
relevant context for any further optimization.

---

## Deferred: parse dates once

`order_date` / `ship_date` are still parsed twice — once in the DQ step and again in the
casting step. Reusing the DQ-step columns would remove one parse.

**Not done, deliberately.** After the reorder, `orders` (158 s) sits only ~40 s above
`sales` (117 s), which does four regexes and four numeric casts and parses no dates. So
the entire remaining date overhead is ~40 s, and removing one of two parses might recover
half of it: **~20 s out of a 698 s run, about 3%.**

Against that: the `_dt` columns do not survive to the casting step, so this is a
restructure rather than a one-line change; it touches inline logic inside
`bronze_to_silver_prod` that the pure-function unit tests do not cover; and a regression
was already introduced in that exact function during this work (a business-rule guard was
commented out along with the code below it, causing the date rule to run for every entity).

Given the pipeline is orchestration-bound, **task consolidation is the higher-value lever**
than a further 3% on the data half.

---

## Reproducing the measurement

```sql
-- per-entity silver timings for the most recent run
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

When comparing runs, confirm `read_rows` and `inserted_rows` match — otherwise the runs
are not comparable (an incremental run processes far less than a full load).
