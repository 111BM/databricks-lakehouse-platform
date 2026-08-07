# ⚡ BACKFILL & REPLAY QUICK REFERENCE

## 1-Page Cheat Sheet for Reprocessing Operations

---

## 🧭 WHICH MODE DO I WANT?

The four modes answer different questions. Picking the wrong one is the most
common mistake, so start here.

| `run_mode` | Use when | Reads from | Bronze runs? |
|---|---|---|---|
| `incremental` | Normal scheduled load | source (new files) | yes |
| `backfill` | Data was **never loaded** — source outage, onboarding history | source (re-read, windowed) | yes |
| `replay` | Data is loaded but **wrong**, because the logic changed | existing Bronze | **no** |
| `full_refresh` | Replay with no window — migration, rebuild | existing Bronze | **no** |

**Backfill = missing data, same logic. Replay = existing data, new logic.**

If you deployed a code fix and existing tables are now wrong, that is a
**replay**. Bronze already has the raw data; re-acquiring it from the source is
wasted work, and once the vendor ages those files out it is not possible at all.

⚠️ **The window filters INGESTION date, not business date.** `start_date=2024-05-01`
means "rows that *arrived* on May 1st", not "orders placed on May 1st". There is
no business-date window.

⚠️ **`full_refresh` is disabled.** Every orchestrator passes
`allow_full_refresh=False`, so it raises. Enabling it takes a code change, not a
parameter — deliberately, because an unbounded reprocess should never be one
typo away.

---

## 📋 COMMON COMMANDS

> **Command shape.** `bundle run` takes the **resource key** (`superstore_data_platform`)
> plus `--target`, *not* the deployed job name. Job parameters are passed with
> `--params` as comma-separated `k=v` pairs — `--var` sets bundle variables at
> resolve time and will **not** override job parameters at run time.
>
> Parameter names are `run_mode`, `start_date`, `end_date`, `dry_run` —
> underscores, matching the widget keys the notebooks read. A misspelled or
> hyphenated parameter name is silently ignored (it never reaches the widget),
> so the job runs a normal incremental load. An **unrecognised mode value** now
> raises instead of degrading. Always confirm the resolved parameters in the
> run's task detail before trusting a reprocessing run.

### **Normal Incremental Run** (Default)
```bash
databricks bundle run superstore_data_platform --target dev
```

### **Dry-Run (Preview Impact)** — always do this first
```bash
databricks bundle run superstore_data_platform --target dev --params run_mode=replay,start_date=2024-05-01,end_date=2024-05-07,dry_run=true
```
> `dry_run` is honoured by **every task that writes** — source acquisition, all
> four layers, and the 11 serving notebooks. Each reports its impact and exits
> before writing. Combine it with any mode.

### **Replay a Week** (after a logic fix)
```bash
databricks bundle run superstore_data_platform --target dev --params run_mode=replay,start_date=2024-05-01,end_date=2024-05-07
```

### **Replay a Single Day**
```bash
databricks bundle run superstore_data_platform --target dev --params run_mode=replay,start_date=2024-05-01,end_date=2024-05-01
```

### **Backfill a Window the Source Never Delivered**
```bash
databricks bundle run superstore_data_platform --target dev --params run_mode=backfill,start_date=2024-05-01,end_date=2024-05-07
```

### **Backfill Yesterday**
```bash
YESTERDAY=$(python3 -c "import datetime;print(datetime.date.today()-datetime.timedelta(days=1))")
databricks bundle run superstore_data_platform --target dev --params run_mode=backfill,start_date=$YESTERDAY,end_date=$YESTERDAY
```
> `date -d "yesterday"` is GNU-only and fails on macOS; the Python form above is portable.

### **From the Databricks UI**
Run now → *Run with different parameters* → set `run_mode`, `start_date`,
`end_date`, `dry_run`. Same four parameters, same semantics.

---

## 🛡️ SAFETY CHECKLIST

Before ANY backfill or replay:

- [ ] Confirm you want **replay** (logic changed) vs **backfill** (data missing)
- [ ] Run with `dry_run=true` first
- [ ] Check estimated `rows_to_process` in logs — an order-of-magnitude surprise means the window is wrong
- [ ] Verify `partitions_affected` are the dates you expect
- [ ] Capture a baseline: row counts per Gold table, plus `bronze = silver + quarantine + audit`
- [ ] Start small (1-7 days)
- [ ] Test in **dev** before **prod**
- [ ] Monitor metrics during run
- [ ] Reconcile afterwards against the baseline — "it ran" is not verification

---

## 📊 MONITORING QUERIES

### **Check Last Backfill Status**
```sql
SELECT 
    layer,
    table_name,
    backfill_start_date,
    backfill_end_date,
    read_rows,
    good_rows,
    deduplicated_rows,
    run_status,
    duration_secs
FROM superstore_catalog.dev.metrics
WHERE load_type = 'BACKFILL'
ORDER BY start_ts DESC
LIMIT 10;
```

### **Verify No Duplicates**
```sql
-- Check Silver layer for duplicates
SELECT silver_customers_hash_id, COUNT(*) as cnt
FROM superstore_catalog.dev_silver.silver_customers
GROUP BY silver_customers_hash_id
HAVING cnt > 1;

-- Should return 0 rows
```

### **Check Backfill Progress**
```sql
SELECT 
    layer,
    COUNT(*) as total_tables,
    SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) as failed
FROM superstore_catalog.dev.metrics
WHERE master_run_id = '<your_master_run_id>'
GROUP BY layer;
```

---

## 🔧 TROUBLESHOOTING

### **Problem: "Unknown run_mode"**
**Solution:** The mode is misspelled. Valid values are `incremental`,
`backfill`, `replay`, `full_refresh`. This raises deliberately — it used to
fall back to `incremental` and run the wrong operation silently.

### **Problem: "start_date is required"**
**Solution:** `backfill` and `replay` need a window. If you genuinely want
everything, that is `full_refresh`, which is gated in code.

### **Problem: "Date range too large"**
**Solution:** Break into smaller chunks or increase `max_days` in code.

### **Problem: 0 rows processed**

For a **replay**, check that Bronze actually holds rows in the window:
```sql
SELECT MIN(ingestion_date), MAX(ingestion_date), COUNT(*)
FROM superstore_catalog.dev_bronze.bronze_superstore
WHERE ingestion_date BETWEEN '2024-05-01' AND '2024-05-07';
```

For a **backfill**, the window filters file modification time, not row content —
check the landing volume instead:
```python
[(f.name, f.modificationTime) for f in dbutils.fs.ls(landing_path)]
```
A file re-downloaded today carries today's modification time, so it will not
match a historical window.

### **Problem: Backfill too slow**
**Solutions:**
1. Increase `shuffle_partitions` in config
2. Use larger cluster
3. Break into smaller date ranges
4. Check for data skew:
```sql
SELECT ingestion_date, COUNT(*) as row_count
FROM superstore_catalog.dev_bronze.bronze_superstore
GROUP BY ingestion_date
ORDER BY row_count DESC;
```

### **Problem: Duplicate data**
**Solution:** Delete and re-run with fresh checkpoint:
```bash
# Delete checkpoint (in Databricks)
dbutils.fs.rm("/checkpoint/path/_backfill_20240501_20240507", recurse=True)

# Re-run backfill
```

---

## 📝 PARAMETERS REFERENCE

| Parameter | Values | Default | Required? |
|-----------|--------|---------|----------|
| `run_mode` | `incremental`, `backfill`, `replay`, `full_refresh` | `incremental` | No |
| `start_date` | `YYYY-MM-DD` | - | Yes (for `backfill` and `replay`) |
| `end_date` | `YYYY-MM-DD` | Today | No |
| `dry_run` | `true`, `false` | `false` | No |

An unrecognised `run_mode` **raises**. It used to fall back to `incremental`,
which meant a typo ran a normal load while the job reported success.

Omitting `start_date` on a `backfill` or `replay` also raises. A missing window
must never mean "everything" — that is what `full_refresh` is for, and it is
gated.

---

## ⚠️ DANGER ZONE

### **Full Refresh** (Reprocess ALL data)

**❌ DO NOT USE unless absolutely necessary!**

1. First, enable in code:
```python
# In each orchestrator
backfill_config = get_backfill_config(
    dbutils, 
    allow_full_refresh=True  # <-- DANGEROUS!
)
```

2. Then run:
```bash
databricks bundle run superstore_data_platform --target dev --params run_mode=full_refresh
```

**Consequences:**
- Processes ALL historical data
- Can cost $100s-$1000s in compute
- Takes hours to days
- Might hit cluster limits

**When to use:**
- Major schema change across all data
- Complete pipeline rewrite
- Data corruption requiring full rebuild

---

## 📊 METRICS FIELDS

Every layer writes per-entity metrics. The field that records the run mode is
`load_type`, on the Bronze, Silver and Gold metrics tables:

| `load_type` | Meaning |
|-------|-------------|
| `INITIAL_LOAD` | First population of the target table |
| `INCREMENTAL` | Normal watermark-based load |
| `BACKFILL` | Windowed re-acquisition from source |
| `REPLAY` | Windowed re-derivation from existing Bronze |
| `FULL_REFRESH` | Unbounded re-derivation |
| `NO_DATA` | Source missing or empty |

So "which runs were replays?" is a query, not an investigation:

```sql
SELECT layer_run_id, table_name, load_type, read_rows, duration_secs
FROM superstore_catalog.dev_metrics.silver_etl_metrics
WHERE load_type IN ('REPLAY', 'BACKFILL', 'FULL_REFRESH')
ORDER BY start_ts DESC;
```

Dry runs write no metrics rows at all — each task exits before its write — so
the absence of a metrics row for a run is itself the confirmation that nothing
was written.

---

## 📦 EXAMPLE SCENARIOS

### **Scenario 1: Missed Files**
**Problem:** Ingestion job failed on May 1st, missed files

This is a **backfill** — the data was never loaded, so Bronze must re-acquire it.

**Solution:**
```bash
databricks bundle run superstore_data_platform --target dev --params run_mode=backfill,start_date=2024-05-01,end_date=2024-05-01
```
> The window filters `source_file_modification_time` — a property of the file,
> stable across re-reads. It deliberately does not filter `ingestion_date`,
> which is re-stamped on every read and so can never match a past window.

### **Scenario 2: Fixed Data Quality Rule**
**Problem:** Fixed regex validation, need to re-validate March data

This is a **replay** — Bronze is unchanged and correct; only the derived layers
are wrong. Bronze skips itself, and Silver/Gold re-derive from what is already
there.

**Solution:**
```bash
databricks bundle run superstore_data_platform --target dev --params run_mode=replay,start_date=2024-03-01,end_date=2024-03-31
```
> Expect quarantine counts to fall and silver counts to rise by the same amount.
> If they don't, the fix didn't do what you thought.

### **Scenario 3: Business Logic Change**
**Problem:** Changed SCD Type 2 logic, need to recompute dimensions

**Solution:**
```bash
# A logic change means replay, not backfill: Bronze is untouched and correct.
databricks bundle run superstore_data_platform --target dev --params run_mode=replay,start_date=2024-01-01,end_date=2024-12-31
```
> Replaying Gold dimensions over already-historized rows does not create
> duplicate SCD2 versions: hash-based change detection means unchanged rows do
> not churn, which the integration test proves by running the pipeline twice.

> **Why not just the Gold task?** `bundle run --only superstore_gold_layer_dimensions`
> runs that task alone — but it skips `superstore_pipeline_master_run_id_init`, whose
> output the Gold task reads via
> `{{tasks.superstore_pipeline_master_run_id_init.values.master_run_id}}`. That
> reference cannot resolve, so the task fails. Use `--only` only for task groups that
> include their own upstream dependencies.

### **Scenario 4: Monthly Historical Load**
**Problem:** Need to load historical data, one month at a time

**Solution:**
```bash
# Script to backfill 6 months, one month per run
for month in 1 2 3 4 5 6; do
  start_date=$(printf "2024-%02d-01" "$month")
  end_date=$(python3 -c "
import datetime,sys
d=datetime.date.fromisoformat(sys.argv[1])
nxt=(d.replace(day=28)+datetime.timedelta(days=4)).replace(day=1)
print(nxt-datetime.timedelta(days=1))" "$start_date")

  echo "Backfilling $start_date to $end_date"

  databricks bundle run superstore_data_platform --target dev --params run_mode=backfill,start_date=$start_date,end_date=$end_date
done
```
> `bundle run` blocks until the run completes and exits non-zero on failure, so no
> `sleep` is needed between months — and unlike a fixed sleep, a failed month stops
> the loop if you add `set -e`.

---

## 📞 SUPPORT

**If backfill fails:**

1. Check logs in Databricks job run
2. Look for error in metrics table:
```sql
SELECT * FROM superstore_catalog.dev.metrics
WHERE run_status = 'FAILED'
ORDER BY start_ts DESC
LIMIT 5;
```
3. Review the TROUBLESHOOTING section above
4. Check `notes` field in metrics for specific errors

---

## 📚 RELATED DOCS

- [Architecture Overview](../README.md) - System architecture
- Backfill implementation lives in `src/superstore_shared_utilities/superstore_backfill_utils.py`
  (`get_backfill_config`, `get_incremental_with_backfill`, `validate_backfill_impact`)
- Databricks Docs: [Delta Lake Time Travel](https://docs.databricks.com/delta/history.html)

---

**Last Updated:** May 2026

**Maintained By:** Data Engineering Team