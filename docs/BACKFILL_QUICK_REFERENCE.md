# ⚡ BACKFILL QUICK REFERENCE

## 1-Page Cheat Sheet for Common Backfill Operations

---

## 📋 COMMON COMMANDS

> **Command shape.** `bundle run` takes the **resource key** (`superstore_data_platform`)
> plus `--target`, *not* the deployed job name. Job parameters are passed with
> `--params` as comma-separated `k=v` pairs — `--var` sets bundle variables at
> resolve time and will **not** override job parameters at run time.
>
> Parameter names are `backfill_mode`, `start_date`, `end_date`, `dry_run` —
> underscores, matching the widget keys the notebooks read. A misspelled or
> hyphenated parameter does **not** error; it is silently ignored and the job
> runs a normal incremental load. Always confirm the resolved parameters in the
> run's task detail before trusting a backfill.

### **Normal Incremental Run** (Default)
```bash
databricks bundle run superstore_data_platform --target dev
```

### **Dry-Run (Preview Impact)**
```bash
databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=2024-05-01,end_date=2024-05-07,dry_run=true
```

### **Backfill Last Week**
```bash
databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=2024-05-01,end_date=2024-05-07
```

### **Backfill Last Month**
```bash
databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=2024-04-01,end_date=2024-04-30
```

### **Backfill Single Day**
```bash
databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=2024-05-01,end_date=2024-05-01
```

### **Backfill Yesterday**
```bash
YESTERDAY=$(python3 -c "import datetime;print(datetime.date.today()-datetime.timedelta(days=1))")
databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=$YESTERDAY,end_date=$YESTERDAY
```
> `date -d "yesterday"` is GNU-only and fails on macOS; the Python form above is portable.

---

## 🛡️ SAFETY CHECKLIST

Before ANY backfill:

- [ ] Run with `dry_run=true` first
- [ ] Check estimated `rows_to_process` in logs
- [ ] Verify `partitions_affected` are correct dates
- [ ] Start small (1-7 days)
- [ ] Test in **dev** before **prod**
- [ ] Monitor metrics during run

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

### **Problem: "Date range too large"**
**Solution:** Break into smaller chunks or increase `max_days` in code.

### **Problem: 0 rows processed**
**Check:**
```sql
SELECT MIN(ingestion_date), MAX(ingestion_date), COUNT(*)
FROM superstore_catalog.dev_bronze.bronze_superstore
WHERE ingestion_date BETWEEN '2024-05-01' AND '2024-05-07';
```

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
| `backfill_mode` | `incremental`, `date_range`, `full_refresh` | `incremental` | No |
| `start_date` | `YYYY-MM-DD` | - | Yes (if date_range) |
| `end_date` | `YYYY-MM-DD` | Today | No |
| `dry_run` | `true`, `false` | `false` | No |

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
databricks bundle run superstore_data_platform --target dev --params backfill_mode=full_refresh
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

**New fields in metrics table:**

| Field | Type | Description |
|-------|------|-------------|
| `load_type` | string | `"INCREMENTAL"` or `"BACKFILL"` |
| `backfill_start_date` | date | Start of backfill range |
| `backfill_end_date` | date | End of backfill range |
| `is_dry_run` | boolean | Was this a dry-run? |

---

## 📦 EXAMPLE SCENARIOS

### **Scenario 1: Missed Files**
**Problem:** Ingestion job failed on May 1st, missed files

**Solution:**
```bash
# Re-ingest May 1st files
databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=2024-05-01,end_date=2024-05-01
```

### **Scenario 2: Fixed Data Quality Rule**
**Problem:** Fixed regex validation, need to re-validate March data

**Solution:**
```bash
# Bronze data is unchanged - re-running the full pipeline is safe and idempotent
# (Auto Loader skips already-ingested files; Silver/Gold re-derive from Bronze).
databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=2024-03-01,end_date=2024-03-31
```

### **Scenario 3: Business Logic Change**
**Problem:** Changed SCD Type 2 logic, need to recompute dimensions

**Solution:**
```bash
# Re-run the full pipeline for the affected range. Bronze/Silver are idempotent,
# so the cost is time rather than correctness, and Gold recomputes from Silver.
databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=2024-01-01,end_date=2024-12-31
```

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

  databricks bundle run superstore_data_platform --target dev --params backfill_mode=date_range,start_date=$start_date,end_date=$end_date
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