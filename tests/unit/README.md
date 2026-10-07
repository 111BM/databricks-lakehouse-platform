# Unit Tests

Fast, isolated tests that import the **real** production functions and run on
a local Spark session — **no Databricks cluster required**. The whole suite
runs in ~20s on a laptop or in CI.

## Setup (one-time)

Requires **Python 3.12** (PySpark does not yet fully support 3.13) and a JDK.

```bash
# from the project root
python3.12 -m venv .venv-tests
./.venv-tests/bin/python -m pip install -r tests/unit/requirements-unit.txt
```

## How to run

```bash
# from the project root
./.venv-tests/bin/python -m pytest tests/unit/ -v

# one layer
./.venv-tests/bin/python -m pytest tests/unit/silver/ -v

# fast (non-Spark) tests only
./.venv-tests/bin/python -m pytest tests/unit/shared tests/unit/bronze/test_sanitize_column.py -v
```

## Layout (organized by medallion layer)

```
tests/unit/
  conftest.py                      # local Spark, portable imports, fake dbutils
  shared/
    test_backfill_config.py        # get_backfill_config (date/mode validation)
    test_liquid_clustering.py      # plan_clustering, validate_cluster_columns
  bronze/
    test_sanitize_column.py        # module 01: sanitize_column
  silver/
    test_silver_dedup.py           # deduplicate_latest_wins (latest-wins)
    test_silver_hashing.py         # row_hash (idempotency key)
    test_silver_dq.py              # clean_string_columns, add_error_columns, add_is_valid
  gold/
    test_scd2_timeline.py          # dimension: compute_scd2_timeline
    test_prepare_fact_columns.py   # facts: prepare_fact_columns
```

## What is unit-tested vs. integration-tested

A function is **unit-testable** only if it is *pure*: DataFrame (or values)
in → DataFrame out, with no table reads/writes, Delta MERGE, or Auto Loader.
Functions that do I/O are verified by **integration tests** (run the real
pipeline on seeded data), not here.

| Layer | Module | Unit-tested (pure) | Integration-only (I/O) |
|-------|--------|--------------------|------------------------|
| Shared | backfill_utils | `get_backfill_config` ✅ | `get_incremental_with_backfill` (reads tables) |
| Shared | liquid_clustering | `plan_clustering` ✅, `validate_cluster_columns` ✅ | `converge_liquid_clustering` (DESCRIBE DETAIL, ALTER/REPLACE TABLE) |
| Bronze | 01 ingest | `sanitize_column` ✅ | `bronze_ingest_incremental` (Auto Loader, Delta write) |
| Bronze | 02 entity split | — (module reads a YAML at import + all funcs do table I/O) | `bronze_entity_incremental_append`, `collect_entity_metrics`, `get_last_processed_ts` |
| Silver | silver_module | `deduplicate_latest_wins` ✅, `row_hash` ✅, `clean_string_columns` ✅, `add_error_columns` ✅, `add_is_valid` ✅ | `bronze_to_silver_prod` shell (read/merge/write), `write_etl_metrics`; the bespoke `ship_date<order_date` rule stays inline |
| Gold | dimension | `compute_scd2_timeline` ✅ | `read_silver_table`, `merge_into_gold_table_scd2`, `handle_soft_deletes`, `collect_metrics` |
| Gold | facts | `prepare_fact_columns` ✅ | `get_incremental_silver_for_facts`, `merge_fact_into_gold`, `collect_fact_metrics` |

### Why some functions aren't here
- **Bronze module 02** loads `superstore_bronze_config.yaml` at *import time* and
  every function performs table I/O — it can't be imported or unit-tested
  locally. Its behavior is covered by the integration test that runs the real
  bronze pipeline.
- The big orchestration functions (`bronze_to_silver_prod`,
  `merge_into_gold_table_scd2`, …) mix logic with Delta I/O. The **pure logic
  was extracted** out of them (e.g. `deduplicate_latest_wins`, `row_hash`)
  precisely so it could be unit-tested; the remaining I/O shell is integration
  territory.

## Design notes
- **Real imports.** Every test imports the actual function production runs.
  Break the pipeline logic and the test goes red. (Verified: temporarily
  inverting the dedup/validation makes these tests fail.)
- **Portable.** No hard-coded `/Workspace/...` paths — `conftest.py` wires
  `src/` onto `sys.path`, so tests run on a laptop and in GitHub CI.
- **Functional core, imperative shell.** Pure transforms live in
  `silver_transformations.py` (and the existing pure gold functions); the
  I/O orchestration stays in the layer modules.
