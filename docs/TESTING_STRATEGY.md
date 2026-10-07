# Testing strategy — unit tests on pure functions, an end-to-end suite on the real pipeline

> Moved out of the top-level README on 2026-10-07 so the README stays a five-minute read. Content unchanged.

## Unit tests (`tests/unit/`) — fast, local, no cluster

Production logic is extracted into **pure functions** ("functional core, imperative shell") so the exact code that runs in production is testable on a local Spark session in seconds:

```bash
pip install -r tests/unit/requirements-unit.txt
pytest tests/unit/ -v
```

Organized by layer (`bronze/`, `silver/`, `gold/`, `shared/`): dedup semantics, DQ rule routing, hash integrity, SCD2 timeline construction, fact column preparation, backfill config parsing. CI blocks every deploy on these.

## Integration test (`tests/integration_databricks/`) — the real pipeline, end to end

A Databricks job (`superstore_integration_test`) that validates the platform the way production would fail:

```
seed dirty data → run REAL pipeline (SUPERSTORE_ENV=integration_test)
  → assert_bronze   (lossless ingest, renames, entity contracts, provenance)
  → assert_silver   (quarantine caught the RIGHT rows, reconciliation, dedup, hashing)
  → assert_gold_dim (SCD2 invariants: one current row, no overlapping ranges)
  → assert_gold_fact (grain uniqueness, referential integrity)
→ seed changed data (incl. a NEW source column) → run pipeline AGAIN
  → assert_scd2_change   (change historized, unchanged rows NOT churned — idempotency)
  → assert_schema_drift  (the new column is detected and recorded)
→ replay the window twice (Silver + Gold only)
  → assert_replay        (re-derivation is idempotent: replay == replay)
(every Silver run also reconciles and records the result to data_quality_checks)
(no end-of-run cleanup — the environment is reset at the START, so a
 failed OR passing run leaves its tables available for post-mortem)
```

Everything runs against isolated `integration_test_*` schemas and a dedicated volume — dev/qa/prod data is never touched. A failed assertion fails the job, which fails CI.

![Integration test job DAG on Databricks Serverless](images/integration_tests_DAG.png)

*The `superstore_integration_test` job: the real pipeline run twice (initial load, then an SCD2 change), asserting every layer in between.*

*The screenshot predates two changes and is left rather than retaken, since the DAG shape it shows is still the point. It depicts the **old** replay legs — two further full-pipeline invocations, since removed — and a trailing `cleanup` task that no longer exists; the environment is now reset at the start instead. Measured timings: **56.2 min** before scoping the replay legs, **36.6 min** after.*

```bash
databricks bundle run superstore_integration_test --target qa
```
