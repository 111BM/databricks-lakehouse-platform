# Dead Optimisation Code Removal — the pipeline claims only the optimisations it runs

> **Status (2026-10-07): done.** About 360 lines deleted, no behaviour changed. Verified
> by dev run `703254340844608`, qa integration run `706611561305372` and prod run
> `261598367632046`, all green.

## What

Removed optimisation code that was written, imported and documented, but **never
executed**, and corrected the text that described it as running.

| Removed | Where | Why it was dead |
|---|---|---|
| `optimize_zorder_bronze_tables` | `src/superstore_bronze/bronze_entity_superstore_module_02.py` | Imported by the Bronze orchestrator; the call was commented out |
| `optimize_gold_table`, `vacuum_gold_table` | both Gold frameworks (`superstore_gold_dimension_framework.py`, `superstore_gold_facts_framework.py`) | Imported by both Gold orchestrators; never called |
| `max_rows_per_bucket` parameter and config key | Gold dimension framework, its orchestrator (5 lines), `superstore_gold_dimension_config.yaml` | Passed into `merge_into_gold_table_scd2` and `collect_metrics`; **neither function read it**. The docstring promised "bucketed merges" that did not exist |
| `z_order_cols` read in the Gold facts orchestrator | `superstore_gold_facts_ETL_pipeline_orchestrator.ipynb` | Assigned, never used |

Text corrected (module docstrings, orchestrator headers, `README.md`,
`tests/unit/README.md`): "Z-Order during off-peak hours", "dynamically repartitions",
"Performs Z-ordering", "vacuuming old data", and the claim that Bronze entities are
partitioned by `ingestion_date` (they are partitioned by `bronze_ingestion_ts`).

**Not changed:** the `z_order_cols` / `z_order_columns` keys in the Gold and Bronze
configs. The Gold dimension orchestrator still uses the first `z_order_cols` entry as
the table's partition column, and those keys are the input for the Liquid Clustering
migration that follows this change. (Done in the next change: they became
`cluster_by_columns` / `cluster_by_cols` — see
[LIQUID_CLUSTERING_MIGRATION.md](LIQUID_CLUSTERING_MIGRATION.md).)

## Why

1. **The code said one thing and did another.** A reader — a reviewer, an
   interviewer, the next engineer — sees `OPTIMIZE … ZORDER BY` and assumes the
   pipeline depends on it. It never ran. Documentation that describes behaviour the
   system does not have is a defect, not a style problem.
2. **The work is already done by the platform.** Unity Catalog **Predictive
   Optimization** is enabled at the metastore level and runs `OPTIMIZE` and `VACUUM`
   on these managed tables automatically. Running them per-load would duplicate it and
   pay compaction cost more often than fragmentation is created.
3. **Z-ORDER is the legacy layout tool.** Current Databricks guidance for new tables is
   Liquid Clustering (`CLUSTER BY`), which Predictive Optimization maintains
   incrementally. Keeping Z-ORDER helpers "for workspaces without PO" kept a path this
   project will never take.
4. **An unused parameter that names a feature is a false claim.** `max_rows_per_bucket`
   was configured, passed through four calls, and described in a docstring as splitting
   large merges — none of which happened.

## When

2026-10-07, after an optimisation review of the codebase found the helpers unwired.
They predate Predictive Optimization being relied on; the decision not to call them
was recorded in the README, but the code itself was never removed.

## Where

| Layer | Files |
|---|---|
| Bronze | `src/superstore_bronze/bronze_entity_superstore_module_02.py`, `superstore_orchestrator/layer_orchestrator/superstore_bronze_layer_ETL_pipeline_orchestrator.ipynb` |
| Gold dimensions | `src/superstore_gold/core/superstore_gold_dimension_framework.py`, `superstore_orchestrator/layer_orchestrator/superstore_gold_dimensional_ETL_pipeline_orchestrator.ipynb`, `configs/superstore_gold_config/superstore_gold_dimension_config.yaml` |
| Gold facts | `src/superstore_gold/core/superstore_gold_facts_framework.py`, `superstore_orchestrator/layer_orchestrator/superstore_gold_facts_ETL_pipeline_orchestrator.ipynb` |
| Docs | `README.md` (Table maintenance), `tests/unit/README.md` (untested-functions table) |

## Which optimisations remain (and do run)

| Technique | Where |
|---|---|
| Serverless `performance_target: PERFORMANCE_OPTIMIZED` | both job YAMLs |
| Auto Loader, `trigger(availableNow=True)`, checkpoints | Bronze raw ingest |
| MERGE upserts with row-hash change detection | Silver, Gold |
| Date-parse format ordering (803 s → 158 s) | Silver — see [PERFORMANCE_INVESTIGATION.md](PERFORMANCE_INVESTIGATION.md) |
| 11 Spark actions collapsed to 2 (no `cache()` on Serverless) | Silver — see [SILVER_ACTION_COLLAPSE.md](SILVER_ACTION_COLLAPSE.md) |
| `broadcast()` of the Silver key set | Gold dimension soft deletes |
| `OPTIMIZE` / `VACUUM` | Predictive Optimization (platform, not code) |

## How

1. Deleted the three helper functions and the unused parameter.
2. Removed their imports, the commented-out call, the unused config reads and the
   `max_rows_per_bucket` config key.
3. Rewrote each docstring and notebook header that described them, so every
   remaining claim matches code that runs.
4. Checked every orchestrator notebook still parses, and ran the unit suite.

No pipeline behaviour changes: nothing removed was ever executed.

## Before and after

| | Before | After |
|---|---|---|
| Table maintenance code in the pipeline | 3 helpers (≈ 300 lines), imported, never called | none — Predictive Optimization, stated once |
| Docstrings claiming Z-ORDER, VACUUM, repartitioning, bucketed merges | 7 places across 5 files | 0 |
| `max_rows_per_bucket` | config → orchestrator → 2 functions → unused | gone |
| What a reader believes the pipeline does | more than it does | what it does |

## One lesson worth keeping

The Gold dimension helper had at one point Z-ordered by the **SCD2 hash column**. That
can never skip data: hash values are uniformly distributed, so every file's min/max
spans the whole range and no file is pruned. Layout keys must be columns that queries
**filter on** (region, state, natural keys) — the same rule applies to Liquid
Clustering keys.

## Verification

- [x] Unit tests green locally and in CI
- [x] dev job run green (`703254340844608`)
- [x] qa integration suite green (`706611561305372`)
- [x] prod deploy, run green (`261598367632046`)
