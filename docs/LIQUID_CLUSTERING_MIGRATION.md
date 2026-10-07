# Liquid Clustering Migration — from Hive partitions to a layout the pipeline converges

> **Status (2026-10-07): complete in every environment.** dev and prod fully migrated
> (prod Bronze on run `261598367632046`, prod Gold on replay `710265257759139`); qa
> creates its tables clustered. See [Verification](#verification).

## What

Every Bronze and Gold table now uses **Liquid Clustering** (`CLUSTER BY`) instead of
Hive-style partitioning (`PARTITIONED BY`). The layout is declared in config and the
pipeline **converges** each table to it on every run, through one shared module,
`src/superstore_shared_utilities/superstore_liquid_clustering.py`.

| Table | Before | After (`CLUSTER BY`) | How it got there |
|---|---|---|---|
| `superstore_raw` | `PARTITIONED BY (ingestion_date)` | `bronze_ingestion_ts` | converted **in place** |
| Bronze `customers` | `PARTITIONED BY (bronze_ingestion_ts)` | `bronze_ingestion_ts, customer_id` | **rewritten** once |
| Bronze `products` | same | `bronze_ingestion_ts, product_id` | rewritten once |
| Bronze `orders` | same | `bronze_ingestion_ts, order_id` | rewritten once |
| Bronze `sales` | same | `bronze_ingestion_ts, order_id, product_id` | rewritten once |
| Gold `dim_customers` | `PARTITIONED BY (region)` | `region, state` | converted in place |
| Gold `dim_products` | `PARTITIONED BY (category)` | `category, sub_category` | converted in place |
| Gold `facts_orders` | none (`z_order_cols` declared, never used) | `customer_id, order_date` | `ALTER TABLE … CLUSTER BY` |
| Gold `facts_sales` | none (same) | `order_id, product_id` | `ALTER TABLE … CLUSTER BY` |

Config keys: `cluster_by_columns` (Bronze) and `cluster_by_cols` (Gold) replace
`partition_col`, `z_order_columns` and `z_order_cols`.

Silver, quarantine, audit, metrics and mart tables were never partitioned and are
unchanged.

## Why

1. **The Bronze entities were partitioned on a timestamp.** `bronze_ingestion_ts` is
   stamped once per run, so every run created a new partition holding that run's rows
   in their own small files — and nothing ever merged partitions. That is the textbook
   small-file anti-pattern, and it grows with every run.
2. **Partitioning is the wrong tool at this size.** The tables hold about 1M rows. Current
   Databricks guidance is not to partition tables under about 1 TB; Liquid Clustering
   or no layout at all. The Gold dimensions were split into one folder per region or
   category for no benefit.
3. **Clustering keys can change; partitions cannot.** Partitioning is fixed at creation
   and changing it means rewriting the table. Clustering keys change with
   `ALTER TABLE … CLUSTER BY`, and new data follows the new keys.
4. **Predictive Optimization maintains it.** Clustered managed tables are re-clustered
   and compacted in the background. The pipeline schedules no `OPTIMIZE` — consistent
   with [DEAD_OPTIMISATION_CODE_REMOVAL.md](DEAD_OPTIMISATION_CODE_REMOVAL.md).

## When

2026-10-07, directly after the dead Z-ORDER helpers were removed. An optimisation review
flagged Hive partitioning as the remaining legacy layout.

## Where

| Part | Files |
|---|---|
| Convergence logic | `src/superstore_shared_utilities/superstore_liquid_clustering.py` |
| Bronze | `bronze_ingest_superstore_module_01.py` and `bronze_entity_superstore_module_02.py` (writers no longer `partitionBy`); `superstore_bronze_layer_ETL_pipeline_orchestrator.ipynb` (convergence calls) |
| Gold | `create_gold_table_if_not_exists` in both Gold frameworks; both Gold orchestrators |
| Config | `superstore_bronze_config.yaml`, `superstore_gold_dimension_config.yaml`, `superstore_gold_facts_config.yaml` |
| Tests | `tests/unit/shared/test_liquid_clustering.py` (17 tests) |

## Which design, and why not the alternatives

**Chosen: the pipeline converges each table on every run.** Idempotent: the first run in
each environment converts, every later run reads `DESCRIBE DETAIL`, finds the layout
right, and does nothing.

| Alternative | Rejected because |
|---|---|
| A one-off migration job | Runs once, then is dead code in the bundle — the problem the previous change removed. It also has to be run by hand in each environment, as the right identity. |
| Running the SQL by hand in prod | Prod tables are owned by the `superstore-ci-prod` service principal; a person would need ownership or admin, and the change would leave no trace in code. |
| `CREATE OR REPLACE … AS SELECT` for every table | Changes the Delta table id. Unsafe for `superstore_raw`, which an Auto Loader stream writes into. |
| `df.write.clusterBy(...)` at creation only | Handles new tables but never converts existing ones, so every existing environment would keep its partitions. |

Converging from the pipeline means each environment migrates itself on its next run,
**as the identity that owns its tables** (the service principal in qa and prod), and a
new environment is created clustered.

## How

`converge_liquid_clustering(spark, logger, table, keys, allow_rewrite=...)`:

```
table missing                      -> TABLE_MISSING       (created later in the run)
clustered on exactly these keys    -> ALREADY_CLUSTERED   (every run after the first)
not partitioned, other/no keys     -> ALTER TABLE t CLUSTER BY (keys)            KEYS_SET
partitioned                        -> ALTER TABLE t REPLACE PARTITIONED BY
                                        WITH CLUSTER BY (keys)                  CONVERTED_IN_PLACE
   ...refused for a TIMESTAMP partition column, and allow_rewrite
                                   -> CREATE OR REPLACE TABLE t CLUSTER BY (keys)
                                        AS SELECT * FROM t                      REWRITTEN
```

The decision (`plan_clustering`) is a pure function, unit tested. Key order counts as part
of the layout. More than four keys, an empty list or a duplicate fails before any table
is touched.

**Where it runs:**

- `superstore_raw`: before the Auto Loader stream (so the stream never writes into a table
  about to change layout) and again after it (a fresh environment's table is created by
  the stream). `allow_rewrite=False`.
- Bronze entities: after each entity's append, whether or not it had rows.
  `allow_rewrite=True`.
- Gold dimensions and facts: inside `create_gold_table_if_not_exists`, so a new table is
  clustered before its first MERGE. `allow_rewrite=True`. This runs only when the run
  has rows for that table, so an existing Gold table converts on the first run that
  brings it data.

### Evidence gathered before writing the code

Each operation was tried on scratch copies of dev tables first (2026-10-07; scratch tables
dropped afterwards):

| Test | Result |
|---|---|
| `CREATE OR REPLACE … CLUSTER BY … AS SELECT * FROM itself` on a timestamp-partitioned copy | ✅ rows equal (1,010,456), grants kept, history kept (v0 → v1) — ⚠️ **Delta table id changed** |
| `ALTER TABLE … CLUSTER BY` on an unpartitioned table | ✅ metadata only, same table id |
| `ALTER TABLE … CLUSTER BY` on a partitioned table | ❌ `DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED` — the error names the in-place conversion |
| `ALTER TABLE … REPLACE PARTITIONED BY WITH CLUSTER BY` on a **TIMESTAMP** partition | ❌ cannot generate stats for `timestamp`; the setting that skips it is `CONFIG_NOT_AVAILABLE` on serverless |
| Same on a **DATE** partition (raw) | ✅ in place, **same table id** |
| Same on a **STRING** partition (Gold dimension) | ✅ in place, same table id |
| Changing keys after conversion (`ingestion_date` → `bronze_ingestion_ts`) | ✅ |

The table-id finding drove the design: the only table a stream depends on (`superstore_raw`)
is partitioned on a DATE and converts in place; the tables that must be rewritten (the
TIMESTAMP-partitioned Bronze entities) are batch-only.

### Rollout

| Environment | How it migrates |
|---|---|
| dev | deploy, then a normal run (Bronze) and a replay run (Gold, which needs rows) |
| qa | the integration suite recreates every table, so it exercises the **create** path: new tables, `ALTER … CLUSTER BY` |
| prod | the first run after deploy, as `superstore-ci-prod`. Bronze converts on that run; Gold converts on the first run that brings rows |

## Before and after

### dev (2026-10-07, run `159524210874323`)

| Table | Before | After |
|---|---|---|
| `superstore_raw` | partitions `[ingestion_date]`, 2 files, id `9e988319` | clustering `[bronze_ingestion_ts]`, 2 files, **id `9e988319` (unchanged)** |
| `customers` | partitions `[bronze_ingestion_ts]`, 2 files | clustering `[bronze_ingestion_ts, customer_id]`, 1 file, new id |
| `orders` | same | clustering `[bronze_ingestion_ts, order_id]`, 1 file |
| `products` | same | clustering `[bronze_ingestion_ts, product_id]`, 1 file |
| `sales` | same | clustering `[bronze_ingestion_ts, order_id, product_id]`, 1 file |
| Row counts | 1,010,456 each | 1,010,456 each — **unchanged** |

Gold, replay run `188360778655328` (Gold converts only on a run that brings it rows):

| Table | Before | After |
|---|---|---|
| `dim_customers` | partitions `[region]` | clustering `[region, state]`, **same table id**, 87,522 rows |
| `dim_products` | partitions `[category]` | clustering `[category, sub_category]`, same table id, 51,653 rows |
| `facts_orders` | no layout | clustering `[customer_id, order_date]`, same id, 842,280 rows |
| `facts_sales` | no layout | clustering `[order_id, product_id]`, same id, 994,793 rows |

After the replay every data-quality check passed: reconciliation balanced on all four
entities (1,010,456), zero orphaned facts, placeholder exposure unchanged.

Dev has little run history, so its file counts are small. Prod is where the per-run
partitions accumulated; its numbers belong here after its first run.

### prod — before (2026-10-07, read-only)

| Table | Layout | Files | Rows |
|---|---|---|---|
| `superstore_raw` | partitions `[ingestion_date]` | 3 | 1,010,534 |
| `customers` / `orders` / `products` / `sales` | partitions `[bronze_ingestion_ts]` | 3 each | 1,010,534 each |
| `dim_customers` | partitions `[region]` | 5 | 87,938 |
| `dim_products` | partitions `[category]` | 4 | 52,050 |
| `facts_orders` / `facts_sales` | none | 3 each | 841,923 / 995,038 |

**Read this honestly: the damage was small.** Each Bronze entity had 3 partitions,
not hundreds, because a partition is created only by a run that brings new data, and
prod has had three (2026-08-16, 09-26, 10-01). The defect was in the design, not yet
in the numbers: with a weekly delivery it adds 52 never-merged partitions per table
per year, indefinitely. The migration removes it before it shows, rather than after a
slowdown — so this change has no runtime headline, and should not be presented as one.

### prod — after (2026-10-07, run `261598367632046`, as `superstore-ci-prod`, 5 min, green)

| Table | After | Table id |
|---|---|---|
| `superstore_raw` | clustering `[bronze_ingestion_ts]`, 3 files | `04c1212c` — **unchanged** (in place; Auto Loader unaffected) |
| `customers` | clustering `[bronze_ingestion_ts, customer_id]`, **1 file** (was 3) | new (rewritten) |
| `orders` | clustering `[bronze_ingestion_ts, order_id]`, 1 file | new |
| `products` | clustering `[bronze_ingestion_ts, product_id]`, 1 file | new |
| `sales` | clustering `[bronze_ingestion_ts, order_id, product_id]`, 1 file | new |

Row counts unchanged (1,010,534 per Bronze table). Reconciliation balanced on all four
entities, zero orphaned facts, all five prod alerts OK afterwards. The Bronze task took
102 s including the one-time rewrite. Grants are held at schema level and were unaffected.

Gold had no new rows on that run, so it was converted by a replay of the last delivery's
window (`run_mode=replay`, 2026-10-01; run `710265257759139`, 7 min, green). A replay
re-derives Silver and Gold from Bronze and is idempotent, so it changes no data:

| Table | After | Table id |
|---|---|---|
| `dim_customers` | clustering `[region, state]` (was partitions `[region]`), 87,938 rows | `a5eb0c6a` — unchanged (in place) |
| `dim_products` | clustering `[category, sub_category]` (was `[category]`), 52,050 rows | `31bdd09b` — unchanged |
| `facts_orders` | clustering `[customer_id, order_date]`, 841,923 rows | `11aa45cc` — unchanged |
| `facts_sales` | clustering `[order_id, product_id]`, 995,038 rows | `9215a785` — unchanged |

Row counts identical to before; all DQ checks passed and all five prod alerts OK.

## A bug the migration found

The first dev replay failed in `superstore_gold_layer_facts`:

```
[DELTA_COLUMN_NOT_FOUND_IN_SCHEMA] Couldn't find column customer_id in: root
 |-- order_id ... |-- product_id ... |-- sales ...
```

`facts_sales` had been configured with `z_order_cols: [customer_id, product_id]` — but
the sales fact has **no `customer_id` column**. The key was wrong from the start, and
nothing noticed, because the Z-order helper that would have used it never ran. Config
that nothing reads cannot be wrong in any way anyone will see.

Fixed: `facts_sales` clusters on its MERGE keys, `order_id, product_id`. And
`converge_liquid_clustering` now checks every declared key against the table's columns
**before** any `ALTER`, so a wrong key fails by name (`missing_cluster_columns`, unit
tested) instead of partway through a conversion. The failed run left nothing
half-done: `facts_orders` had been clustered correctly and `facts_sales` was untouched.

## Things to know

- **One-time cost.** The run that converts pays for rewriting the four Bronze entities
  (about 1M rows each). Every later run pays one `DESCRIBE DETAIL` per table.
- **The rewrite starts a new Delta table id for the Bronze entities.** History and grants
  survive (verified). Anything that tracked those tables by id — a stream, a Delta
  Sharing share — would need to restart. Nothing does today; keep it that way, or
  set `allow_rewrite=False` for that table.
- **Existing rows are not re-sorted by the conversion itself.** The in-place routes change
  metadata; Predictive Optimization clusters existing files in the background, and new
  writes follow the keys.
- **Choosing keys:** columns that queries and MERGEs **filter on**. Never a hash column:
  hash values are uniformly distributed, so every file's min/max spans the whole range
  and nothing is skipped (see [DEAD_OPTIMISATION_CODE_REMOVAL.md](DEAD_OPTIMISATION_CODE_REMOVAL.md)).

## Verification

- [x] Unit tests: 288 passed (17 new)
- [x] dev Bronze: converted, rows unchanged, raw table id unchanged
- [x] dev Gold: dimensions converted in place, fact keys set, all DQ checks passed
- [x] qa integration suite green, run `1056823645949824`; its fresh tables were created clustered
- [x] prod: deployed, run `261598367632046` green, Bronze converted, before/after recorded above, all alerts OK
- [x] prod Gold: converted by replay `710265257759139`, rows unchanged, all checks and alerts OK
