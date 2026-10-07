# Architecture — layers, cross-cutting design, performance, repository layout

> Moved out of the top-level README on 2026-10-07 so the README stays a five-minute read. Content unchanged.

The diagram is in the [README](../README.md#architecture). This document is the detail behind it.

## Layers

| Layer | Modules | What it does |
|---|---|---|
| **Acquisition** | `bronze_source_acquisition` | Pulls new source files from an external HTTP feed (GitHub Contents API) into the environment's landing volume, standing in for a vendor drop. Idempotent by construction — downloads the set difference between the source listing and what has already landed, so re-runs land nothing. Distinguishes "no *new* files" (healthy) from "no files *at all*, and nothing ever landed" (fatal), because the latter used to exit cleanly and let a run that could not produce data report SUCCESS. Each env reads its own source folder; `integration_test` has no source configured and skips, since its data comes from the seed |
| **Bronze** | `bronze_ingest_superstore_module_01`, `bronze_entity_superstore_module_02` | Auto Loader (`cloudFiles`) incremental CSV ingest into a one-big-table `superstore_raw` with metadata enrichment (source file, ingestion ts), then splits into entity tables (customers, products, orders, sales) preserving row counts and provenance |
| **Silver** | `superstore_silver_module` + `superstore_silver_transformations` (pure functions) | Cleansing, conforming known source dialects to the canonical vocabulary *before* validating them ([docs/VALUE_STANDARDIZATION.md](VALUE_STANDARDIZATION.md)), null-business-key / regex / categorical / business-rule validation with **quarantine routing**, latest-wins deduplication with **audit trail**, SHA-256 row hashing, Delta MERGE upserts |
| **Gold** | `superstore_gold_dimension_framework`, `superstore_gold_facts_framework` | Config-driven **SCD2 dimensions** (one current row per key, closed non-overlapping validity ranges dated in processing time — [docs/SCD2_VALIDITY_DATING.md](SCD2_VALIDITY_DATING.md)) and incremental fact tables keyed on natural keys. Facts are **not** referentially enforced against dimensions — see [docs/REFERENTIAL_COMPLETENESS.md](REFERENTIAL_COMPLETENESS.md) |
| **Serving** | marts / features / metrics notebooks | Customer 360, sales daily, product performance marts; ML feature tables; business KPI views |

## Cross-cutting design

- **Git is the single source of truth** — every notebook, module, and YAML config is deployed by the bundle (`${workspace.file_path}` paths + runtime-derived `BUNDLE_ROOT`); nothing is hand-synced to the workspace.
- **Config-driven** — column contracts, DQ rules, and env-specific paths live in YAML (`configs/`), not code. Adding a column rule (null, regex, allowed values, severity) or an entity is a config change; cross-column business rules (`ship_date` before `order_date`) are still code.
- **Environment isolation** — `SUPERSTORE_ENV` (dev/qa/prod/integration_test) resolves schemas (`{env}_bronze`, …) and volume paths per environment via a single job parameter.
- **Idempotency, backfill & replay** — hash-based change detection, Auto Loader checkpoints, and four job parameters (`run_mode`: incremental / backfill / replay / full_refresh, plus `start_date`/`end_date` and `dry_run`). Backfill re-acquires from source; replay skips Bronze and re-derives Silver and Gold from the data already held; `dry_run` is honoured by every task that writes. Re-deriving is idempotent at every layer that holds derived data: Silver and Gold through their merge keys, quarantine and audit through a delete scoped to exactly the window a run re-reads — they were previously appended to, so each replay added a second copy of every dirty row and the reconciliation invariant over-counted (**[docs/RUN_MODE_IDEMPOTENCY.md](RUN_MODE_IDEMPOTENCY.md)**). The window itself now means one thing at every layer — Gold dimensions selected on Silver write time rather than Bronze ingestion time, which made replay silently select nothing (**[docs/GOLD_WINDOW_ALIGNMENT.md](GOLD_WINDOW_ALIGNMENT.md)**). Operator procedures: **[docs/BACKFILL_QUICK_REFERENCE.md](BACKFILL_QUICK_REFERENCE.md)**.
- **Observability** — structured logging (`superstore_logger`) with `master_run_id`/`layer_run_id` traceability, per-entity metrics tables per layer, and job failures delivered to **email and a Slack channel** (`#superstore-data-platform-alerts`) through a Databricks notification destination — verified 2026-10-03 by a deliberately failed run. Data-quality checks are *recorded*, not merely logged: every run writes reconciliation, orphaned facts and placeholder exposure to `{env}_metrics.data_quality_checks`, and four SQL Alerts — bundle resources, like the freshness SLA ([FRESHNESS_ALERT.md](FRESHNESS_ALERT.md)) — notify email and Slack when reconciliation is unbalanced, orphans appear, the source schema drifts, or a dimension's placeholder share grows (**[docs/DATA_QUALITY_ALERTS.md](DATA_QUALITY_ALERTS.md)**). Conditions that are normal at any level — a positive `superseded` count, a steady share of placeholder rows — are deliberately not alerted on, because a permanently red channel is a muted one. What each alert means and what to do about it: **[docs/ALERT_RESPONSE.md](ALERT_RESPONSE.md)**.
- **Reconciliation, and where it stops** — `bronze == silver + quarantine + audit + superseded` proves every row is accounted for at Silver in every run mode. The fourth term is computed from Bronze, not stored, because Bronze already retains every arrival and materialising it would have duplicated 921,915 rows in dev alone; the three-term form holds only after a full re-derivation ([docs/RECONCILIATION_INVARIANT.md](RECONCILIATION_INVARIANT.md)). It proves rows are **accounted for**, which is not the same as proving they **should exist**: when a backfill duplicated 505 Bronze rows that had never arrived twice, the invariant balanced perfectly — the duplicates lost Silver's dedup and were correctly counted in the audit term. It does **not** by itself prove a row is usable downstream either — a dimension absent from Gold contributes nothing to the marts while every upstream check passes, which is how 49,539 fact rows went missing from product reporting. Severity tiers closed that path (a descriptive violation no longer removes the row) and the orphan counters in the marts now read 0 structurally, so **[docs/SEVERITY_TIERS.md](SEVERITY_TIERS.md)** replaced them with a placeholder-exposure metric — a monitor that goes blind when the failure it watches becomes impossible is worse than none. Mechanism, measurements and the rejected fixes: **[docs/REFERENTIAL_COMPLETENESS.md](REFERENTIAL_COMPLETENESS.md)**.

## Performance

Validated end-to-end at **3M source rows**. The per-entity metrics tables make the pipeline
self-profiling — they were used to find and fix a Silver bottleneck that **halved total runtime**:

| Source rows | Before | After |
|---|---|---|
| 1,000,000 | 12 min 00 s | 8 min 32 s |
| 3,000,000 | 23 min 53 s | **11 min 38 s** |

The cause was a 7-format `coalesce(try_to_date(...))` where the actual source format sat second,
so every row paid for a failed parse first — 803 s → 158 s for the affected entity after
reordering. Full write-up, including the measurement method and a deliberately deferred
optimization: **[docs/PERFORMANCE_INVESTIGATION.md](PERFORMANCE_INVESTIGATION.md)**.

A follow-up pass addressed the reason that fix over-delivered: because Databricks Serverless
forbids `cache()`/`persist()`, every Spark action replayed the whole Silver lineage, and the
layer was triggering eleven of them per entity just to collect metrics. Collapsing those into
two fused aggregations removed eight full-lineage scans per entity with no metric lost —
verified numerically equivalent, runtime impact not yet measured:
**[docs/SILVER_ACTION_COLLAPSE.md](SILVER_ACTION_COLLAPSE.md)**.

Runtime is now ~60% serverless task startup and ~40% data processing at 3M rows, scaling
linearly — so task consolidation, not Spark tuning, is the next meaningful lever.

## Repository structure

```
databricks.yml                     # bundle: targets (dev/qa/prod), variables
resources/
  superstore_lakehouse_job.job.yml # 18-task pipeline DAG + parameters + notifications
  integration_test_job.job.yml     # reset → seed → pipeline → asserts
  superstore_freshness_alert.alert.yml      # SQL alert: pipeline silence
  superstore_data_quality_alerts.alert.yml  # SQL alerts: reconciliation, orphans, drift, placeholders
configs/                           # YAML: column contracts, DQ rules, env paths
src/
  superstore_bronze/               # ingestion + entity split modules
  superstore_silver/               # DQ/dedup module + pure transformation functions
  superstore_gold/core/            # SCD2 dimension + facts frameworks
  superstore_shared_utilities/     # logger, platform config, backfill utils, run-id init
  dashboards/                      # executive, customer, product dashboards
  features/ | marts/ | metrics/    # serving-layer notebooks
superstore_orchestrator/
  layer_orchestrator/              # per-layer orchestration notebooks
governance/                        # who can access what — separate lifecycle from the pipeline
  terraform/                       # Unity Catalog permission model (intended; unapplied on Free Edition)
  manual_grants/                   # grants actually applied for the prod and qa service principals
docs/                              # one write-up per design decision or defect, with evidence
tests/
  unit/                            # pytest suite (local Spark), by layer
  integration_databricks/          # reset / seed / per-layer assert notebooks
.github/workflows/                 # CI/CD
```

## About the pipeline screenshot

*Screenshot captured 2026-08-02, and worth being precise about what it shows: the DAG
shape, not a productive run. Prod had no source file until 2026-08-16, so every green run
before that date — including this one — ran 18 tasks over an empty landing volume and
produced nothing. That is the defect described in
[Defects found by measurement](DEFECTS_FOUND_BY_MEASUREMENT.md), and it is
left visible here rather than swapped for a flattering screenshot. The first prod run to
carry real data completed **2026-08-16 in 5.6 min**: 18/18 tasks, 92 source rows, four
Silver entities at `run_status = 'SUCCESS'`, reconciliation balanced on all four, zero
orphaned facts.*
