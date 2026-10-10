# Dashboards — the consumption layer, deployed as code

## What

Three Databricks AI/BI dashboards, defined in the repo and deployed by the bundle
to every environment:

| Dashboard | Reads | Answers |
|---|---|---|
| **Executive overview** | `<env>_semantic_layer` | Revenue, profit and margin over the last 30 days vs the 30 before; weekly trends; category mix |
| **Customers** | `<env>_semantic_layer` | Customer value by region and segment; top 10 customers per region |
| **Pipeline flow** | `<env>_metrics` | How rows moved Bronze → Silver → Gold in the latest run, where every Bronze row ended up, timings, reliability and data quality |

Files:

- [`src/dashboards/*.lvdash.json`](../src/dashboards/) — SQL and layout, one file per dashboard
- [`resources/superstore_dashboards.dashboard.yml`](../resources/superstore_dashboards.dashboard.yml) — the bundle resources

## Why

Before this change the dashboard SQL lived in three places, and none of them was
the real source of truth:

| Where | Problem |
|---|---|
| `src/dashboards/0{1,2,3}_*.ipynb` | No job ran them. They had already drifted from the live dashboards, and 03 was empty |
| The live dashboards, in the operator's home folder | The SQL that actually ran was not in git, so nobody could review it or see its history |
| Every query | Hardcoded to `superstore_catalog.dev_semantic_layer`, so the same dashboard could not show prod |

The `.lvdash.json` files now hold the only copy. A dashboard change is a
reviewed commit and goes through the same CI as the pipeline.

## How environments work

The queries name tables **without** a catalog or schema:

```sql
SELECT ... FROM metrics_daily_kpi
```

The bundle supplies them per target:

```yaml
dataset_catalog: ${var.catalog}                  # superstore_catalog
dataset_schema:  ${var.schema}_semantic_layer    # dev_ / qa_ / prod_semantic_layer
```

So one file renders dev, qa or prod data. The notebooks resolve their
environment the same way, through `superstore_platform_config`.

## Changing a dashboard

Edit the `.lvdash.json` and deploy, or edit in the workspace UI and pull the
change back:

```bash
databricks bundle generate dashboard --resource superstore_pipeline_flow --force
```

Pulling back matters: the next `bundle deploy` overwrites any edit that exists
only in the UI.

## Defects fixed on the way

The original dashboards were reviewed against their data before being moved.
Each problem had a specific cause:

| Symptom | Cause | Fix |
|---|---|---|
| Revenue tile showed **$0.00M ▲25%** | The KPI read the single latest `order_date`, a partial day holding only a few test rows | Trailing 30 days vs the previous 30 |
| Profit arrow pointed the wrong way | `(cur - prev) / prev` flips sign when `prev` is negative | Divide by `ABS(prev)` |
| Margin change shown as "▼ 57%" | A percentage change of a margin is unreadable | Show percentage points (`pp`) |
| **100% of customers "Churn Risk"** | Recency was measured from `CURRENT_DATE()`, but the data ended months earlier | Removed; see below |
| "Unknown" region and segment in charts | Placeholder dimension rows ([Referential completeness](REFERENTIAL_COMPLETENESS.md)) are not business categories | Filtered out of business views |
| Region filter changed only two charts | The filter was bound to one dataset | Bound to every dataset |
| Trends plunged at the right edge | A partly loaded final week | Weekly charts use complete weeks only |

## The data has no business patterns

The executive and customer dashboards look flat on purpose. They are not
hiding anything. On the 100M-row prod load:

| Question | Answer |
|---|---|
| Profit margin | −10.0% in every region, segment, category, discount level and quantity |
| Does a bigger discount cut margin? | No: −10.0% at every level from 0% to 20% |
| Seasonality | None: monthly revenue is flat |
| Ship mode vs delivery time | Same Day takes 4.97 days on average, exactly like Standard |

The data generator draws every field independently at random, so no
relationship exists for a chart to show. The −10% margin is in the raw CSV
(about 62% of source rows have negative profit), not introduced by the pipeline.

That is why the customer-recency pie was removed rather than fixed: measured
correctly, every customer is "Active", which says nothing. Each dashboard carries
a note about the synthetic margin, so a reader does not mistake it for a
pipeline defect.

The pipeline flow dashboard does not depend on business patterns. Its numbers
come from what the pipeline did, so it is the most informative of the three.

## Pipeline flow, panel by panel

| Panel | Source | Notes |
|---|---|---|
| Since last successful run | `silver_layer_metrics` | Same measure as the [freshness alert](FRESHNESS_ALERT.md): `SUCCESS` only, alert threshold 216 h |
| Latest-run tiles | `bronze_` / `silver_` / `gold_layer_metrics` | "Latest" means the latest run that **ingested data**. An incremental run with no new files is a correct no-op, but a dashboard of zeros explains nothing |
| Where every Bronze row ended up (Sankey) | `data_quality_checks`, `check_name = 'reconciliation'` | The [reconciliation invariant](RECONCILIATION_INVARIANT.md) drawn as a picture: `bronze = silver current + superseded + audit + quarantine` |
| Rows and timing per entity | all three layer metrics tables | Silver is the slowest layer, as measured in [Performance investigation](PERFORMANCE_INVESTIGATION.md) |
| Run history | all three layer metrics tables | Wall clock per layer, not summed per entity: Gold builds dimensions and facts in parallel |
| Reliability | layer metrics + `data_quality_checks` | Every run, including no-op runs |
| Data quality, schema drift | `data_quality_checks`, `schema_drift` | Latest run's checks; the drift log |

### What it shows on prod

From the 100M-row load on 2026-10-09:

- **100.2M raw rows** in, Bronze → Gold in **30.1 minutes** wall clock, **8 / 8** checks passed.
- **15.5M orders quarantined (15%)**, almost all for
  `ship_date_before_order_date`. The raw rows do have a ship date before the
  order date (for example ordered 18-05-2026, shipped 16-05-2026), so the
  dates are parsed correctly and the rule is doing its job.

## Freshness on the business dashboards

The executive and customer dashboards read `<env>_semantic_layer`, not
`<env>_metrics`, so they cannot reuse the pipeline tile. They show **"Last
refreshed"** instead: when the tables behind the dashboard were last written,
from Unity Catalog's `information_schema.tables.last_altered`. That is the
question a business reader actually has: are these numbers current?

The query filters on `table_schema = current_schema()`, and the current schema
is the `dataset_schema` the bundle sets. Each environment therefore reports its
own tables, with no environment name in the SQL.

## Permissions

The qa and prod dashboards are deployed by their CI service principals, like
everything else in those targets. They need nothing new:

- **Warehouse:** the service principals already use `Serverless Starter Warehouse` for the SQL alerts.
- **Tables:** they already hold `SELECT` on `<env>_semantic_layer` and `<env>_metrics` ([grants](../governance/manual_grants/)).
- **Viewing:** the operator has `CAN_MANAGE` on qa and prod resources through the target permissions.
