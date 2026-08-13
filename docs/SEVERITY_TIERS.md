# Severity Tiers — quarantine should mean untrustworthy, not imperfect

## What

Every data-quality violation was fatal. A customer missing only a `postal_code` was
quarantined whole, never reached `dim_customers`, and its orders then contributed
nothing to any mart — while every upstream check passed green
(**[REFERENTIAL_COMPLETENESS.md](REFERENTIAL_COMPLETENESS.md)**).

Violations are now tiered:

| Tier | Columns | On violation |
|---|---|---|
| Fatal | `customer_id`, `product_id` | quarantine the row |
| Repairable | the other 11 business columns | record it, keep the row, `'Unknown'` at Gold |

Only business keys are fatal, because only they make a row unusable. **Anything absent
from the severity map defaults to fatal** — an unclassified column keeps the stricter
behaviour rather than being silently downgraded, which is the failure direction this
whole backlog is about.

## Why the work splits across two layers

**Silver decides and records.** `add_error_columns` emits two arrays: `error_columns`
(fatal, still the sole quarantine signal) and `repaired_columns` (recorded, row
proceeds). It does **not** modify the value — Silver stays diffable against Bronze, and
no `_raw` preservation column is needed because nothing was overwritten.

**Gold substitutes.** `substitute_missing_attributes` fills nulls when the dimension is
built, so completeness is guaranteed *by construction*: a dimension row cannot be
written with a null attribute, because the step that writes it fills them. Substituting
in Silver instead would rely on the tier config staying in sync with the dimension
config, with nothing enforcing it. `'Unknown'` is also a presentation decision, and Gold
is the presentation layer.

Kimball's rule is the reason for a token rather than a null: a dimension attribute is
never NULL. Nulls behave badly in group-bys, joins and BI tools, and push a `COALESCE`
into every consumer.

Substitution runs **before** the row hash, so a repaired row and a later clean row hash
differently and SCD2 records the enrichment as a genuine change.

## The bug this created, and the fix

Tiers let an incomplete row reach Silver — where it then competed in deduplication with
complete observations of the same entity. Dedup was latest-arrival-wins, so a newer row
missing `region` beat an older complete one, and Gold substituted `'Unknown'` over a
value that was present in the same batch.

Measured in `dev`: **81 of 99** dimension rows showing `region = 'Unknown'` had a real
region in the audit table for that customer. Not a gap being labelled — data being
discarded and labelled as absent.

`classify_duplicates` now orders by:

```
1. fewest repaired attributes      <- new
2. most recent bronze_ingestion_ts
3. content hash of business columns
```

Completeness outranks recency, and that costs nothing: `bronze_ingestion_ts` carries no
business meaning here — the source has no change timestamp, which is why the hash
tiebreak exists at all. Recency was never evidence of truth, so there is nothing to
trade away. Entities without a severity map (orders, sales) have no `repaired_columns`
column and keep exactly the old ordering.

## Where

| Concern | Location |
|---|---|
| Tier classification | `superstore_silver_transformations.add_error_columns` |
| Dedup ordering | `superstore_silver_transformations.classify_duplicates` |
| Substitution | `superstore_gold_dimension_framework.substitute_missing_attributes` |
| Applied at | `prepare_scd2_columns`, before the hash |
| Config | `severity:` per entity in the silver config |
| Wiring | silver orchestrator (**two** places), gold dimensional orchestrator |
| Placeholder monitor | `superstore_placeholder_monitor`, called from both dimension-driven marts |
| Tests | `test_silver_dq.py`, `test_silver_dedup.py`, `test_dimension_placeholder.py`, `test_placeholder_monitor.py` |

## Verification

Replayed on `dev`, window 2026-08-08:

| metric | before | after |
|---|---|---|
| `quarantine_products` | 3,770 | 920 |
| `quarantine_customers` | 7,901 | 1,019 |
| `dim_products_current` | 51,511 | 51,653 |
| `dim_customers_current` | 87,445 | 87,522 |
| orphaned `facts_sales` rows | 142 | **0** |
| orphaned `facts_orders` rows | 62 | **0** |
| dimension rows with a null attribute | 0 | **0** |
| `'Unknown'` regions displacing a real value | 81 | **0** |

Quarantine now holds exactly the fatal-key counts measured beforehand (920 and 1,019).
Dimension growth of +142 and +77 matches the orphan counts exactly.

After the dedup fix, 77 dimension rows still carry an `'Unknown'` — precisely the
customers step 3 newly recovered, whose only rows had missing attributes. Every
surviving placeholder now belongs to an entity with no better value available anywhere.

## What this does not do

**It recovers nothing on real data.** The `dev` set holds 793 real customers and 1,862
real products, all with complete records; the nulls are injected and the affected rows
are generated. This is a fix to a *policy*, not a recovery — preparation for a source
that arrives incomplete, which is when it earns its place. Quoting the row counts as
business impact would repeat the mistake called out in
**[VALUE_STANDARDIZATION.md](VALUE_STANDARDIZATION.md)**.

**`'Unknown'` is monitored, and had to be.** Substitution converts revenue that was
*absent* from reports into revenue *attributed to a placeholder* — an improvement only
because it is visible, and visible only if something looks. Nothing did: the orphaned-fact
counters in the marts now read 0 *permanently*, because tiers made the condition they
detect impossible. They would keep reading 0 whether the next feed were pristine or badly
incomplete.

`superstore_placeholder_monitor` is their successor, logged from both dimension-driven
marts beside the counters it replaces. Current exposure in dev:

| dimension | placeholder rows | of | share |
|---|---|---|---|
| `dim_customers` | 77 | 87,522 | 0.088% |
| `dim_products` | 142 | 51,653 | 0.275% |

Those counts equal exactly the entities severity tiers recovered, so every placeholder is
a genuine gap rather than a displaced value. As with the orphan count, the number to watch
is not the absolute figure but whether it **grows per run** — that is what says a source
started arriving incomplete.

**`repaired_columns` may be more than this project needs.** Per-row DQ annotation is the
minority approach — DLT and dbt both record violations as aggregate metrics. The column
forced schema evolution on live tables, which failed on the first attempt (see below). A
repaired-row count in the metrics table would deliver most of the observability for less.
It is kept because it is consistent with the existing `error_columns` design.

**Serverless note.** Adding the column via `spark.databricks.delta.schema.autoMerge.enabled`
fails on Databricks Serverless with `CONFIG_NOT_AVAILABLE`. The supported route is
`.withSchemaEvolution()` on the merge builder, and `.option("mergeSchema", "true")` on a
write. The first attempt failed all four Silver entities and the job still reported
SUCCESS — because `bronze_to_silver_prod` caught every exception without re-raising. That
is why this work is verified against data counts rather than job status throughout. The
swallow itself is now fixed: the orchestrator collects every failed entity and raises
after the loop, verified by fault injection.
