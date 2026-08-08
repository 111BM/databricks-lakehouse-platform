# Referential Completeness — where the reconciliation guarantee stops

## Summary

The pipeline's reconciliation invariant is a **Silver-layer** guarantee. It proves every
bronze row is *accounted for*:

```
bronze_entity == silver + quarantine + audit
```

It does **not** prove those rows are *usable downstream*. A fact whose dimension row is
absent from Gold contributes nothing to the marts, and until now nothing reported it — the
row was not quarantined, not audited, and not counted anywhere. It was simply missing from
the output while every upstream check passed green.

This document records how that was found, the mechanism, and what was decided.

---

## How it was found

Tracing an unrelated question about late-arriving data raised a simpler one: what happens to
a fact whose dimension does not exist? An anti-join answered it:

```sql
SELECT COUNT(*) AS orphaned_orders
FROM superstore_catalog.dev_gold.facts_orders o
LEFT ANTI JOIN superstore_catalog.dev_gold.dim_customers c
  ON o.customer_id = c.customer_id AND c.is_current = true;
```

`dev` returned **5**.

Following those five customers back through the layers:

| Customer | in bronze | in silver | in quarantine | rejected on |
|---|---|---|---|---|
| NC-18535 | yes | no | yes | `customer_name` |
| SU-20665 | yes | no | yes | `segment` |
| PN-18775 | yes | no | yes | `country` |
| CG-38485 | yes | no | yes | `state` |
| CG-74136 | yes | no | yes | `postal_code` |

Five customers, five different columns, one violation each.

---

## The mechanism

Nothing here is broken in the sense of a crash or a bad value. Every component did exactly
what it was designed to do:

1. The customer row failed a data-quality rule on **one attribute**.
2. Silver quarantined the whole row and recorded the violated column in `error_columns` —
   correct behaviour, and the reconciliation invariant still balances.
3. The customer therefore never reached `dim_customers`.
4. Their orders passed Silver cleanly — `customer_id` is not validated, and nothing checks
   referential integrity — and landed correctly in `facts_orders`.
5. The marts join facts to dimensions on the current dimension version. With no dimension
   row, those orders contribute nothing.

The result is that the orders exist in Gold, are counted in every metrics table, and appear
in no mart and no KPI view. **The revenue is silently absent from reporting.**

Two mart shapes lose the rows by different routes, with the same outcome:

| Mart | Shape | How the fact is lost |
|---|---|---|
| `mart_sales_daily` | fact-driven, `JOIN dim_customers` / `JOIN dim_products` | inner join drops it |
| `mart_customer_360` | dimension-driven, `FROM dim_customers LEFT JOIN facts_orders` | no dimension row to hang the fact off |
| `mart_product_performance` | dimension-driven, `FROM dim_products LEFT JOIN facts_sales` | same |

There is a second path through the same predicate: the joins require `is_current = true`, so
a customer that exists but has **no current version** — soft-deleted, for example — loses
their facts the same way.

---

## What changed

Each mart now counts what it is about to lose and logs it before writing:

```python
_orphans_customers = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {catalog}.{gold_schema}.facts_orders f
    LEFT ANTI JOIN {catalog}.{gold_schema}.dim_customers d
      ON f.customer_id = d.customer_id AND d.is_current = true
""").first()["n"]
```

Logged at `WARN` when non-zero, `INFO` when zero, with `orphaned_facts`, `fact_table` and
`dimension_table` as structured fields.

This is deliberately **instrumentation, not a fix**. No row is recovered and no reported
number changes. It converts a silent loss into a monitored one — the same reason quarantine
records which rule a row violated rather than just dropping it.

### Monitoring

```sql
-- current orphan count, per dimension
SELECT COUNT(*) FROM superstore_catalog.<env>_gold.facts_orders o
LEFT ANTI JOIN superstore_catalog.<env>_gold.dim_customers c
  ON o.customer_id = c.customer_id AND c.is_current = true;

SELECT COUNT(*) FROM superstore_catalog.<env>_gold.facts_sales s
LEFT ANTI JOIN superstore_catalog.<env>_gold.dim_products p
  ON s.product_id = p.product_id AND p.is_current = true;
```

The number to watch is not the absolute count but whether it **grows per run**. Each
execution that quarantines another dimension row adds permanently to the orphan set.

---

## What was NOT changed, and why

### Inferred members — rejected

The standard fix for an orphaned fact is a placeholder dimension row (natural key,
attributes `Unknown`, `is_current = true`) so the join succeeds, superseded later by the real
row through the existing SCD2 merge.

It is the wrong tool **for this cause**. Inferred members exist for dimensions that have not
arrived yet. These dimensions *have* arrived — they are sitting in quarantine because of a
cosmetic attribute. A placeholder would leave the same customer represented twice, wrongly:
`Unknown` in the dimension and the real dirty values in quarantine. It treats the symptom and
leaves the policy unexamined.

Inferred members remain the correct answer if a genuine late-arriving dimension case appears
— a second source with its own schedule, for example.

### Data-quality severity tiers — the real fix, deferred

The root cause is that **every column is treated as equally fatal**. A malformed
`postal_code` currently removes a customer, and all of their revenue, from every report. That
is a severe consequence for a field nothing aggregates on.

The correct design classifies columns:

| Tier | Columns | On violation |
|---|---|---|
| Fatal | business keys (`customer_id`), type-critical fields | quarantine the row |
| Repairable | `customer_name`, `segment`, `country`, `state`, `postal_code` | null the value, keep the row, record the violation |

Quarantine would then mean *this row cannot be trusted at all*, rather than *this row is
imperfect* — which is what quarantine should mean.

**Why it was deferred:** it restructures the reconciliation invariant. Today every row is in
exactly one bucket. A repaired row would be in Silver *and* carry a recorded violation, so
repairs become an attribute of a Silver row rather than a fourth bucket. That is a real
change to the strongest guarantee in the platform, made to recover five rows in a dataset of
this size. The trade is not worth it here; it would be worth it at volume, and it is the
change to make first if this pipeline were ever run for real.

---

## Related limits worth knowing

- **`customer_id` is never validated in Silver.** Only `order_date` and `ship_date` carry
  regex rules on the orders entity. A fact can reference any customer id at all.
- **Facts store natural keys, not surrogate keys.** There is no dimension lookup at fact-load
  time, so nothing can fail there — the mismatch only surfaces at the mart.
- **The marts have no metrics table.** Silver and Gold both write per-entity metrics; the
  serving layer writes none, which is why this loss had no instrumentation to appear in.

---

## The general lesson

A reconciliation invariant is scoped to the layer that defines it. This one proves rows are
accounted for at Silver; it says nothing about whether a row that survived Silver can still
be used at Gold or beyond. Extending it end to end — treating facts that cannot resolve their
dimension as a reconciliation failure rather than a silent zero — is the direction this would
go if the platform grew.
