# Value Standardization — conforming source dialects before validating them

## Summary

The products feed emits three-letter category codes (`OFF`, `TEC`, `FUR`) alongside the
full labels the contract expects (`Office Supplies`, `Technology`, `Furniture`). The
Silver layer validated first and conformed never, so every row using a code failed the
categorical rule and was quarantined.

Quarantined dimension rows never reach Gold, and facts referencing them then contribute
nothing to any mart — the mechanism documented in
**[REFERENTIAL_COMPLETENESS.md](REFERENTIAL_COMPLETENESS.md)**. In `dev` that removed
**49,539 fact rows** from every product report while every upstream check passed green.

The fix is one step, placed before the rules rather than after them: translate known
source dialects into the canonical vocabulary, then validate.

---

## Why translation, not repair

A row saying `OFF` is not bad data. It is the same fact written in another dialect, and
the correct value is fully recoverable. Two other options were available and both are
worse:

| Option | Result |
|---|---|
| Quarantine (what it did) | row lost, facts orphaned, revenue absent from reporting |
| Severity tiers → `'Unknown'` | row recovered, **category destroyed**, product reports still wrong |
| **Conform the value** | row recovered with its real category |

The middle option matters because it looks like a fix. Substituting `'Unknown'` returns
the revenue to the totals while making every category breakdown wrong — a silent error
traded for a quieter one. Severity tiers are the right tool for a value that is genuinely
missing, not for one that is merely spelled differently.

This is the same reasoning `REFERENTIAL_COMPLETENESS.md` used to reject inferred members:
right pattern, wrong cause.

---

## How it works

`standardize_values(df, value_standardization)` — a pure function in
[`superstore_silver_transformations.py`](../src/superstore_silver/superstore_silver_transformations.py)
— runs as **step 2b** of `bronze_to_silver_prod`, between column cleaning and
`add_error_columns`.

```yaml
# configs/superstore_silver_config/superstore_silver_config.yaml  (products)
value_standardization:
  category:
    "OFF": Office Supplies
    "TEC": Technology
    "FUR": Furniture
```

**Exact match only.** `off`, `OFFICE` and anything else unlisted pass through untouched
and still face the categorical rule. Standardization must not become a catch-all that
coerces unrecognised values into whichever canonical value looks closest — an unknown
value should still be rejected loudly. Adding a dialect is a config change, the same
contract every other DQ rule follows.

**No flag column is written.** Unlike a repair, the mapping is deterministic, lossless
and declared in config, and Bronze retains the value as received. Nothing is lost that a
reader would need warning about.

---

## Where

| Concern | Location |
|---|---|
| Transformation | `superstore_silver_transformations.standardize_values` |
| Pipeline step | `superstore_silver_module.bronze_to_silver_prod`, step 2b |
| Config | `configs/superstore_silver_config/superstore_silver_config.yaml` |
| Wiring | `superstore_silver_layer_ETL_pipeline_orchestrator` — **two** places |
| Tests | `tests/unit/silver/test_silver_dq.py` |

---

## Two bugs that produced a successful no-op

Both were caught only by measuring. Neither raised, logged, or failed a test.

### 1. YAML 1.1 reads a bare `OFF` as boolean `false`

```python
{False: 'Office Supplies', 'TEC': 'Technology', 'FUR': 'Furniture'}
```

The config loaded, deployed and ran without complaint while never matching one of the
16,837 `OFF` rows. Two thirds of the fix would have worked, which is harder to notice
than none of it working. Keys are now quoted, and
`test_products_category_mapping_keys_are_strings_not_booleans` pins the parsed type.

### 2. The orchestrator rebuilds the config from an allowlist

`silver_table_configs` is not the YAML — the orchestrator constructs a new dict naming
each key it intends to pass. `value_standardization` was absent from that list, so a
correct config reached a correct function as `{}`. The first verification replay changed
**nothing**, which is how it was found.

Any future config key needs adding in *both* the reconstruction and the call. There is
now a comment at the reconstruction saying so.

Both failures share the shape the README backlog already names: **input accepted and
quietly reinterpreted rather than rejected.** Here it was configuration rather than data.

---

## Verification

Replayed on `dev`, ~1M source rows:

| metric | before | after |
|---|---|---|
| `silver_products` | 1,862 | 51,511 |
| `quarantine_products` | 53,882 | 3,770 |
| `dim_products_current` | 1,862 | 51,511 |
| orphaned `facts_sales` rows | 49,539 | **142** |
| orphaned revenue | $138,005,153 | $416,844 |
| orphaned `facts_orders` rows (control) | 62 | **62** |

The control held: customers were untouched, as intended — they have no dialect to
conform (every invalid customer value is a plain null).

All 142 remaining orphans have valid business keys and are recoverable by severity tiers.

---

## The caveat that matters more than the numbers

**The dollar figure is an artifact of generated data, and should not be quoted as
business impact.**

The `dev` dataset contains two populations:

| | products | sales rows | sales per product |
|---|---|---|---|
| real Superstore catalog | 1,862 | 945,254 | 507.7 |
| generated | 49,649 | 49,397 | 0.995 |

The generator invented 49,649 products — each bought exactly once, spread in perfect
thirds across the three categories — and gave *those* rows category codes. Every genuine
product row already carried a canonical label.

So the $137.6M recovered is precisely the generated population's revenue, and **applied
to the real 1,862 products this change would have recovered nothing.** There was nothing
wrong with the real data.

That does not make the fix unnecessary. A vendor feed emitting `OFF` is an ordinary
thing, and the pipeline should conform it rather than silently drop every affected row
from reporting. But the honest claim is about the failure mode, not the money:

> A source-dialect mismatch quarantined every affected product and orphaned its facts
> from all reporting, with no error raised anywhere. Reproduced at 1M rows, measured,
> and fixed by conforming values before validation.

The same caution applies to the customer dimension: 793 real customers, 86,652 generated.
`dev` proves mechanisms; it does not measure impact.

---

## What this does not fix

**Gold does not receive the recovered rows on a normal replay.** The windowed Gold read
derives its date from `silver_ingestion_ts` — when a row was written to Silver — while
the same `start_date`/`end_date` mean *Bronze ingestion date* at Silver. Replaying the
window the data belongs to therefore updates Silver and leaves Gold untouched, reporting
success throughout.

The verification above only reached Gold by replaying with the *current* date, which
exploits that mismatch rather than fixing it. Until the windowing is reconciled, the
recovery procedure in **[BACKFILL_QUICK_REFERENCE.md](BACKFILL_QUICK_REFERENCE.md)**
cannot propagate a rule change to a dimension. That is a separate defect.

**The remaining 3,770 quarantined products and 142 orphans** are null-valued, not
mis-dialected. They need severity tiers, not conformance.
