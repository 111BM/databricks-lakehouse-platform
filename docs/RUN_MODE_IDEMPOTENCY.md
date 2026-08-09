# Run-Mode Idempotency — which modes are safe to repeat, and which tables were not

## Summary

Silver merges into its target table, so re-deriving the same Bronze rows leaves it
unchanged. Quarantine and audit did not: both were plain appends. That is correct for
`incremental`, which never re-reads a Bronze row, and wrong for every mode that does.

A `replay`, `backfill` or `full_refresh` re-read rows it had already processed and
appended a **second copy** of each dirty and duplicate row. The tables grew by a full
copy per run, and the platform's reconciliation invariant

```
bronze == silver + quarantine + audit
```

over-counted by the size of the replayed window.

---

## How it was found

Not from a failure. The recovery plan for the orphaned-facts problem
(**[REFERENTIAL_COMPLETENESS.md](REFERENTIAL_COMPLETENESS.md)**) is: change the DQ
rules, then `replay` so previously quarantined rows re-derive into Silver. Checking
what a replay would actually do to the quarantine table surfaced the append.

The runbook would have caught it too. **[BACKFILL_QUICK_REFERENCE.md](BACKFILL_QUICK_REFERENCE.md)**
already tells operators to baseline `bronze = silver + quarantine + audit` before a
replay and reconcile afterwards — a check the replay itself would have broken.

---

## The mechanism

`get_incremental_with_backfill` scopes the Bronze read per mode. The write did not
know about that scoping, so the two were free to disagree:

| Mode | Rows re-read | Rows appended | Result |
|---|---|---|---|
| `incremental` | none (watermarked) | new only | correct |
| `backfill` / `replay` | the window | the window, again | duplicated |
| `full_refresh` | everything | everything, again | duplicated |

Silver escaped because its MERGE key made the second write a no-op. The two
append-only tables had no such key.

---

## What changed

**`reprocessed_scope_predicate(backfill_config, ingestion_col)`** —
[`superstore_backfill_utils.py`](../src/superstore_shared_utilities/superstore_backfill_utils.py) —
names the rows a run is about to re-derive, mirroring exactly what the read re-reads:
`None` for incremental, the date window for backfill/replay, everything for
full_refresh.

**`write_derived_table(...)`** —
[`superstore_silver_module.py`](../src/superstore_silver/superstore_silver_module.py) —
creates the table if absent, deletes that scope, then appends. Both the quarantine and
audit call sites use it, replacing two near-identical blocks.

The window predicate filters `to_date(bronze_ingestion_ts)` rather than the
`ingestion_date` column the read uses, because the derived tables do not carry
`ingestion_date`. The two are equal by construction — Bronze defines
`ingestion_date = to_date(bronze_ingestion_ts)`.

---

## What was NOT changed, and why

### A MERGE on the row hash — rejected

The obvious fix, and wrong here. Audit rows are deduplication *losers*: two identical
losers of the same business key produce the same `row_hash(business_columns)`. A MERGE
would collapse them into one row and break the reconciliation count in the opposite
direction. A scoped delete needs no key at all.

### The metrics tables — deliberately still append-only

`bronze_layer_entity_metrics`, `silver_layer_metrics` and `gold_layer_metrics` append
one row per run and should keep doing so. They are a run log, not derived data; a
replay is a real event that deserves its own row, which is what makes the `load_type`
stamp from `run_mode_load_type` worth having. Making them idempotent would delete the
history they exist to record.

### Bronze under `backfill` — untested

`replay` and `full_refresh` skip Bronze entirely (`reads_from_source`), so this fix
covers them completely. `backfill` does run Bronze, using a separate per-window Auto
Loader checkpoint. Repeating an identical backfill should therefore be safe, but a
backfill window overlapping files already ingested incrementally has not been tested
and Bronze has no dedup. Same shape of bug, different layer.

---

## Verification

Two identical replays of the same window on `dev`, ~1M rows per entity:

| entity | quarantine (run 1 → run 2) | audit (run 1 → run 2) | reconciles |
|---|---|---|---|
| customers | 7,901 → 7,901 | 915,110 → 915,110 | yes |
| orders | 159,686 → 159,686 | 8,490 → 8,490 | yes |
| products | 53,882 → 53,882 | 954,712 → 954,712 | yes |
| sales | 5,758 → 5,758 | 9,905 → 9,905 | yes |

Identical across both runs. Under the old code the second run would have roughly
doubled every one of these tables.

Unit coverage pins the mode-to-scope correspondence in
`tests/unit/shared/test_backfill_config.py`, including that `incremental` clears
nothing and that window bounds stay inclusive at both ends — an exclusive bound would
strand a stale copy of the boundary day.

---

## An unrelated finding from the same run

Before the replay, `customers` and `products` were short of reconciling by 416 and 413
rows; `orders` and `sales` balanced exactly. The replay **repaired** both — the missing
rows appeared in the audit tables and all four entities then balanced.

The likely cause is that Silver's MERGE updates an existing row in place when an entity
re-appears in a later run with changed attributes, leaving the superseded version in no
bucket: Bronze counted it, Silver keeps only the winner, and the audit table receives
only intra-run deduplication losers. Replaying a whole window puts both versions in one
batch, where the older one is correctly classified as a loser.

That is a hypothesis consistent with the evidence, not a confirmed cause, and it is a
separate defect from the one this document describes.
