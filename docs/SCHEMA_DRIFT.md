# Schema Drift — noticing when the source changes shape

## What

A pure detector that compares the columns actually present in `superstore_raw`
against the columns the entity configs declare, and classifies every source
column into one of three states:

| State | Meaning | Where it is recorded |
|---|---|---|
| **declared** | carried into an entity table | `bronze_entities.*.columns` |
| **ignored** | knowingly dropped, decision recorded | `ignored_source_columns` |
| **drift** | nobody has decided | reported |

Implemented in
[`src/superstore_shared_utilities/superstore_schema_drift.py`](../src/superstore_shared_utilities/superstore_schema_drift.py),
25 unit tests in
[`tests/unit/shared/test_schema_drift.py`](../tests/unit/shared/test_schema_drift.py).

**Status: detection logic only.** The function exists and is tested; it is not
yet called by the pipeline and no `schema_drift` metrics table is written. See
[What is not built](#what-is-not-built).

## Why

Auto Loader runs with `cloudFiles.schemaEvolutionMode = addNewColumns`, so a new
source column reaches `superstore_raw` intact — verified, the table carries 28
columns including Auto Loader's `col__rescued_data`. **Nothing is lost at
Bronze.**

The entity split then does this:

```python
all_columns = entity_cfg["columns"] + entity_cfg["metadata_columns"]
entity_df = df_entity.select(*all_columns)
```

`columns` is a hardcoded YAML list, so the contract is an explicit allowlist and
a column not on it never leaves Bronze. **That is correct.** A human deciding
what enters the model is the point, and this work deliberately does not change
it.

What is wrong is that the decision happens *by silence*.

### The three drift types behave completely differently

| Drift | What happens today | Loud? |
|---|---|---|
| Column **added** | lands in `superstore_raw`, dropped at the split | silent |
| Column **removed** | `select()` raises `AnalysisException` | **loud** |
| Column **retyped** | value rescued, typed column goes NULL | silent |

The loud one is the safe one — it fails the same day and someone fixes it. The
quiet ones are the problem, and they are quiet in different ways:

**Added** loses no data. It costs *knowledge*. A source starts emitting
`discount_reason` in March and nobody finds out until someone hand-diffs the raw
schema against the config. You cannot act on data you do not know you have.

**Retyped** is worse: the entity column silently becomes NULL, Silver's DQ sees
nulls and does something reasonable — repairs or quarantines — so the *symptom*
surfaces while the *cause* stays invisible. That gets debugged as a data-quality
problem for a while before anyone reaches the rescue column.

### The same shape as every other defect here

`col__rescued_data` already exists, is already populated by Auto Loader, and
`grep -rn "rescued"` across every `.py`/`.yaml`/`.yml` in this repo returns
**nothing**. A safety net that works and that nobody checks.

That is the pattern this codebase keeps rediscovering — the pipeline already
knows, it just never writes it down. Same as the swallowed Silver exceptions,
the orphan counters that went blind, and the auth-mode `print()` the Jobs API
cannot read.

## How

```python
detect_drift(observed_columns, bronze_entities, ignored_source_columns)
# -> {"new": [...], "missing": [...], "ignored": [...]}
```

Three decisions in that signature, each of which changes what the detector can
report:

**Pipeline-added columns are excluded.** `superstore_raw` carries six metadata
columns the pipeline adds after ingestion (`bronze_ingestion_ts`,
`ingestion_date`, `source_file_*`) plus the rescue column. Counting any of them
as source data would report permanent drift on every run, and the usual response
to a permanently red monitor is to stop reading it. The rescue column is matched
on the `_rescued_data` **suffix** rather than an exact name, because Auto Loader
names it — it appears as `col__rescued_data` here.

**Declared columns are a union across entities, not an intersection.** A source
column is known to the model if *any* entity claims it. `sales` is declared only
by the sales entity; reporting it as drift for the other three would bury the
signal.

**`ignored_source_columns` exists because of `row_id`.** The first version had
only two states, and running it against the real config immediately reported
`row_id` as drift — present in every source file, declared by no entity, dropped
on every run since the platform was built. Almost certainly deliberate, but with
no third state it would be reported forever. The fix was not to widen the filter
but to make the existing decision explicit and write down *why*:

```yaml
ignored_source_columns:
  # Row number of the CSV, assigned by whoever exported the file. It identifies
  # a position in a delivery, not a business entity, and it is not stable across
  # re-exports.
  - row_id
```

An ignore entry is a decision, not a silencing. A test asserts the list cannot
mask a `MISSING` column, so it can never be used to switch off the loud, safe
failure mode.

Output is sorted, because an unsorted set would order rows differently each run
and make the metrics table's history unreadable. `drift_rows()` emits **one row
per column, not per run**, so the table can answer *"when did this column first
appear"* — the question actually asked months later. No drift emits **no rows**;
a synthetic "all clear" row per run would bloat the table with nothing.

### The evolution rule

Written down, because this paragraph does more work than any tool:

- **Adding** a column is free — no coordination, no version bump. It appears as
  drift, someone decides to declare or ignore it.
- **Removing** one: stop writing it → keep it nullable for a release → then drop.
- **Retyping**: add a *new* column with the new type, migrate readers, retire
  the old one. Never change a type in place.

This is expand-and-contract, and it is what almost every mature team converges
on regardless of tooling.

## When

**Detection runs per pipeline run**, once, comparing the raw schema against the
config — not per entity, since the union makes it a single comparison.

**Alerting deliberately does not run yet.** A new column breaks nothing, and
paging on it trains the reader to mute the channel — the same discipline that
keeps `superseded > 0` and placeholder rows off the alert path
([ALERT_RESPONSE.md](ALERT_RESPONSE.md)). The intended sequence is: record for
several runs, look at the table, and only wire an alert if the data shows drift
actually happens. `MISSING` and retyped columns are genuinely wrong and can join
the existing path when the table exists.

**Frequency is unknown and that is the point.** Nobody should write a drift
policy against imagined frequency. The source here is a static CSV, so the honest
expectation is that this reports `SCHEMA_STABLE` indefinitely.

## Where

| Concern | Location |
|---|---|
| Detection logic | `src/superstore_shared_utilities/superstore_schema_drift.py` |
| Unit tests | `tests/unit/shared/test_schema_drift.py` |
| Ignore decisions | `ignored_source_columns` in `configs/superstore_bronze_config/` |
| Column contracts | `bronze_entities.*.columns`, same file |
| The drop itself | `bronze_entity_superstore_module_02.py:523` |
| Rescued values | `col__rescued_data` on `{env}_bronze.superstore_raw` |

## What is not built

Stated plainly rather than implied, because a detector that is written but not
called reports nothing just as reliably as one that is broken:

1. **The pipeline does not call it.** No wiring into
   `bronze_entity_superstore_module_02` yet.
2. **No `schema_drift` metrics table.** `drift_rows()` produces the rows; nothing
   writes them.
3. **`col__rescued_data` is still unread.** Counting non-null values per run is
   the type-drift signal and is not yet collected.
4. **Not proven against real drift.** The integration test seeds its own data, so
   adding a column to the seed and asserting the drift table records it *is*
   possible here — and that step is what separates a detector from a detector you
   know works. Not done.

What **is** verified: the logic runs against the shipped config and the real
`prod_bronze.superstore_raw` schema, reports `SCHEMA_STABLE` with one knowingly
ignored column, and reports `discount_reason` when a new column is simulated. A
test runs that comparison against the real config file rather than a fixture, so
it fails if someone declares a column that no longer arrives.
