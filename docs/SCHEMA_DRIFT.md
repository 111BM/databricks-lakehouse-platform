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

**Status: wired and recording.** The Bronze orchestrator calls it once per run
and writes to `{env}_metrics.schema_drift`. Not yet proven against real drift —
see [What is not built](#what-is-not-built).

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
| Column **added** | see below — **not yet verified** | probably both |
| Column **removed** | `select()` raises `AnalysisException` | **loud** |
| Column **retyped** | value rescued, typed column goes NULL | silent |

### The added case is not as simple as it was first written here

An earlier version of this document, and the commit that introduced the
detector, both said an added column is *silently* dropped. That is likely wrong
for the **first** encounter.

`cloudFiles.schemaEvolutionMode = addNewColumns` is documented to **fail the
stream** with `UnknownFieldException` when an unknown column appears, write the
updated schema to the schema location, and require a restart. Nothing in
`bronze_ingest_superstore_module_01` handles that — `grep` for
`UnknownField|retry|restart` returns nothing.

So the real sequence is probably:

1. run fails — loud, but with an error naming no business cause
2. someone reruns
3. the column lands in `superstore_raw`
4. the entity split drops it — silent from here on

That does not make the detector less useful; it arguably makes it more so.
Without it you get one baffling failure, a rerun that goes green, and no record
anywhere of what changed or that a new column now exists unused.

**This is stated as uncertain because it has never been observed on this
platform.** The integration suite now manufactures the event (see below), and
whatever it does is the answer. If the stream does fail, restart handling is a
real gap in backlog item 2 and a bigger one than detection.

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
| Call site | `superstore_bronze_layer_ETL_pipeline_orchestrator.ipynb`, step `1b` |
| Recorded to | `{env}_metrics.schema_drift` |
| Ignore decisions | `ignored_source_columns` in `configs/superstore_bronze_config/` |
| Column contracts | `bronze_entities.*.columns`, same file |
| The drop itself | `bronze_entity_superstore_module_02.py:523` |
| Rescued values | `col__rescued_data` on `{env}_bronze.superstore_raw` |

## The table

`{env}_metrics.schema_drift`, appended once per run:

| Column | Meaning |
|---|---|
| `master_run_id` | run that observed it |
| `env` | dev / qa / prod / integration_test |
| `column_name` | the drifted column, or the rescue column for `RESCUED` |
| `drift_status` | `NEW` / `MISSING` / `RESCUED` |
| `row_count` | non-null rescued rows; `NULL` for `NEW`/`MISSING` |
| `detected_at` | observation time |

`NEW` and `MISSING` rows appear only when they occur. **`RESCUED` is written
every run**, even at zero — it is a measurement rather than an event, and a row
per run doubles as proof the check executed. A run with no `schema_drift` row is
itself evidence the detector did not run, which is the failure a monitor is
least likely to notice about itself.

### Why the check does not fail the run

`record_schema_drift` is wrapped in a `try/except` that logs at `ERROR` and
continues. That cuts against this codebase's usual rule, so the reasoning is
recorded rather than assumed.

Every other swallowed exception fixed here was wrapping a **data** operation,
where a silent failure meant wrong or missing rows. This is an **instrument**.
If it breaks, the load is still correct — and failing the run would make adding
a monitor strictly riskier than having none, which is how teams stop adding them.

The trade is only acceptable because the failure is *detectable*: it logs at
ERROR, and the per-run `RESCUED` heartbeat means a broken monitor can be found by
querying the very table it failed to write.

## Proving it fires

The detector reports `SCHEMA_STABLE` in every environment, because the source is
a static CSV and no column has ever appeared. **A detector that has only ever
said "nothing to report" is indistinguishable from one that cannot report** —
and this platform has already shipped that exact thing twice: the freshness
alert evaluated daily while structurally unable to breach its own threshold, and
the orphaned-fact counters read 0 permanently once severity tiers made their
condition impossible.

So the suite manufactures the event. `02_seed_scd2_change` now sends a
`Discount Reason` column the first seed did not, appended last — an additive
change at the end is what a real source change looks like, and inserting it
mid-header would additionally test positional parsing and muddy what a red run
means. It sanitises to `discount_reason`, since unmapped columns fall through to
`sanitize_column()`.

[`assert_schema_drift`](../tests/integration_databricks/assert_schema_drift.py)
then requires four things:

1. **Heartbeat** — one `RESCUED` row per load. A run missing its row means the
   detector did not execute, which is the failure a monitor is least likely to
   notice about itself, and the reason `record_schema_drift` is allowed to
   swallow its own exceptions.
2. **It fires** — `discount_reason` recorded as `NEW`. Without this the whole
   feature is unfalsifiable.
3. **No false positives** — `row_id` must not be reported, and no `MISSING`.
4. **Behaviour unchanged** — `discount_reason` present in `superstore_raw` and
   absent from all four entity tables. A detector that quietly started widening
   the contract would be worse than none.

The task is wired as a leaf off `run_pipeline_incremental_load`, and `cleanup`
depends on it as well as on `assert_replay` — otherwise cleanup can start
dropping `integration_test_*` while the assertions are still reading them, which
is the same race `max_concurrent_runs: 1` closes between runs, reintroduced
inside a single run by adding a parallel branch.

## What is not built

**No alerting.** Deliberate, and covered under [When](#when).

**Restart handling for `UnknownFieldException`.** If the suite shows Auto Loader
fails the stream on an unknown column, that gap is real and unaddressed — and
probably the larger half of backlog item 2.

What **is** verified: the logic runs against the shipped config and the real
`prod_bronze.superstore_raw` schema, reports `SCHEMA_STABLE` with one knowingly
ignored column, and reports `discount_reason` when a new column is simulated. A
test runs that comparison against the real config file rather than a fixture, so
it fails if someone declares a column that no longer arrives. The wiring itself
has been validated only by `bundle validate` and the unit suite — no pipeline run
has yet executed it.
