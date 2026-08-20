# Integration Test Granularity — paying for orchestration, not computation

## What

The integration suite's two **replay** legs no longer invoke the full 18-task
pipeline. They call the three layer orchestrators a replay actually touches —
Silver, Gold dimensions, Gold facts — using the same production notebooks, just
fewer of them.

| | Before | After |
|---|---|---|
| `run_job_task` invocations | 4 | **2** |
| Task startups per suite | 88 | **57** |
| Suite tasks | 16 | 21 |

The initial and incremental loads are **deliberately unchanged** — still full
`run_job_task` invocations of the real pipeline.

## Why

Measured on a green suite (`1003634973079938`), not estimated:

```
suite duration        56.2 min
pipeline invocations  4
  initial      10.0 min · 18 tasks
  incremental   9.5 min · 18 tasks
  replay 1     13.7 min · 18 tasks
  replay 2      8.7 min · 18 tasks
                -------
                41.9 min of 56.2 (75%)
```

**The seed is nine rows.** Almost none of that 41.9 minutes is computation — it
is serverless task startup. The performance section already measures startup at
~60% of runtime on real data at 3M rows; at nine rows it approaches 100%.

### What the replays were running that nothing checked

A replay re-derives Silver and Gold from Bronze already held. Bronze exits
immediately — `reads_from_source()` is false for `replay` — and
[`assert_replay`](../tests/integration_databricks/assert_replay.py) inspects
`BRONZE`, `SILVER`, `GOLD`, `QUARANTINE`, `AUDIT` and `METRICS`. It never touches
marts, features or KPI views.

So each replay ran eleven tasks — three marts, four feature tables, four KPI
views — that no assertion looked at. Twice per suite.

### Why it was deferred, and what changed

The item said promotions are infrequent and one suite is simpler to reason about
than a fast subset plus a nightly remainder. That reasoning was sound, and the
cost is invisible on a weekly promotion.

**It is not invisible while debugging.** In a single session on 2026-08-20, five
or six suite cycles were spent confirming one fact each — an assertion notebook
whose checks never executed, three runs measuring the wrong environment, two
failed attempts at a stream restart. Roughly five hours, almost all of it task
startup. The cost is per *iteration*, not per promotion, and iterations are when
you can least afford an hour of latency.

## How

Each replay leg is now:

```
superstore_pipeline_master_run_id_init      (shared by both legs)
  └── replay_N_silver                       superstore_silver_layer_ETL_pipeline_orchestrator
        ├── replay_N_gold_dims              superstore_gold_dimensional_ETL_pipeline_orchestrator
        └── replay_N_gold_facts             superstore_gold_facts_ETL_pipeline_orchestrator
```

These are the **production orchestrator notebooks**, receiving the same
`run_mode`, `start_date`, `end_date` and `master_run_id` the pipeline job passes
them. Nothing is reimplemented — the suite simply stops invoking eleven layers
that a replay does not assert on.

### Both legs share one `master_run_id`

Not a choice. The layer orchestrators read their environment from a **hardcoded**
task key:

```python
env = dbutils.jobs.taskValues.get(
    taskKey="superstore_pipeline_master_run_id_init", key="SUPERSTORE_ENV")
```

A job may contain only one task with that key, so both legs depend on a single
init task. In production each replay is a separate job run with its own id.

The assertions are count-based on table contents rather than run ids, so this
does not affect them. Recorded because it is a genuine divergence from
production, not because it currently bites.

## What this gives up

**The replay legs are now a hand-maintained subset of the pipeline's internal
wiring.** Add a layer that a replay should cover, and these legs will not run it.
That is a real loss of fidelity and it is the reason the change is scoped this
narrowly:

- **Initial and incremental loads stay full `run_job_task` invocations.** They
  are what proves the *real invocation path* works — that the orchestrator wires
  layers together correctly, passes parameters correctly, and resolves the
  environment correctly.
- **The replays re-run the same code on the same data** with a different
  `run_mode`. Fidelity is already established by the time they start; they are
  checking a property, not a wiring.

That distinction matters because the fidelity risk is not hypothetical. In the
same session, `assert_schema_drift` resolved its environment through
`superstore_platform_config` instead of `assertion_helpers`, silently defaulting
to `dev`, and reported confidently about the wrong environment for three runs. A
harness that calls production code its own way can drift from how production
calls it — which is exactly why the *first two* loads were left alone.

### The guard against silent drift

[`assert_replay`](../tests/integration_databricks/assert_replay.py) already
requires a `REPLAY` `load_type` row in **both** `silver_layer_metrics` and
`gold_layer_metrics`:

```python
for layer, tbl in (("silver", "silver_layer_metrics"), ("gold", "gold_layer_metrics")):
    replays = spark.table(m).filter(col("load_type") == "REPLAY").count()
    check(f"{layer} metrics recorded a REPLAY load_type", replays >= 1,
          f"replay rows={replays} — 0 means {layer} skipped the replay entirely")
```

Written originally to catch the Gold windowing defect — where Silver updated,
Gold selected nothing, and every task went green. It now does double duty:
dropping Silver or Gold from these legs fails the suite rather than quietly
narrowing what is tested.

**It does not catch the other direction.** A layer *added* to the pipeline that a
replay should cover would be missed here, silently. That is the residual risk,
and there is no automated guard for it — only this paragraph.

## When

Every suite run, which is every push to `qa`. The saving is largest exactly when
it matters most: while iterating on a failure.

## Where

| Concern | Location |
|---|---|
| Replay leg definition | `resources/integration_test_job.job.yml`, tasks `replay_1_*` / `replay_2_*` |
| Layer orchestrators invoked | `superstore_orchestrator/layer_orchestrator/` |
| Assertions | `tests/integration_databricks/assert_replay.py` |
| Drift guard | the `REPLAY` `load_type` check, same file |
| Unchanged full-DAG legs | `run_pipeline_initial_load`, `run_pipeline_incremental_load` |

## The gap this change exposed

Scoping the replay legs meant asking, task by task, *what asserts on this?* That
question turned up something the old shape had hidden: **`assert_replay` never
touched Gold facts.**

It checks `dim_customers` and `dim_products`, plus Bronze/Silver/quarantine/audit
counts and metrics. Facts were re-derived on every replay, twice per suite, and
nothing looked at the result — under the full-DAG shape that was invisible among
eleven other unasserted tasks.

By the criterion used to remove the marts, `replay_N_gold_facts` should have gone
too. It did not, because the right fix was the opposite:

```python
merge_fact_into_gold(df, gold_tbl, natural_keys, hash_column, ...)
```

Facts merge on natural keys, so a replay **should** update in place rather than
append. That word is doing a lot of work. The backfill defect was a mechanism
assumed idempotent that appended 505 duplicate Bronze rows while every downstream
count stayed plausible — and Bronze at least had Silver's dedup absorbing the
damage. **Facts have no such absorber:** a duplicate fact row lands directly in
the marts.

`assert_gold_fact` does check grain uniqueness, but only after the initial load,
never after a replay. So the suite verified the property in the one mode where
duplication was least likely.

Two checks now close it, using the snapshot the replay legs already write:

| Check | Catches |
|---|---|
| `facts_N` row count unchanged between replay 1 and 2 | a merge that appends instead of updating |
| grain still unique after replay | duplication by any route |

Every task in the replay legs is now asserted on, which was not true before this
change *or* after the first version of it.

## What is not done

**The remaining two `run_job_task` invocations are ~19.5 minutes** and are staying
that way on purpose. Shortcutting those would remove the only thing in this suite
that exercises the real end-to-end invocation path, which is the property most
worth having.

**The measured saving is projected, not yet observed.** 31 fewer task startups
should remove roughly 20 minutes, but that number comes from arithmetic on the
previous run's timings. The first suite run after this change is what confirms
it — and if it does not, that is a finding rather than a rounding error.
