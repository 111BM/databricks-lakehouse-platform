# Freshness Alert — noticing when the pipeline does not run

## What

A Databricks SQL Alert that fires when no Silver entity has completed
successfully for more than **216 hours (9 days)**. Defined as a bundle resource
in
[`resources/superstore_freshness_alert.alert.yml`](../resources/superstore_freshness_alert.alert.yml),
deployed with everything else, and active in **prod only**.

## Why

The pipeline emails on **failure**. It says nothing when a run does not happen at
all — a paused schedule, a deleted job, a workspace outage, or a trigger that
quietly stopped firing all look exactly like a quiet week.

That gap became real the moment prod was scheduled. Before then it did not exist:
every run was someone typing `databricks bundle run`, so the operator *was* the
monitor. An unattended weekly run has no such observer.

### Why it cannot live inside the pipeline

A check that runs as a pipeline task can only report on runs that **happened**.
It is structurally incapable of noticing the pipeline not running — the one
failure it would exist to catch is the one that prevents it executing.

It therefore needs its own schedule, independent of the thing it watches.

### Why a SQL Alert rather than more code

An earlier iteration of this work routed data-quality monitors to Slack from
inside the pipeline, via a custom module with a payload builder, a webhook
secret and sixteen unit tests. Freshness was going to be built the same way, on
a second scheduled job.

Checking the bundle schema first would have avoided that: `config.Resources`
exposes an `alerts` resource with `query_text`, `evaluation`, `schedule`,
`warehouse_id` and subscriptions. Everything the custom module does, as
configuration — and with an independent cadence, which the in-pipeline design
could never have had.

The argument that had been keeping the custom approach alive was that SQL Alerts
are UI-configured workspace state, contradicting *"Git is the single source of
truth"*. That argument was simply wrong. They are bundle resources: versioned,
reviewed, promoted through dev → qa → prod like any job.

## How

```sql
SELECT
  ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(end_ts))) / 3600.0, 1)
    AS hours_since_last_success
FROM ${var.catalog}.${var.schema}_metrics.silver_layer_metrics
WHERE run_status = 'SUCCESS'
```

Four decisions in that query and its evaluation, each of which changes what the
alert is capable of detecting:

**Measured from `silver_layer_metrics`, not the Jobs API.** A job can report
SUCCESS while entities inside it fail — that happened on this platform before
the exception handling was fixed, when all four Silver entities failed and the
run went green. Filtering on `run_status = 'SUCCESS'` means a run that completed
while failing every table does **not** refresh the clock.

**`SKIPPED` does not count.** An incremental run with no new data is a healthy
run, but it is not evidence that processing works, and this alert is about
whether the pipeline is alive.

**`empty_result_state: TRIGGERED`.** A missing, empty or unreadable metrics table
fires the alert rather than reporting OK. Silence caused by a broken monitor is
the exact failure this backlog keeps rediscovering, so the default is set the
loud way.

**216 hours, not 168.** The schedule is weekly, so a 7-day threshold would fire
on any run that slipped by an hour. Nine days means a single late run is
tolerated and two consecutive misses are not.

Evaluated **daily at 08:00 Sydney**, roughly 13 hours after the Saturday 19:00
run. Daily rather than weekly so a missed run surfaces within a day instead of
waiting until the next scheduled Saturday. `retrigger_seconds: 86400` caps it at
one notification per day while it stays broken, and `notify_on_ok: true` closes
the incident when it recovers.

## Where

| Concern | Location |
|---|---|
| Alert definition | `resources/superstore_freshness_alert.alert.yml` |
| Bundle wiring | `include:` in `databricks.yml` |
| Per-target pausing | `targets.dev` / `targets.qa` in `databricks.yml` |
| Data source | `{env}_metrics.silver_layer_metrics` |

**Active in prod only.** Freshness presupposes a cadence, and only prod is
scheduled — in dev and qa the alert would report "stale" every day forever and
train the reader to ignore it. It is *paused* rather than deleted so all three
environments keep one definition.

The schema is parameterised for the same class of reason: hardcoding
`prod_metrics` would have made the dev and qa copies monitor **production**, so a
healthy prod would have silenced all three while dev sat broken.

## What this does not cover

**Data freshness.** This measures whether the *pipeline* ran, not whether the
*data* is recent. The source is a static dataset, so the newest `order_date` is
whatever the original CSV held regardless of how often the pipeline runs — a
recency check on the data would be permanently red. With a real vendor feed both
would be worth having; here only the first is meaningful.

**Delivery is unverified.** The alert is defined and validates across all three
targets, and the query has been run against real metrics. Nothing has yet
confirmed that a triggered alert arrives — that needs either a deliberately
lowered threshold or a genuinely missed run.
