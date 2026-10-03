# Data Quality Alerts — recorded every run, delivered to email and Slack

> **Status (2026-10-03): live in prod and verified.** The pipeline records its
> data-quality checks every run; four SQL Alerts notify email and Slack. The first
> automated prod reconciliation balanced all four entities on 1,010,534 rows.

## What

| Check | Recorded by | Alert fires when |
|---|---|---|
| **Reconciliation** — `bronze == silver + quarantine + audit + superseded`, per entity | Silver, after every successful run | any entity does not balance in the latest run |
| **Orphaned facts** — facts with no current dimension version | `mart_customer_360` (orders → customers), `mart_production_performance` (sales → products) | the latest count is above 0 |
| **Schema drift** — the source changed shape | Bronze (already recorded, in `{env}_metrics.schema_drift`) | the latest run has a `MISSING` or `NEW` column, or `RESCUED` rows |
| **Placeholder exposure** — share of a dimension's rows carrying `'Unknown'` | the same two marts | a dimension's share grew by more than 5 points since its previous run |

Checks are written to **`{env}_metrics.data_quality_checks`**, one row per check per
run: `master_run_id`, `env`, `check_name`, `subject`, `observed_value`,
`expected_value`, `passed`, `details` (JSON), `checked_at`. The four alerts are bundle
resources in
[`resources/superstore_data_quality_alerts.alert.yml`](../resources/superstore_data_quality_alerts.alert.yml),
evaluated daily at 08:00 Sydney, delivered to email and to
`#superstore-data-platform-alerts` through the Databricks notification destination.
What to do when one fires: **[ALERT_RESPONSE.md](ALERT_RESPONSE.md)**.

## Why

Before this, a **crashed** job or a **stopped** pipeline reached a person (failure emails,
the freshness alert) — but **wrong data** reached no one:

- The checks were computed and **only logged**. Logs are read by nothing: a
  5%-of-revenue orphan problem once sat in correctly-written log lines, unread.
- The fix attempted for that, `superstore_alerting`, posted to a Slack webhook from
  inside the pipeline. The webhook was **never configured**, so it delivered nothing.
- And measuring the replacement found the worst gap: the reconciliation monitor,
  `log_reconciliation`, was written and unit-tested on 2026-08-14 and **never called
  by the pipeline**. Prod had never automatically checked that every Bronze row is
  accounted for — reconciliation ran only in the qa integration suite and by hand.

## Which approach, and why

| Option | Verdict |
|---|---|
| **Pipeline records facts; SQL Alerts decide delivery** | **Chosen.** Delivery is a versioned, reviewed bundle resource, promoted dev → qa → prod like any job; the same proven mechanism as the freshness alert; one notification destination for everything |
| Configure the existing webhook (`superstore/slack_webhook_url`) | Rejected: keeps delivery logic and a second copy of a webhook URL inside pipeline code — the design the README had already called wrong |
| Fail the pipeline run on a failed check | Rejected for these checks: the data is already merged when they run, and monitoring that turns good runs red teaches people to ignore red runs. A failure to **record** does fail the run — an alert reading an empty table would stay silent |
| Alert on any placeholder rows | Rejected: placeholders are normal at a steady level, so it would fire forever and get muted. Growth between runs is the signal |

## How

1. **Silver reconciles after every successful run** —
   `superstore_data_quality_checks.run_reconciliation_checks`, called by the Silver
   orchestrator once all entities succeeded, never in a dry run. The verdict also goes in
   Silver's exit string (`SILVER_DONE reconciliation_checked=4 unbalanced=none`), the only
   output the Jobs API returns.
2. **The marts record** orphaned facts (pass only at 0 — structurally impossible since
   severity tiers, so any count is a new cause) and placeholder exposure (informational;
   the alert compares runs). `mart_sales_daily` computes the same orphan counts and only
   logs them, so each is recorded once.
3. **Row construction is pure and unit-tested**; the Spark write is a thin shell with an
   explicit schema, so appends never fail on an inferred type.
4. **Each alert query is an aggregate returning one number, 0 — never NULL — when there
   is nothing to report**, and looks only at the latest recorded run. This is the
   freshness alert's lesson: `NULL > 0` is not TRUE, so a NULL reads as OK whatever
   happened. A missing table makes the query fail instead, which Databricks reports as
   ERROR — loud, not silent.
5. **Alerts live in the bundle's own folder** (`${workspace.resource_path}`), not a
   person's home: they are created by the deploying service principal, which cannot
   write into anyone's home folder.
6. **Active in prod only.** dev and qa pause them: their data is test data, and the
   integration suite deliberately sends a new column and dirty rows.
7. **`superstore_alerting` deleted** — the module, its 16 tests, and
   `log_reconciliation`, its last user.

## When it runs

| Event | What happens |
|---|---|
| Each pipeline run | checks recorded to `{env}_metrics.data_quality_checks`; drift to `{env}_metrics.schema_drift` |
| Daily 08:00 Sydney | each prod alert evaluates the latest recorded run |
| A check fails | TRIGGERED → email + Slack, repeated at most daily while it stays failed |
| It recovers | OK → a recovery notification (`notify_on_ok`) |
| The query itself fails (e.g. a missing table) | ERROR → email + Slack |

## Where

| Piece | Location |
|---|---|
| Check rows and recording | [`superstore_data_quality_checks.py`](../src/superstore_shared_utilities/superstore_data_quality_checks.py) |
| Reconciliation call | Silver orchestrator, end of the run |
| Orphan / placeholder recording | `mart_customer_360`, `mart_production_performance` |
| Alerts | [`resources/superstore_data_quality_alerts.alert.yml`](../resources/superstore_data_quality_alerts.alert.yml); paused in dev/qa in [`databricks.yml`](../databricks.yml) |
| Unit tests | `tests/unit/shared/test_data_quality_checks.py` (10) |
| Response playbook | [ALERT_RESPONSE.md](ALERT_RESPONSE.md) |

## Before and after

| | Before | After |
|---|---|---|
| Reconciliation in prod | **never run** | every Silver run, recorded, alerted |
| Orphaned facts | logged; webhook never configured | recorded, alerted at > 0 |
| Schema drift | recorded; no alert | alerted on `MISSING`, `NEW`, `RESCUED` rows |
| Placeholder exposure | logged | recorded; alerted on growth > 5 points |
| Who hears about wrong data | nobody | email + `#superstore-data-platform-alerts` |
| Delivery mechanism | Python posting to an unconfigured webhook | SQL Alerts + one notification destination |

## Verification

| Claim | Evidence |
|---|---|
| Reconciliation runs in every Silver mode | qa integration run `384501441550609`: all four Silver runs (initial, incremental, two replays) exited `reconciliation_checked=4 unbalanced=none` |
| Checks are recorded correctly | qa integration run `796673863256601`: 16 reconciliation rows, 4 orphan rows (all 0), 4 placeholder rows (customers 16.67%, products 0%) — all as predicted from the seed |
| Each alert query returns one non-NULL number | run against real data before deploying: integration_test 0 / 0 / **2** / 0.0, prod drift 0 |
| The drift alert can fire | the **2** is the column the suite deliberately adds — a real positive, not a synthetic one |
| Service principals can create the alerts | the qa deploy created all four, owned by and running as `superstore-ci-qa` |
| Prod is healthy and recorded | prod run `384790915967386`: reconciliation balanced on **1,010,534** rows for all four entities; orphans 0; placeholders customers 0.07%, products 0.25% (the baseline for the growth alert); all four prod alert queries return 0; `[prod] reconciliation` evaluated OK |
| Notifications reach Slack | `[qa] reconciliation`, evaluated by hand: ERROR (table absent in qa), notified email and `superstore-data-platform-alerts`, no failed destinations |

## What this does not cover

- **A check that stops being recorded.** If a future change stopped Silver recording
  reconciliation, the alert would keep evaluating the last recorded run and stay OK. The
  freshness alert covers the pipeline not running; nothing yet covers a check silently
  not running. A guard would be an integration assertion that every expected check has a
  row — the same lesson as the monitors in the README that went blind.
- **The placeholder threshold** (5 points) is a starting value with one run of history.
- **Prod-only.** dev and qa alerts are paused, and qa's would have no data anyway: the qa
  pipeline only ever runs as `integration_test`.
- **`mart_sales_daily`** logs orphan counts without recording them, by design.
