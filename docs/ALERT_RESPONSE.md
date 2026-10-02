# Alert Response — what each alert means and what to do

Operator companion to **[BACKFILL_QUICK_REFERENCE.md](BACKFILL_QUICK_REFERENCE.md)**, which
covers run modes and recovery procedures. This one covers the other direction: an alert
has fired, what now.

**There are no escalation tiers and no rota, deliberately.** This platform has one
operator. A page-the-secondary-after-15-minutes policy would be a fabricated artifact.
What survives that omission is the half that actually gets used even in large teams — what
an alert means, what to check first, and what to do — so that is what this records.

## Where alerts arrive

| Source | Email (`bireshmoktan@gmail.com`) | Slack `#superstore-data-platform-alerts` |
|---|---|---|
| Pipeline job failure (any environment) | ✅ | ✅ — verified 2026-10-03, run `290632771048520` |
| Integration suite failure | ✅ | ✅ — same destination; not yet exercised by a real failure |
| Freshness alert (prod) | ✅ — verified 2026-08 | ✅ wired — not yet fired to Slack |

Slack delivery goes through a Databricks **notification destination**
(`superstore-data-platform-alerts`, ID `9da11076-679f-4b8b-b29b-c815842f4b35`), which
holds the channel's webhook URL. The URL is a secret and lives only there; the bundle
references the destination by ID. Email is kept as the fallback: if the Slack app is
removed or its webhook revoked, failures still reach a person.

A message in the channel names the job, the run and the failed task, with a link to the
run. Start from the run page, then use the sections below.

---

## Before anything else: a green run is not evidence

`bronze_to_silver_prod` catches its own exceptions and reports them in a metrics column.
That was fixed (the orchestrator now raises after attempting every entity), but the habit
is worth keeping, because it cost a full replay cycle to learn:

```sql
SELECT target_table, run_status, notes
FROM superstore_catalog.<env>_metrics.silver_layer_metrics
ORDER BY end_ts DESC LIMIT 8;
```

If any row reads `failure`, treat the run as failed regardless of what the Jobs UI says.

Note the column is `target_table`, not `table_name`.

---

## What is NOT an alert

Two measurements look alarming and are normal. Neither is wired to alerting, and neither
should send you looking for a defect:

| Measurement | Why it is fine |
|---|---|
| `superseded_rows > 0` | Normal under incremental loading. Silver keeps one row per key, Bronze one per arrival; a key arriving again in a later run supersedes the earlier version. Goes to 0 after a replay. See **[RECONCILIATION_INVARIANT.md](RECONCILIATION_INVARIANT.md)** |
| placeholder rows > 0 | Normal for an entity with genuinely no better value anywhere. Watch the **trend**, not the number. See **[SEVERITY_TIERS.md](SEVERITY_TIERS.md)** |

Alerting on either would produce a permanently red channel, and a permanently red channel
is a muted one — after which the real alert is invisible too.

---

## Alert: Reconciliation failed for `<entity>`

**Severity:** ERROR. This is the strongest guarantee in the platform breaking.

**What it means.** A Bronze row exists that no rule explains. Every row should be in Silver,
in quarantine, in audit, or superseded by a later arrival of the same key.

**What it does not mean.** It is *not* the same as `superseded_rows` being large. The alert
fires on `balanced = false`, not on any single term.

**First query** — re-run the check and read the terms:

```sql
-- from superstore_reconciliation.reconciliation_sql; run per entity
SELECT COUNT(*) FROM superstore_catalog.<env>_bronze.<entity>;
SELECT COUNT(*) FROM superstore_catalog.<env>_silver.<entity>;
SELECT COUNT(*) FROM superstore_catalog.<env>_quarantine.<entity>_dirty;
SELECT COUNT(*) FROM superstore_catalog.<env>_audit.<entity>_duplicates;
```

**Likely causes, in order of probability:**

1. **A partial run.** A task failed midway, so Silver has rows whose quarantine or audit
   counterparts were never written. Check `run_status` above first.
2. **Two runs overlapping.** The `integration_test_*` schemas are global and guarded by
   `max_concurrent_runs: 1`, but a manual `bundle run` against dev while another is in
   flight will corrupt counts the same way. Check for concurrent runs.
3. **A genuine accounting gap** — a new code path that drops or duplicates rows.

**Remediation.** For 1 and 2, re-run: `run_mode=replay` over the affected window
re-derives Silver and Gold from Bronze and restores the balance. For 3, the counts tell you
the direction — a sum *below* Bronze means rows are being lost, *above* means duplicated,
and the second is the more serious.

---

## Alert: Orphaned facts detected in `<fact_table>`

**Severity:** ERROR.

**What it means.** Facts reference a dimension member that does not exist, so their revenue
is absent from every mart while all upstream checks pass. This is how 49,539 fact rows and
5% of revenue went missing from product reporting.

**Why it is worth waking for.** Since severity tiers, this count is **structurally 0** — a
dimension is no longer removed for a descriptive violation. Any non-zero value means a
cause nobody has anticipated, not a recurrence of the known one.

**First query:**

```sql
SELECT COUNT(*) AS orphans
FROM superstore_catalog.<env>_gold.facts_sales s
WHERE NOT EXISTS (
  SELECT 1 FROM superstore_catalog.<env>_gold.dim_products p
  WHERE p.product_id = s.product_id AND p.is_current = true
);
```

Then find out *why* the dimension is missing — that decides the fix:

```sql
-- is the key quarantined, and on which column?
SELECT filter(error_columns, x -> x IS NOT NULL) AS violated, COUNT(*)
FROM superstore_catalog.<env>_quarantine.products_dirty
GROUP BY 1 ORDER BY 2 DESC;
```

**Likely causes:**

| Finding | Cause | Fix |
|---|---|---|
| Key is in quarantine with a **fatal** violation | genuinely unusable row | fix at source |
| Key is in quarantine on a **descriptive** column | the severity map is wrong for that column | config change, see **[SEVERITY_TIERS.md](SEVERITY_TIERS.md)** |
| Key is **not in quarantine at all** | the dimension never arrived, or a new source dialect | see **[VALUE_STANDARDIZATION.md](VALUE_STANDARDIZATION.md)** |
| Dimension exists but has no `is_current` row | soft-delete path | **[REFERENTIAL_COMPLETENESS.md](REFERENTIAL_COMPLETENESS.md)** |

**Remediation.** Fix the rule or the config, then `run_mode=replay` over the affected
window. The facts are already correct and are never rewritten — only the dimension side
changes.

---

## Alert: job failure email

**Severity:** ERROR. Sent by the job itself on any task failure.

**First actions, in order:**

1. Check `silver_layer_metrics.run_status` — the query at the top of this page. A failed
   entity names its own error in `notes`.
2. Open the failed task in the Jobs UI and read the exception.
3. If the Silver task failed, **downstream tasks were skipped**, so Gold and the marts hold
   the previous run's data. That is correct behaviour, not a second fault.

**Two causes seen in practice:**

- **An unsupported Spark configuration on Serverless.** `CONFIG_NOT_AVAILABLE` means the
  setting is blocked; use the per-operation option instead (`.option("mergeSchema", "true")`
  on a write, `.withSchemaEvolution()` on a merge).
- **A config key that never reached the code.** The Silver orchestrator rebuilds each
  entity's config from an explicit allowlist, so a new YAML key is dropped silently unless
  it is added in **both** the reconstruction and the call.

---

## Alert: `NOT ROUTED` in the logs

**Severity:** WARN, and it is about the alerting itself.

No webhook secret is configured for `superstore_alerting`, so one of its data-quality
alerts fired and went nowhere. Job failures and the freshness alert are unaffected — they
reach Slack through the notification destination above, not this module. Set
`superstore/slack_webhook_url` in the Databricks secret scope only if this module's route
is kept.

This is logged rather than silent on purpose: an unrouted workspace and a healthy one must
not look identical.

---

## Weekly schedule

Prod runs Mondays 09:00 Sydney. A missed run currently produces **no alert** — the
freshness check that would catch it is the remaining open item in the backlog. Until it
exists, a silent Monday is the one failure this document cannot help you with.
