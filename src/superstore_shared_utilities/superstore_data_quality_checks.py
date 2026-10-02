"""
==============================================================
Module: Data quality check results
==============================================================

Purpose
-------
Persist the platform's data-quality checks, one row per check per run, to
`{env}_metrics.data_quality_checks`, so that Databricks SQL Alerts -- bundle
resources, like the freshness alert -- can query them and notify a person.

Why this exists
---------------
The checks were computed and only LOGGED. A SQL Alert can only query data that
is stored, and logs are read by nothing: the 5%-of-revenue orphan problem sat
in correctly-written log lines, unread. Worse, the reconciliation monitor
(`superstore_reconciliation.log_reconciliation`) was built and unit-tested on
2026-08-14 and never called by the pipeline at all -- prod had never checked
that every Bronze row is accounted for. This module is where that is fixed.

It replaces `superstore_alerting`, which posted to a Slack webhook from inside
the pipeline and was never configured. Delivery now belongs to SQL Alerts and a
Databricks notification destination; the pipeline only records facts.

Design rules
------------
1. **Record, never decide delivery.** The pipeline writes what it observed;
   whether a person is told is the alert's job, configured as a bundle resource.
2. **A failed check does not fail the run.** An unbalanced reconciliation is
   recorded with passed = false and alerted on; the data already merged stays.
   Monitoring that turns good runs red teaches people to ignore red runs.
3. **A failure to RECORD does fail the run.** If the results cannot be written,
   the alerts have nothing to read and would stay silent -- the "reports
   success while doing nothing" shape this project keeps finding.
4. **Pure core, thin shell.** Row construction is pure and unit-tested; the
   Spark calls are a few lines around it.
==============================================================
"""

import json
from datetime import datetime, timezone

TABLE_NAME = "data_quality_checks"

# Explicit, not inferred: a schema inferred from the first batch of rows would
# differ by check type (int vs float values), and appends would then fail or
# silently widen columns.
TABLE_SCHEMA = (
    "master_run_id STRING, env STRING, check_name STRING, subject STRING, "
    "observed_value DOUBLE, expected_value DOUBLE, passed BOOLEAN, "
    "details STRING, checked_at TIMESTAMP"
)

CHECK_RECONCILIATION = "reconciliation"

_RECONCILIATION_TERMS = (
    "bronze_rows", "silver_rows", "quarantine_rows", "audit_rows",
    "superseded_rows", "accounted_rows",
)


def reconciliation_check_row(env, master_run_id, entity, result, checked_at):
    """
    Turn one `reconciliation_sql` result into a check row. Pure.

    observed_value = rows accounted for (silver + quarantine + audit +
    superseded); expected_value = Bronze rows. `passed` is the query's own
    `balanced` flag rather than a recomputed comparison, so the definition of
    "balanced" lives in exactly one place.

    `result` is any mapping with the reconciliation terms and `balanced` -- a
    Spark Row (via asDict) or a plain dict in tests.
    """
    terms = {k: int(result[k]) for k in _RECONCILIATION_TERMS}
    return {
        "master_run_id": master_run_id,
        "env": env,
        "check_name": CHECK_RECONCILIATION,
        "subject": entity,
        "observed_value": float(terms["accounted_rows"]),
        "expected_value": float(terms["bronze_rows"]),
        "passed": bool(result["balanced"]),
        "details": json.dumps(terms, sort_keys=True),
        "checked_at": checked_at,
    }


def record_checks(spark, catalog, metrics_schema, rows):
    """
    Append check rows to `{catalog}.{metrics_schema}.data_quality_checks`,
    creating the table on first use. Raises on any failure (rule 3).
    """
    table = f"{catalog}.{metrics_schema}.{TABLE_NAME}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({TABLE_SCHEMA})")
    if rows:
        (spark.createDataFrame(rows, schema=TABLE_SCHEMA)
              .write.mode("append").saveAsTable(table))
    return table


def run_reconciliation_checks(
    spark, logger, *, catalog, env, schemas, entities, master_run_id, layer_run_id=None,
):
    """
    Reconcile every entity, log each result, and record all of them.

    `schemas`: dict with bronze, silver, quarantine, audit, metrics schema names.
    `entities`: list of (entity_name, business_keys).

    Returns the recorded rows, so a caller can report or assert on them.
    """
    from superstore_reconciliation import reconciliation_sql
    from superstore_logger import log_event

    checked_at = datetime.now(timezone.utc)
    rows = []
    for entity, business_keys in entities:
        result = spark.sql(
            reconciliation_sql(
                catalog, schemas["bronze"], schemas["silver"], schemas["quarantine"],
                schemas["audit"], entity, business_keys,
            )
        ).first().asDict()
        row = reconciliation_check_row(env, master_run_id, entity, result, checked_at)
        rows.append(row)
        log_event(
            logger,
            "INFO" if row["passed"] else "ERROR",
            f"reconciliation for {entity}: bronze={int(row['expected_value'])}, "
            f"accounted={int(row['observed_value'])} ({row['details']})",
            entity=entity,
            reconciliation_balanced=row["passed"],
            master_run_id=master_run_id,
            layer_run_id=layer_run_id,
            layer="Silver",
        )
    record_checks(spark, catalog, schemas["metrics"], rows)
    return rows
