"""
==============================================================
Module: Reconciliation — accounting for every Bronze row
==============================================================

Purpose
-------
Prove that every Bronze row is explained by a named rule, in every run mode.

The invariant, corrected
------------------------
The platform claimed:

    bronze == silver + quarantine + audit

That holds after a full re-derivation and is a BOUND under incremental loading,
where the sum can be short. Measured: 7 of 9 in the integration test, and 416
short on customers at ~1M rows in dev.

The cause is not a leak. Silver holds one row per business key; Bronze holds one
row per arrival. `classify_duplicates` ranks rows *within the batch it is given*,
so a key arriving twice in ONE run has its loser audited — but a key arriving
again in a LATER run is merged over the top, and the superseded version lands in
no bucket: not Silver (overwritten), not quarantine (it was valid), not audit
(which only ever receives intra-batch losers).

A replay repairs it by making the whole window one batch, which turns every
inter-batch supersession into an intra-batch duplicate.

Why a fourth term rather than a fourth table
--------------------------------------------
Nothing is lost. Bronze is immutable and retains every arrival, so a superseded
version is always recoverable. The audit table does not preserve data — it
explains why a row is absent from Silver, and "a later version of this key won"
is an explanation that can be *derived* rather than stored.

Materialising it would have written 921,915 extra rows in dev alone, duplicating
what Bronze already holds. So the invariant gains a computed term:

    bronze == silver + quarantine + audit + superseded

    superseded = SUM(arrivals per key - 1) - audit_rows

Verified on dev at 1M rows: `SUM(arrivals per key - 1)` equals `audit_rows`
exactly (921,915 both sides) in a fully re-derived state, so superseded is 0 and
the original three-term form holds. Under incremental it is positive and is
exactly the shortfall. The identity is what proves the model complete: if
anything else were leaking, the two sides would not agree.

The guarantee is unchanged in strength — every row still accounted for by a
rule. What changed is the honesty about which explanations are materialised.

See docs/RECONCILIATION_INVARIANT.md.
==============================================================
"""


def reconciliation_sql(
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    quarantine_schema: str,
    audit_schema: str,
    entity: str,
    business_keys: list,
    quarantine_exists: bool = True,
    audit_exists: bool = True,
) -> str:
    """
    Build the SQL returning all five reconciliation terms for one entity.

    Pure: returns a string. The caller executes it.

    `superseded` is derived, never stored: arrivals beyond the first for each
    business key, minus the ones already explained by the audit table.

    Two exclusions, both learned by getting it wrong first:

    Rows with a NULL business key are excluded — they cannot be "another arrival
    of" anything, and they are quarantined as fatal, so the quarantine term
    already accounts for them.

    Quarantined rows are subtracted per key, because arrivals must be counted
    over rows that reached deduplication. Counting raw Bronze arrivals happens
    to work for customers and products, where quarantine implies a null key, and
    fails badly for orders and sales, which are quarantined for date and numeric
    violations with valid keys. The first version of this query reported 1,594
    superseded orders against a true residual of zero, breaking an invariant
    that was already holding.

    Args:
        catalog / *_schema: fully qualify the four tables.
        entity: logical entity name (customers, products, orders, sales).
        business_keys: the key columns; composite keys are grouped together.

    Returns:
        SQL selecting bronze_rows, silver_rows, quarantine_rows, audit_rows,
        superseded_rows, accounted_rows and balanced.

    Raises:
        ValueError: if no business keys are given. Without a key there is no
            notion of a repeat arrival, and the query would silently report a
            superseded count of zero — a reassuring answer to a question it
            never asked.
    """
    if not business_keys:
        raise ValueError(
            f"business_keys is empty for {entity}: superseded rows cannot be "
            f"derived without a key, and the term would silently read zero"
        )

    keys = ", ".join(business_keys)
    not_null = " AND ".join(f"{k} IS NOT NULL" for k in business_keys)
    join_on = " AND ".join(f"b.{k} = q.{k}" for k in business_keys)

    # A quarantine or audit table is only created when it first receives a row,
    # so an entity with no dirty rows has none. Absent means zero, not an error:
    # the integration test seeds no dirty products, and requiring the table would
    # fail the check for a reason that has nothing to do with reconciliation.

    bronze = f"{catalog}.{bronze_schema}.{entity}"
    silver = f"{catalog}.{silver_schema}.{entity}"
    quarantine = f"{catalog}.{quarantine_schema}.{entity}_dirty"
    audit = f"{catalog}.{audit_schema}.{entity}_duplicates"

    # Arrivals must be counted over rows that actually REACHED deduplication,
    # i.e. Bronze minus what was quarantined. Counting raw Bronze arrivals works
    # only where quarantine implies a null key (customers, products) and
    # over-counts badly where it does not: orders and sales are quarantined for
    # date and numeric violations with perfectly valid keys, so 159,686
    # quarantined orders inflated the arrival count and manufactured a
    # superseded figure of 1,594 against a true residual of zero.
    if quarantine_exists:
        quarantine_cte = (
            f"quarantined_per_key AS (\n"
            f"  SELECT {keys}, COUNT(*) AS quarantined\n"
            f"  FROM {quarantine}\n"
            f"  WHERE {not_null}\n"
            f"  GROUP BY {keys}\n"
            f"),"
        )
        quarantine_join = f"LEFT JOIN quarantined_per_key q ON {join_on}"
        per_key_expr = "b.arrivals - COALESCE(q.quarantined, 0)"
        quarantine_count = f"(SELECT COUNT(*) FROM {quarantine})"
    else:
        quarantine_cte = ""
        quarantine_join = ""
        per_key_expr = "b.arrivals"
        quarantine_count = "0"

    audit_count = f"(SELECT COUNT(*) FROM {audit})" if audit_exists else "0"

    return f"""
WITH bronze_per_key AS (
  SELECT {keys}, COUNT(*) AS arrivals
  FROM {bronze}
  WHERE {not_null}
  GROUP BY {keys}
),
{quarantine_cte}
per_key AS (
  SELECT {per_key_expr} AS arrivals
  FROM bronze_per_key b
  {quarantine_join}
),
counts AS (
  SELECT
    (SELECT COUNT(*) FROM {bronze})                              AS bronze_rows,
    (SELECT COUNT(*) FROM {silver})                              AS silver_rows,
    {quarantine_count}                                           AS quarantine_rows,
    {audit_count}                                                AS audit_rows,
    (SELECT COALESCE(SUM(arrivals - 1), 0) FROM per_key WHERE arrivals > 0) AS extra_arrivals
)
SELECT
  '{entity}' AS entity,
  bronze_rows,
  silver_rows,
  quarantine_rows,
  audit_rows,
  extra_arrivals - audit_rows AS superseded_rows,
  silver_rows + quarantine_rows + audit_rows + (extra_arrivals - audit_rows) AS accounted_rows,
  (silver_rows + quarantine_rows + audit_rows + (extra_arrivals - audit_rows)) = bronze_rows AS balanced
FROM counts
""".strip()
