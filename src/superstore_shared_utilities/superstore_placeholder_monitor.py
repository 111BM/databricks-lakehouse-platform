"""
==============================================================
Module: Placeholder Exposure Monitor
==============================================================

Purpose
-------
Report how much of a Gold dimension is a placeholder rather than a real value.

Why this exists
---------------
Severity tiers (docs/SEVERITY_TIERS.md) stopped a row being quarantined for a
missing descriptive attribute. Gold now substitutes 'Unknown' so the dimension
is complete and the facts resolve.

That changed the shape of the failure rather than removing it. Revenue that used
to be ABSENT from reports is now ATTRIBUTED to a bucket carrying no information,
which is an improvement only because it is visible -- and only if something
looks.

Nothing did. The orphaned-fact counters in the marts now read 0 permanently:
they measure a condition that severity tiers made impossible, so they will keep
reading 0 whether the next feed is pristine or 40% incomplete. The metric that
tracked this failure went blind when the failure moved.

This is its successor. Same principle as the orphan count in
docs/REFERENTIAL_COMPLETENESS.md:

    "The number to watch is not the absolute count but whether it grows per run."

An absolute figure is expected and harmless -- some entities genuinely have no
value anywhere. A figure that climbs run over run means a source started
arriving incomplete, which is the event worth catching.

The SQL builder is kept pure so it can be unit tested without a catalog.
==============================================================
"""

PLACEHOLDER = "Unknown"


def placeholder_count_sql(
    catalog: str,
    schema: str,
    dimension_table: str,
    attribute_columns: list,
    placeholder: str = PLACEHOLDER,
    current_only: bool = True,
) -> str:
    """
    Build the SQL counting current dimension rows carrying `placeholder` in any
    of `attribute_columns`.

    Pure: returns a string, touches nothing. The caller executes it.

    Counts a ROW once however many of its attributes are placeholders -- the
    question is "how much of this dimension is uninformative", not "how many
    fields are blank".

    Args:
        catalog / schema / dimension_table: fully qualifies the table.
        attribute_columns: descriptive columns to inspect. Business keys should
            not appear here; they are never substituted.
        placeholder: token Gold substitutes.
        current_only: restrict to the live SCD2 version. Historic versions
            legitimately hold whatever was known at the time.

    Returns:
        SQL returning current_rows, placeholder_rows.

    Raises:
        ValueError: if no attribute columns are given -- a monitor over nothing
            would report a reassuring zero forever, which is the failure mode
            this module exists to prevent.
    """
    if not attribute_columns:
        raise ValueError(
            f"attribute_columns is empty for {dimension_table}: a placeholder "
            f"monitor with no columns silently reports zero forever"
        )

    predicate = " OR ".join(f"{c} = '{placeholder}'" for c in attribute_columns)
    where = "WHERE is_current = true" if current_only else ""

    return (
        "SELECT\n"
        "  COUNT(*) AS current_rows,\n"
        f"  SUM(CASE WHEN {predicate} THEN 1 ELSE 0 END) AS placeholder_rows\n"
        f"FROM {catalog}.{schema}.{dimension_table}\n"
        f"{where}"
    ).strip()


def log_placeholder_exposure(
    spark,
    logger,
    catalog: str,
    schema: str,
    dimension_table: str,
    attribute_columns: list,
    master_run_id: str,
    layer: str = "Mart",
    placeholder: str = PLACEHOLDER,
):
    """
    Execute the count and log it, WARN when any row is a placeholder.

    Thin imperative shell over `placeholder_count_sql`. Mirrors the orphaned-fact
    logging the marts already do: it fixes nothing and changes no reported
    number, it converts an invisible condition into a monitored one.

    Returns the counts so a caller can assert on them.
    """
    from superstore_logger import log_event

    row = spark.sql(
        placeholder_count_sql(
            catalog, schema, dimension_table, attribute_columns, placeholder
        )
    ).first()

    current_rows = row["current_rows"] or 0
    placeholder_rows = row["placeholder_rows"] or 0
    pct = round(100 * placeholder_rows / current_rows, 4) if current_rows else 0.0

    log_event(
        logger,
        "WARN" if placeholder_rows else "INFO",
        f"{dimension_table} rows carrying a '{placeholder}' attribute: "
        f"{placeholder_rows} of {current_rows} ({pct}%)",
        placeholder_rows=placeholder_rows,
        dimension_rows=current_rows,
        placeholder_pct=pct,
        dimension_table=dimension_table,
        master_run_id=master_run_id,
        layer=layer,
    )

    return {
        "current_rows": current_rows,
        "placeholder_rows": placeholder_rows,
        "placeholder_pct": pct,
    }
