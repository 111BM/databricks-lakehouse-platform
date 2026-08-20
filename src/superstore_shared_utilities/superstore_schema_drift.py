"""
==============================================================
Module: Schema Drift Detection
==============================================================

Purpose
-------
Compare the columns that actually arrived in `superstore_raw` against the
columns the entity configs declare, and turn the difference into rows a metrics
table can hold.

Why this exists
---------------
Auto Loader is configured with `schemaEvolutionMode = addNewColumns`, so a new
source column reaches `superstore_raw` intact -- verified: the table carries a
`col__rescued_data` column and 28 columns in total. Nothing is lost at Bronze.

The entity split then does this:

    all_columns = entity_cfg["columns"] + entity_cfg["metadata_columns"]
    entity_df = df_entity.select(*all_columns)

`columns` is a hardcoded list in YAML, so the contract is an explicit allowlist
and a column that is not on it never leaves Bronze. That is CORRECT -- a human
deciding what enters the model is the point, and this module deliberately does
not change it.

What is wrong is that the decision happens by silence. A new column is dropped
with no exception, no log line and no metric, so the data sits in
`superstore_raw` indefinitely while everyone believes it was never received.
`col__rescued_data` has the same problem one level down: it exists, it is
populated by Auto Loader, and `grep -rn "rescued"` across every .py/.yaml/.yml
in this repo returns nothing. A safety net nobody checks.

The three drift types are not equal
-----------------------------------
  added   -> lands in superstore_raw, dropped at the split.  SILENT
  removed -> select() on a missing column raises AnalysisException.  LOUD
  retyped -> value rescued, typed column goes NULL.  SILENT

The loud one is the safe one: it fails the same day and someone fixes it. The
quiet ones are what this module is for. Same shape as every other defect in
docs/ -- the pipeline already knows, it just never writes it down.

Scope
-----
Detection only. This module does not drop, promote, repair or alert. It reports
what differs so the caller can record it, and a new column stays out of Silver
until someone edits the config on purpose.

Pure so it can be unit tested without a cluster, a catalog or a source file.
==============================================================
"""

from datetime import datetime

# Columns the pipeline itself adds after ingestion. They are never in the source
# feed, so counting them as "new" would report permanent drift on every run.
PIPELINE_ADDED_COLUMNS = frozenset(
    {
        "bronze_ingestion_ts",
        "ingestion_date",
        "source_file_path",
        "source_file_name",
        "source_file_size_bytes",
        "source_file_modification_time",
    }
)

# Auto Loader's rescue column. Named by the `cloudFiles` reader rather than by
# us -- it appears as `col__rescued_data` in this workspace -- so it is matched
# on suffix rather than an exact string.
RESCUE_COLUMN_SUFFIX = "_rescued_data"

# A column present at the source that no entity config declares. Informational:
# nothing is broken and nothing is lost, but the model is ignoring data that has
# started arriving.
DRIFT_NEW = "NEW"

# A column an entity config declares that is no longer present at the source.
# This one is fatal downstream -- the entity split's select() will raise on it.
DRIFT_MISSING = "MISSING"

# Not a column-level event: the per-run count of rows Auto Loader could not fit
# into the inferred schema. This is the ONLY signal for a type change, which is
# otherwise completely silent -- the typed column simply goes NULL.
DRIFT_RESCUED = "RESCUED"


# Substrings identifying Auto Loader's "a new column arrived" failure. Matched on
# the message rather than the exception class because it surfaces wrapped in a
# StreamingQueryException, and the class alone cannot distinguish it from any
# other stream failure -- restarting blindly on those would retry genuine faults.
#
# Observed 2026-08-17 in dev and integration_test:
#   [UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE] Encountered unknown fields
#   during parsing: [Discount Reason], which can be fixed by an automatic
#   retry: true
SCHEMA_EVOLUTION_MARKERS = ("UNKNOWN_FIELD_EXCEPTION", "NEW_FIELDS_IN_FILE")


def is_schema_evolution_error(message: str) -> bool:
    """
    True when an exception is Auto Loader reporting a new source column rather
    than a genuine fault.

    `schemaEvolutionMode = addNewColumns` fails the stream on first sight of an
    unknown column, records the new schema, and expects a restart. Databricks
    also auto-retries this error class on its own -- which is how a new column
    currently produces a red task that silently heals, with nothing saying why.

    Distinguishing it lets the caller restart deliberately and say so, instead
    of leaving an unexplained failure in the run history.
    """
    if not message:
        return False
    upper = message.upper()
    return any(marker in upper for marker in SCHEMA_EVOLUTION_MARKERS)


def missing_columns_message(missing, bronze_entities: dict, env: str) -> str:
    """
    Build the failure message for a declared column that stopped arriving.

    Without this the failure surfaces from inside the entity split as
    `AnalysisException: cannot resolve segment`, several steps after the point
    where the cause was already known -- `detect_drift` computes `missing`
    before the loop runs.

    Names which entities declared each column, because that is what determines
    the blast radius and it is not obvious from the column name alone.
    """
    claims = []
    for column in missing:
        owners = sorted(
            name
            for name, cfg in bronze_entities.items()
            if column in (cfg.get("columns") or [])
        )
        claims.append(f"{column} (declared by: {', '.join(owners) or 'nothing'})")

    return (
        f"SCHEMA_DRIFT_FATAL env={env}: {len(missing)} column(s) declared in the "
        f"entity configs are no longer present at the source:\n  - "
        + "\n  - ".join(claims)
        + "\nThe entity split selects declared columns by name, so it cannot "
        "proceed. Either the source stopped sending these, or a rename was not "
        "mirrored in configs/superstore_bronze_config. Failing here rather than "
        "inside the split so the cause is named at the point it was detected."
    )


def is_pipeline_column(column: str) -> bool:
    """
    True if `column` was added by this pipeline rather than received from the
    source.

    Kept separate from the frozenset so the rescue-column suffix rule lives in
    exactly one place.
    """
    return column in PIPELINE_ADDED_COLUMNS or column.endswith(RESCUE_COLUMN_SUFFIX)


def source_columns(observed_columns) -> set:
    """
    Narrow the raw table's column list to the ones that actually came from the
    source feed.

    Args:
        observed_columns: every column on `superstore_raw`, e.g. from
            `spark.table(...).columns`.

    Returns:
        Set of source-originated column names.
    """
    return {c for c in observed_columns if not is_pipeline_column(c)}


def declared_columns(bronze_entities: dict) -> set:
    """
    Union of the `columns` lists across every entity config.

    A union rather than per-entity on purpose: a source column is "known to the
    model" if ANY entity claims it. Reporting it as drift for the three entities
    that legitimately ignore it would bury the signal in noise.

    Args:
        bronze_entities: the `bronze_entities` block of the Bronze config.

    Returns:
        Set of every column any entity declares.

    Raises:
        ValueError: if an entity config has no `columns` key -- an entity that
            declares nothing would silently widen the "known" set to nothing and
            make every column look new.
    """
    known = set()
    for name, cfg in bronze_entities.items():
        if "columns" not in cfg:
            raise ValueError(f"entity '{name}' has no 'columns' key")
        known.update(cfg["columns"])
    return known


def detect_drift(
    observed_columns,
    bronze_entities: dict,
    ignored_source_columns=None,
) -> dict:
    """
    Compare what arrived against what is declared.

    Pure: takes a column list and a config dict, returns a dict. Touches no
    Spark session and no table.

    A source column falls into exactly one of three states, and the third one
    is why this signature has an `ignored` parameter at all:

      declared -> carried into an entity table
      ignored  -> deliberately not carried, decision recorded in config
      DRIFT    -> nobody has decided

    Running the two-state version against the real config found `row_id`:
    present in every source file, declared by no entity, dropped on every run
    since the platform was built. Almost certainly intentional -- it is the CSV's
    row number, not business data -- but with no third state it would be
    reported as drift forever, and a monitor that is always red is a monitor
    people stop reading. The fix is not to widen the filter but to make the
    existing decision explicit, which is the whole point of the exercise.

    Args:
        observed_columns: every column on `superstore_raw`.
        bronze_entities: the `bronze_entities` config block.
        ignored_source_columns: columns knowingly not carried downstream.

    Returns:
        {"new": [...], "missing": [...], "ignored": [...]}, all sorted for
        stable output -- an unsorted set would produce a different row order
        each run and make the metrics table's history unreadable.
    """
    arrived = source_columns(observed_columns)
    declared = declared_columns(bronze_entities)
    ignored = set(ignored_source_columns or ())

    return {
        "new": sorted(arrived - declared - ignored),
        "missing": sorted(declared - arrived),
        "ignored": sorted(arrived & ignored),
    }


def drift_rows(
    drift: dict,
    master_run_id: str,
    env: str,
    detected_at: datetime = None,
) -> list:
    """
    Flatten a drift result into one row per drifted column.

    One row per column rather than one per run so the table can answer "when did
    this column first appear", which is the question that actually gets asked
    months later.

    Args:
        drift: output of `detect_drift`.
        master_run_id: run that observed it.
        env: dev / qa / prod / integration_test.
        detected_at: observation time; defaults to now.

    Returns:
        List of dicts, empty when there is no drift -- the common case, and the
        caller should still write nothing rather than a synthetic "no drift" row.
    """
    ts = detected_at or datetime.now()

    return [
        {
            "master_run_id": master_run_id,
            "env": env,
            "column_name": column,
            "drift_status": status,
            "detected_at": ts,
        }
        for status, columns in ((DRIFT_NEW, drift["new"]), (DRIFT_MISSING, drift["missing"]))
        for column in columns
    ]


def rescue_column_of(observed_columns):
    """
    Find Auto Loader's rescue column, or None if the reader was not configured
    with one.

    Returned rather than assumed, because the prefix is chosen by the reader --
    `col__rescued_data` in this workspace -- and a hardcoded name would make the
    type-drift signal silently unavailable if that ever changed.
    """
    for column in observed_columns:
        if column.endswith(RESCUE_COLUMN_SUFFIX):
            return column
    return None


def record_schema_drift(
    spark,
    logger,
    raw_table: str,
    drift_table: str,
    bronze_entities: dict,
    ignored_source_columns=None,
    master_run_id: str = None,
    layer_run_id: str = None,
    env: str = None,
    layer: str = "Bronze",
) -> dict:
    """
    Detect drift against the live raw table and record it.

    Impure half of this module, matching the `reconciliation_sql` /
    `log_reconciliation` split used elsewhere: the comparison is pure and
    unit-tested, this executes it.

    Deliberately does NOT change behaviour. An undeclared column is still
    dropped at the entity split; this only stops that happening in silence. A
    new column entering Silver must remain a human decision -- see
    docs/SCHEMA_DRIFT.md.

    Call it AFTER Bronze ingest and BEFORE the entity loop. That ordering is the
    point of the MISSING case: `select()` on an absent column raises an opaque
    AnalysisException, whereas this names the column and says why it matters
    while the run is still in a position to explain itself.

    Log severity is graded, because treating these alike is how alert channels
    get muted:
        MISSING  -> ERROR. The entity split is about to fail.
        RESCUED  -> ERROR. Values are being silently discarded.
        NEW      -> WARNING. Nothing is broken; the model is ignoring data.
        stable   -> INFO.

    Returns the drift dict with `rescued_rows` added, so a caller or a test can
    assert on it.
    """
    from superstore_logger import log_event

    observed = spark.table(raw_table).columns
    drift = detect_drift(observed, bronze_entities, ignored_source_columns)

    # Cumulative across the whole raw table, not scoped to this run. Deliberate:
    # the expected value is 0 forever, so any non-zero deserves attention
    # regardless of which run produced it, and scoping would let an old rescue
    # scroll out of view unexamined.
    rescue_column = rescue_column_of(observed)
    rescued_rows = 0
    if rescue_column:
        rescued_rows = (
            spark.table(raw_table)
            .where(f"`{rescue_column}` IS NOT NULL")
            .count()
        )
    drift["rescued_rows"] = rescued_rows

    if drift["missing"] or rescued_rows:
        severity = "ERROR"
    elif drift["new"]:
        severity = "WARNING"
    else:
        severity = "INFO"

    log_event(
        logger,
        severity,
        f"{drift_summary(drift)} | rescued_rows={rescued_rows}",
        schema_drift_new=drift["new"],
        schema_drift_missing=drift["missing"],
        schema_drift_ignored=drift["ignored"],
        rescued_rows=rescued_rows,
        master_run_id=master_run_id,
        layer_run_id=layer_run_id,
        layer=layer,
    )

    rows = drift_rows(drift, master_run_id, env)

    # The RESCUED row is written every run, unlike NEW/MISSING which are written
    # only when they occur. It is a measurement rather than an event, and a row
    # per run doubles as proof the check actually executed -- an empty table is
    # otherwise indistinguishable from a detector that never ran.
    if rescue_column:
        rows.append(
            {
                "master_run_id": master_run_id,
                "env": env,
                "column_name": rescue_column,
                "drift_status": DRIFT_RESCUED,
                "detected_at": datetime.now(),
            }
        )

    _write_drift_rows(spark, drift_table, rows, rescued_rows)
    return drift


def _write_drift_rows(spark, drift_table: str, rows: list, rescued_rows: int):
    """
    Append drift observations, creating the table on first write.

    Separated so `record_schema_drift` stays readable and so the schema is
    declared in exactly one place -- an inferred schema would flip
    `row_count` to a different type on the first run that has no rescue column.
    """
    if not rows:
        return

    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("master_run_id", StringType(), True),
            StructField("env", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("drift_status", StringType(), True),
            StructField("row_count", LongType(), True),
            StructField("detected_at", TimestampType(), True),
        ]
    )

    payload = [
        (
            r.get("master_run_id"),
            r.get("env"),
            r.get("column_name"),
            r.get("drift_status"),
            int(rescued_rows) if r.get("drift_status") == DRIFT_RESCUED else None,
            r.get("detected_at"),
        )
        for r in rows
    ]

    (
        spark.createDataFrame(payload, schema)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(drift_table)
    )


def drift_summary(drift: dict) -> str:
    """
    One-line human summary for the run log.

    Deliberately states the consequence rather than only the counts: "2 new"
    means nothing to someone reading a log at 3am, whereas naming the columns
    and saying they are being dropped does.
    """
    if not drift["new"] and not drift["missing"]:
        ignored = drift.get("ignored") or []
        suffix = f" ({len(ignored)} column(s) knowingly ignored)" if ignored else ""
        return f"SCHEMA_STABLE: no drift against declared entity columns{suffix}"

    parts = []
    if drift["new"]:
        parts.append(
            f"NEW at source and NOT in any entity config, so dropped at the "
            f"entity split: {', '.join(drift['new'])}"
        )
    if drift["missing"]:
        parts.append(
            f"MISSING from source but still declared, so the entity split will "
            f"fail: {', '.join(drift['missing'])}"
        )
    return "SCHEMA_DRIFT: " + " | ".join(parts)
