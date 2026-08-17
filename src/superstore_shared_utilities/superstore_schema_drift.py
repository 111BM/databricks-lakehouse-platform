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
