"""
==============================================================
Module: Source Acquisition State
==============================================================

Purpose
-------
Decide whether an empty source folder is a quiet week or a broken pipeline.

Why this exists
---------------
`bronze_source_acquisition` used to treat "the source contains no data files"
as a clean exit:

    dbutils.notebook.exit(f"NO_SOURCE_FILES env={env} at {listing_url}")

`dbutils.notebook.exit` succeeds. So the task went GREEN, the fourteen
downstream tasks ran against an empty landing volume, and the job reported
SUCCESS having produced nothing.

That is not hypothetical. Until 2026-08-16 `superstore_data_platform/prod`
contained nothing but a README, so the prod landing volume had never held a
single file and prod_bronze / prod_silver / prod_metrics were empty -- while
four prod runs in July 2026 all reported TERMINATED/SUCCESS. Three weeks of
green runs on a pipeline that had never processed a byte.

Resolved on 2026-08-16: a source file was added and prod produced real tables
for the first time. The guard stays because the condition it detects is rare,
not impossible -- a new environment, a mistyped source_listing_url, a recreated
volume, or a source folder emptied upstream all reach it again.

The distinction that matters
----------------------------
"No NEW files" and "no files AT ALL" are different events and the old code
conflated them.

  - Source has files, none of them new -> healthy. This is what an idempotent
    weekly pipeline looks like on a week the vendor delivered nothing. The
    existing set-difference already handles it: `new_files` is empty, the
    download loop does nothing, the task exits ACQUIRED 0.

  - Source has files, landing has files, none new -> same thing. Fine.

  - Source has NO data files, but landing does -> the vendor aged its files
    out. Nothing to acquire, but everything already landed is still there and
    downstream can legitimately re-derive from it. Warn, continue.

  - Source has NO data files AND landing is empty -> nothing can possibly be
    produced. Every downstream table will be empty and the run will still be
    green. This is the case that must fail.

Deliberately keyed on DATA, not on target
-----------------------------------------
The obvious implementation is "fail in prod, tolerate elsewhere". That was
rejected: an environment allowlist is exactly the shape that silently dropped
`value_standardization` and then `severity` from the Silver config, and it
would need editing every time an environment is added.

Keying on what is actually present needs no such list, and it tracks reality as
environments change rather than encoding a snapshot of them. Deliberately NOT
documented here as a per-environment mapping: an earlier version of this
docstring claimed "dev and qa have landed files so an empty source is tolerated
there", which was wrong about dev the day it was written (dev's source folder
has a CSV, so dev takes SOURCE_POPULATED) and went stale for prod within hours
of being committed. The rule is the durable thing; which branch a given
environment happens to take is not.

Pure so it can be unit tested without a workspace, a volume or a network call.
==============================================================
"""

# Source has data files. Proceed to the set difference against the landing zone.
SOURCE_POPULATED = "SOURCE_POPULATED"

# Source has no data files, but the landing zone does. Nothing to acquire; the
# run continues on data already held.
SOURCE_DRAINED = "SOURCE_DRAINED"

# Source has no data files and nothing has ever landed. The run cannot produce
# anything and must not report success.
NO_DATA_ANYWHERE = "NO_DATA_ANYWHERE"


def classify_source_state(source_data_file_count: int, landed_file_count: int) -> str:
    """
    Classify what an acquisition run is looking at.

    Pure: takes two counts, returns a constant. The caller decides what to do
    with the answer -- this function never exits, logs or raises on the
    pipeline's behalf.

    Args:
        source_data_file_count: data files (.csv/.csv.gz) visible at the source,
            AFTER filtering out READMEs and directories.
        landed_file_count: files already present in the landing volume.

    Returns:
        One of SOURCE_POPULATED, SOURCE_DRAINED, NO_DATA_ANYWHERE.

    Raises:
        ValueError: on a negative count, which means the caller miscounted
            rather than observed something real.
    """
    if source_data_file_count < 0 or landed_file_count < 0:
        raise ValueError(
            "counts cannot be negative "
            f"(source={source_data_file_count}, landed={landed_file_count})"
        )

    if source_data_file_count > 0:
        return SOURCE_POPULATED

    return SOURCE_DRAINED if landed_file_count > 0 else NO_DATA_ANYWHERE


def no_data_anywhere_message(env: str, listing_url: str, landing: str) -> str:
    """
    Build the failure message for NO_DATA_ANYWHERE.

    Kept here, and kept long, because the person reading it is looking at a red
    task with no other context. The old failure mode produced no message at all,
    so the useful thing this change adds is not the raising -- it is saying
    precisely which of the two locations is empty and what to put where.
    """
    return (
        f"NO_DATA_ANYWHERE env={env}: the source listing contains no .csv/.csv.gz "
        f"files AND the landing volume is empty, so this run cannot produce data "
        f"in any downstream table.\n"
        f"  source : {listing_url}\n"
        f"  landing: {landing}\n"
        f"Failing rather than exiting cleanly: a green run here would report "
        f"success while every Bronze, Silver and Gold table stayed empty, which "
        f"is what hid this condition for three weeks. Add a data file to the "
        f"source folder, or point source_listing_url at one that has data."
    )
