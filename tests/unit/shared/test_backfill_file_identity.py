"""
==============================================================
Unit Tests: Backfill file identity and scope classification
Target: superstore_backfill_utils
==============================================================

Measured in dev on 2026-08-20: a backfill over 2026-07-27 re-ingested a file
that had landed weeks earlier, taking Superstore_12-02-2026.csv from 505 rows
to 1,010 -- an exact duplicate of the file.

The defect was structural. `backfill` uses a per-window Auto Loader checkpoint
so it cannot disturb the incremental one, which also means every in-window file
looks new to it. The Bronze write is a plain append with no deduplication.

It stayed invisible because bronze_ingestion_ts is re-stamped on re-read, so the
duplicates are not identical rows: Silver's dedup keeps one, the loser lands in
the audit table, and the reconciliation invariant still BALANCES. Nothing
downstream looks wrong.

These tests pin the two decisions that stop it recurring.
==============================================================
"""

import pytest

from superstore_backfill_utils import (
    BACKFILL_HAS_WORK,
    BACKFILL_NOTHING_MISSING,
    BACKFILL_WINDOW_MATCHED_NOTHING,
    backfill_exclusion_predicate,
    classify_backfill_scope,
    file_identity,
)


@pytest.mark.unit
class TestFileIdentity:

    def test_same_name_and_time_is_the_same_file(self):
        a = file_identity("Superstore_12-02-2026.csv", "2026-07-27 09:19:14")
        b = file_identity("Superstore_12-02-2026.csv", "2026-07-27 09:19:14")
        assert a == b

    def test_same_name_different_time_is_a_different_file(self):
        # A vendor re-export under the same filename is a REAL event. Skipping
        # it on name alone would drop genuine data -- the opposite failure to
        # the one being fixed, and a worse one.
        a = file_identity("Superstore.csv", "2026-07-27 09:19:14")
        b = file_identity("Superstore.csv", "2026-08-01 11:00:00")
        assert a != b

    def test_different_name_same_time_is_a_different_file(self):
        a = file_identity("a.csv", "2026-07-27 09:19:14")
        b = file_identity("b.csv", "2026-07-27 09:19:14")
        assert a != b


@pytest.mark.unit
class TestExclusionPredicate:

    ROWS = [
        ("Superstore_12-02-2026.csv", "2026-07-27 09:19:14"),
        ("Superstore_06-02-2025.csv", "2026-08-08 09:03:41"),
    ]

    def test_nothing_ingested_yields_no_predicate(self):
        # The first backfill against an empty Bronze table must not filter at
        # all. None makes that explicit at the call site rather than implied by
        # a predicate that happens to match everything.
        assert backfill_exclusion_predicate([]) is None

    def test_predicate_excludes_every_known_file(self):
        p = backfill_exclusion_predicate(self.ROWS)
        assert "Superstore_12-02-2026.csv|2026-07-27 09:19:14" in p
        assert "Superstore_06-02-2025.csv|2026-08-08 09:03:41" in p

    def test_predicate_is_an_exclusion_not_an_inclusion(self):
        # Inverting this would ingest ONLY files already present -- duplicating
        # everything and fetching nothing, a strictly worse version of the
        # defect being fixed.
        assert "NOT IN" in backfill_exclusion_predicate(self.ROWS)

    def test_matches_on_both_halves_of_the_identity(self):
        p = backfill_exclusion_predicate(self.ROWS)
        assert "source_file_name" in p
        assert "source_file_modification_time" in p

    def test_output_is_deterministic(self):
        # An unordered set would produce a different predicate string each run,
        # making the logged plan unreadable across runs.
        assert backfill_exclusion_predicate(self.ROWS) == backfill_exclusion_predicate(
            list(reversed(self.ROWS))
        )

    def test_duplicate_input_rows_collapse(self):
        p = backfill_exclusion_predicate(self.ROWS + self.ROWS)
        assert p.count("Superstore_12-02-2026.csv") == 1

    def test_single_quotes_in_a_filename_are_escaped(self):
        # A filename with an apostrophe would otherwise terminate the string
        # literal and produce a syntactically broken predicate.
        p = backfill_exclusion_predicate([("o'brien.csv", "2026-01-01 00:00:00")])
        assert "o''brien.csv" in p


@pytest.mark.unit
class TestScopeClassification:

    def test_window_with_new_files_has_work(self):
        assert classify_backfill_scope(3, 1) == BACKFILL_HAS_WORK

    def test_window_where_everything_is_present_is_nothing_missing(self):
        # Healthy. The gap this backfill was asked to fill is already filled --
        # which is exactly what the 2026-08-20 dev run should have reported
        # instead of silently duplicating 505 rows.
        assert classify_backfill_scope(2, 2) == BACKFILL_NOTHING_MISSING

    def test_empty_window_is_suspicious_not_healthy(self):
        # Asking for a range in which nothing was ever delivered is far more
        # often a typo than a fact, and must not look identical to "nothing to
        # do" -- the same distinction as SOURCE_DRAINED vs NO_DATA_ANYWHERE.
        assert classify_backfill_scope(0, 0) == BACKFILL_WINDOW_MATCHED_NOTHING

    def test_the_dev_defect_condition_classifies_as_nothing_missing(self):
        # One file in the window, already ingested. Regression guard named for
        # what it was: the exact condition that duplicated the file.
        assert classify_backfill_scope(1, 1) == BACKFILL_NOTHING_MISSING

    @pytest.mark.parametrize("window,ingested", [(-1, 0), (0, -1)])
    def test_negative_counts_raise(self, window, ingested):
        with pytest.raises(ValueError, match="cannot be negative"):
            classify_backfill_scope(window, ingested)

    def test_more_ingested_than_in_window_raises(self):
        # Cannot happen from real observation, so it means the caller counted
        # two different populations. Classifying it silently would launder a bug
        # into an operational decision.
        with pytest.raises(ValueError, match="different populations"):
            classify_backfill_scope(2, 5)

    def test_every_branch_returns_a_known_constant(self):
        known = {
            BACKFILL_HAS_WORK,
            BACKFILL_NOTHING_MISSING,
            BACKFILL_WINDOW_MATCHED_NOTHING,
        }
        for window in range(4):
            for ingested in range(window + 1):
                assert classify_backfill_scope(window, ingested) in known
