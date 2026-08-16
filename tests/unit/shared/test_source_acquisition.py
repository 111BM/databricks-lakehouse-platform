"""
==============================================================
Unit Tests: Source acquisition state
Target: superstore_source_acquisition.classify_source_state
==============================================================

The behaviour under test is the one that let prod report SUCCESS four times
while never processing a byte: an empty source folder exited cleanly, so a run
that could not possibly produce data still went green.

These tests pin the distinction the old code did not make -- "no NEW files" is
healthy, "no files ANYWHERE" is not.
==============================================================
"""

import pytest

from superstore_source_acquisition import (
    NO_DATA_ANYWHERE,
    SOURCE_DRAINED,
    SOURCE_POPULATED,
    classify_source_state,
    no_data_anywhere_message,
)


@pytest.mark.unit
class TestClassifySourceState:

    def test_source_with_files_proceeds_regardless_of_landing(self):
        # The set difference downstream decides what is new. Whether the landing
        # zone is empty or full is not this function's business once the source
        # has something to offer.
        assert classify_source_state(1, 0) == SOURCE_POPULATED
        assert classify_source_state(1, 99) == SOURCE_POPULATED

    def test_empty_source_with_landed_files_is_drained_not_fatal(self):
        # The vendor aged its files out. Everything already landed is still
        # there and downstream can legitimately re-derive from it, so failing
        # here would take a working pipeline offline over a non-event.
        assert classify_source_state(0, 3) == SOURCE_DRAINED

    def test_empty_source_and_empty_landing_is_fatal(self):
        # Nothing can possibly be produced. This is the exact prod condition:
        # only a README at the source, nothing ever landed.
        assert classify_source_state(0, 0) == NO_DATA_ANYWHERE

    def test_the_prod_condition_that_went_green_for_three_weeks(self):
        # Regression guard, named for what it actually was rather than for the
        # numbers. If this ever returns anything else, green runs on an empty
        # prod become possible again.
        assert classify_source_state(0, 0) != SOURCE_POPULATED
        assert classify_source_state(0, 0) != SOURCE_DRAINED

    def test_tolerance_scales_with_however_much_has_landed(self):
        # Any non-zero landing count tolerates an empty source, so no
        # environment needs special-casing -- which is the whole reason this is
        # keyed on data rather than on target.
        #
        # Deliberately not named after specific environments. The previous name
        # asserted a claim about dev that was wrong when written and would have
        # gone stale regardless: which branch an environment takes changes as
        # its data changes, and a test name is a bad place to record that.
        # Starts at 1 on purpose: the rule is "anything at all", not "enough to
        # be useful".
        for landed in (1, 2, 3, 50):
            assert classify_source_state(0, landed) == SOURCE_DRAINED

    @pytest.mark.parametrize("source,landed", [(-1, 0), (0, -1), (-1, -1)])
    def test_negative_counts_raise(self, source, landed):
        # A negative count means the caller miscounted rather than observed
        # something real, and silently classifying it would launder a bug into
        # a routing decision.
        with pytest.raises(ValueError, match="cannot be negative"):
            classify_source_state(source, landed)

    def test_every_branch_returns_a_known_constant(self):
        # Guards against a future edit returning a bare string the notebook's
        # equality checks would silently never match -- which would resurrect
        # the clean-exit behaviour without any test failing.
        known = {SOURCE_POPULATED, SOURCE_DRAINED, NO_DATA_ANYWHERE}
        for source in range(3):
            for landed in range(3):
                assert classify_source_state(source, landed) in known


@pytest.mark.unit
class TestNoDataAnywhereMessage:

    MSG = no_data_anywhere_message(
        "prod",
        "https://api.github.com/repos/111BM/Datasets/contents/superstore_data_platform/prod",
        "/Volumes/workspace/default/my_filestore_prod/raw/",
    )

    def test_names_both_locations(self):
        # The reader is looking at a red task with no other context, and the
        # whole point is telling them WHICH location is empty.
        assert "superstore_data_platform/prod" in self.MSG
        assert "my_filestore_prod/raw/" in self.MSG

    def test_names_the_environment(self):
        assert "env=prod" in self.MSG

    def test_says_what_to_do(self):
        # A failure that does not say how to clear it just moves the confusion.
        assert "Add a data file" in self.MSG

    def test_explains_why_it_fails_rather_than_exits(self):
        # The non-obvious part: exiting cleanly here was a deliberate choice
        # once, and without this sentence someone will helpfully restore it.
        assert "green run" in self.MSG
