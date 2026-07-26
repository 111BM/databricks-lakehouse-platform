"""
==============================================================
Unit Tests: Bronze Auto Loader run-mode detection
Target: bronze_ingest_superstore_module_01.is_initial_run
==============================================================

`is_initial_run` decides `cloudFiles.includeExistingFiles` when no backfill
mode is requested:

  initial run (no committed checkpoint) -> True  -> ingest files already present
  incremental run (checkpoint has state) -> False -> only files arriving later

Getting this wrong is silent: a first run that reports "incremental" skips
every existing file and ingests 0 rows without failing. The empty-directory
case below is the one that bites in practice - a checkpoint path can exist
while holding no committed state (pre-created by another task, or a run that
crashed before its first commit).

Pure filesystem logic: no Spark, no cluster -> runs in milliseconds.
==============================================================
"""

import pytest

# Real production function (path wired in pytest.ini pythonpath)
from bronze_ingest_superstore_module_01 import is_initial_run


@pytest.mark.unit
class TestInitialRunDetection:
    """A checkpoint counts as 'used' only when it actually holds state."""

    def test_missing_checkpoint_is_initial_run(self, tmp_path):
        # brand-new environment: the path has never been created
        assert is_initial_run(str(tmp_path / "does_not_exist")) is True

    def test_empty_checkpoint_dir_is_initial_run(self, tmp_path):
        # path exists but holds no state -> still a first run.
        # Treating this as incremental is what silently ingests 0 rows.
        empty = tmp_path / "checkpoint"
        empty.mkdir()
        assert is_initial_run(str(empty)) is True

    def test_checkpoint_with_state_is_incremental_run(self, tmp_path):
        checkpoint = tmp_path / "checkpoint"
        checkpoint.mkdir()
        (checkpoint / "metadata").write_text("{}")
        assert is_initial_run(str(checkpoint)) is False

    def test_real_autoloader_checkpoint_layout_is_incremental_run(self, tmp_path):
        # what Structured Streaming actually writes once a batch commits
        checkpoint = tmp_path / "checkpoint"
        for sub in ("offsets", "commits", "sources"):
            (checkpoint / sub).mkdir(parents=True)
        (checkpoint / "metadata").write_text('{"id":"a-b-c"}')
        assert is_initial_run(str(checkpoint)) is False

    def test_file_instead_of_dir_is_initial_run(self, tmp_path):
        # not a directory -> no usable checkpoint state
        not_a_dir = tmp_path / "checkpoint"
        not_a_dir.write_text("")
        assert is_initial_run(str(not_a_dir)) is True
