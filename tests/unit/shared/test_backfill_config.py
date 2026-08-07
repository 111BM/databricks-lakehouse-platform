"""
==============================================================
Unit Tests: Backfill Config Parsing & Validation
Target: superstore_backfill_utils.get_backfill_config
==============================================================

WHY THIS FILE IS A "REAL" UNIT TEST
-----------------------------------
It IMPORTS the actual production function and CALLS it:

    from superstore_backfill_utils import get_backfill_config

If someone breaks the date validation in the pipeline, these tests go RED.
That is the whole point of a unit test — it guards your code, not Spark's.

Compare to the old silver tests, which re-typed the logic inside the test
and therefore protected nothing.

WHAT WE'RE TESTING
------------------
get_backfill_config reads Databricks job widgets (run_mode, start_date,
end_date, dry_run) and turns them into a validated config dict. It is exactly
the kind of bug-prone, branch-heavy logic that deserves unit coverage:
  - default to safe "incremental" mode when nothing is set
  - RAISE on an unrecognised mode rather than silently degrading to incremental
  - reject invalid date formats
  - reject start_date after end_date
  - reject absurdly large ranges
  - block full_refresh unless explicitly allowed
  - report which modes read the source, so Bronze knows when to skip

We never touch a real Databricks workspace — `dbutils` is faked (see the
`make_dbutils` fixture in conftest.py). That fake is how FAANG teams unit
test code that depends on platform globals.
==============================================================
"""

from datetime import datetime

import pytest

# THE import that makes this a real test:
from superstore_backfill_utils import (
    get_backfill_config,
    is_dry_run,
    reads_from_source,
)


@pytest.mark.unit
class TestDefaultAndIncrementalMode:
    """Safe defaults: missing/empty widgets must NOT trigger a backfill."""

    def test_no_widgets_defaults_to_incremental(self, make_dbutils):
        # No widgets set at all (fresh scheduled run)
        dbutils = make_dbutils({})

        config = get_backfill_config(dbutils)

        assert config["mode"] == "incremental"
        assert config["is_backfill"] is False
        assert config["start_date"] is None
        assert config["end_date"] is None
        assert config["dry_run"] is False

    def test_explicit_incremental(self, make_dbutils):
        dbutils = make_dbutils({"run_mode": "incremental"})

        config = get_backfill_config(dbutils)

        assert config["mode"] == "incremental"
        assert config["is_backfill"] is False
        assert config["reads_source"] is True
        assert config["is_windowed"] is False

    def test_invalid_mode_raises(self, make_dbutils):
        # A typo must fail the run. Falling back to incremental means the job
        # performs a different operation from the one the operator asked for,
        # and still reports success.
        dbutils = make_dbutils({"run_mode": "delete_everything"})

        with pytest.raises(ValueError, match="Unknown run_mode"):
            get_backfill_config(dbutils)

    def test_empty_mode_is_treated_as_unset(self, make_dbutils):
        # The bundle declares empty defaults, so "" reaches the widget on a
        # normal scheduled run. Absent is not the same as invalid.
        dbutils = make_dbutils({"run_mode": ""})

        config = get_backfill_config(dbutils)

        assert config["mode"] == "incremental"

    def test_dry_run_flag_is_parsed(self, make_dbutils):
        dbutils = make_dbutils({"run_mode": "incremental", "dry_run": "true"})

        config = get_backfill_config(dbutils)

        assert config["dry_run"] is True


@pytest.mark.unit
class TestWindowedModes:
    """backfill and replay must parse and validate the window correctly."""

    def test_valid_backfill_window(self, make_dbutils):
        dbutils = make_dbutils({
            "run_mode": "backfill",
            "start_date": "2026-01-01",
            "end_date": "2026-01-10",
        })

        config = get_backfill_config(dbutils)

        assert config["mode"] == "backfill"
        assert config["is_backfill"] is True
        assert config["is_windowed"] is True
        assert config["start_date"] == datetime(2026, 1, 1)
        assert config["end_date"] == datetime(2026, 1, 10)

    def test_missing_start_date_raises(self, make_dbutils):
        dbutils = make_dbutils({"run_mode": "backfill"})

        with pytest.raises(ValueError, match="start_date is required"):
            get_backfill_config(dbutils)

    def test_replay_missing_start_date_raises(self, make_dbutils):
        # Omitting the window must never mean "everything" — that is what
        # full_refresh is for, and it is gated.
        dbutils = make_dbutils({"run_mode": "replay"})

        with pytest.raises(ValueError, match="start_date is required"):
            get_backfill_config(dbutils)

    def test_bad_start_date_format_raises(self, make_dbutils):
        dbutils = make_dbutils({
            "run_mode": "backfill",
            "start_date": "01-01-2026",  # wrong format (DD-MM-YYYY)
        })

        with pytest.raises(ValueError, match="Invalid start_date format"):
            get_backfill_config(dbutils)

    def test_start_after_end_raises(self, make_dbutils):
        dbutils = make_dbutils({
            "run_mode": "backfill",
            "start_date": "2026-02-01",
            "end_date": "2026-01-01",
        })

        with pytest.raises(ValueError, match="cannot be after end_date"):
            get_backfill_config(dbutils)

    def test_range_too_large_raises(self, make_dbutils):
        dbutils = make_dbutils({
            "run_mode": "backfill",
            "start_date": "2020-01-01",
            "end_date": "2026-01-01",  # ~6 years, exceeds default max_days=365
        })

        with pytest.raises(ValueError, match="Date range too large"):
            get_backfill_config(dbutils)

    def test_range_too_large_can_be_overridden(self, make_dbutils):
        dbutils = make_dbutils({
            "run_mode": "backfill",
            "start_date": "2024-01-01",
            "end_date": "2026-01-01",
        })

        # Explicitly raising the cap should allow it
        config = get_backfill_config(dbutils, max_days=5000)

        assert config["mode"] == "backfill"
        assert config["is_backfill"] is True


@pytest.mark.unit
class TestFullRefreshSafety:
    """full_refresh is dangerous and must be explicitly enabled."""

    def test_full_refresh_blocked_by_default(self, make_dbutils):
        dbutils = make_dbutils({"run_mode": "full_refresh"})

        with pytest.raises(ValueError, match="full_refresh mode is disabled"):
            get_backfill_config(dbutils)

    def test_full_refresh_allowed_when_opted_in(self, make_dbutils):
        dbutils = make_dbutils({"run_mode": "full_refresh"})

        config = get_backfill_config(dbutils, allow_full_refresh=True)

        assert config["mode"] == "full_refresh"
        assert config["is_backfill"] is True
        # No window: full_refresh is deliberately unbounded.
        assert config["is_windowed"] is False


@pytest.mark.unit
class TestSourceAcquisition:
    """Which modes contact the source decides whether Bronze runs at all."""

    def test_incremental_and_backfill_read_source(self, make_dbutils):
        for mode, widgets in [
            ("incremental", {"run_mode": "incremental"}),
            ("backfill", {"run_mode": "backfill", "start_date": "2026-01-01"}),
        ]:
            config = get_backfill_config(make_dbutils(widgets))
            assert config["reads_source"] is True, mode
            assert reads_from_source(config) is True, mode

    def test_replay_does_not_read_source(self, make_dbutils):
        # A replay re-derives from the Bronze already held. Re-acquiring is
        # wasted work, and impossible once the source ages its files out.
        dbutils = make_dbutils({"run_mode": "replay", "start_date": "2026-01-01"})

        config = get_backfill_config(dbutils)

        assert config["mode"] == "replay"
        assert config["is_windowed"] is True
        assert config["reads_source"] is False
        assert reads_from_source(config) is False

    def test_full_refresh_does_not_read_source(self, make_dbutils):
        dbutils = make_dbutils({"run_mode": "full_refresh"})

        config = get_backfill_config(dbutils, allow_full_refresh=True)

        assert config["reads_source"] is False


@pytest.mark.unit
class TestDryRunIsOrthogonal:
    """dry_run modifies any mode; it is not a mode of its own."""

    def test_dry_run_combines_with_every_mode(self, make_dbutils):
        cases = [
            {"run_mode": "incremental", "dry_run": "true"},
            {"run_mode": "backfill", "start_date": "2026-01-01", "dry_run": "true"},
            {"run_mode": "replay", "start_date": "2026-01-01", "dry_run": "true"},
        ]
        for widgets in cases:
            config = get_backfill_config(make_dbutils(widgets))
            assert config["dry_run"] is True, widgets

    def test_is_dry_run_reads_the_flag_alone(self, make_dbutils):
        # The serving layer uses this: it must never raise on the full_refresh
        # gate, which is enforced upstream.
        assert is_dry_run(make_dbutils({"dry_run": "true"})) is True
        assert is_dry_run(make_dbutils({"dry_run": "false"})) is False
        assert is_dry_run(make_dbutils({})) is False
        assert is_dry_run(make_dbutils({"run_mode": "full_refresh", "dry_run": "true"})) is True
