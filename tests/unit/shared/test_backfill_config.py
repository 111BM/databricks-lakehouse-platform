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
get_backfill_config reads Databricks job widgets (backfill_mode, start_date,
end_date, dry_run) and turns them into a validated config dict. It is exactly
the kind of bug-prone, branch-heavy logic that deserves unit coverage:
  - default to safe "incremental" mode
  - reject invalid date formats
  - reject start_date after end_date
  - reject absurdly large ranges
  - block full_refresh unless explicitly allowed

We never touch a real Databricks workspace — `dbutils` is faked (see the
`make_dbutils` fixture in conftest.py). That fake is how FAANG teams unit
test code that depends on platform globals.
==============================================================
"""

from datetime import datetime

import pytest

# THE import that makes this a real test:
from superstore_backfill_utils import get_backfill_config


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
        dbutils = make_dbutils({"backfill_mode": "incremental"})

        config = get_backfill_config(dbutils)

        assert config["mode"] == "incremental"
        assert config["is_backfill"] is False

    def test_invalid_mode_falls_back_to_incremental(self, make_dbutils):
        # Garbage mode must NOT crash and must NOT silently backfill
        dbutils = make_dbutils({"backfill_mode": "delete_everything"})

        config = get_backfill_config(dbutils)

        assert config["mode"] == "incremental"
        assert config["is_backfill"] is False

    def test_dry_run_flag_is_parsed(self, make_dbutils):
        dbutils = make_dbutils({"backfill_mode": "incremental", "dry_run": "true"})

        config = get_backfill_config(dbutils)

        assert config["dry_run"] is True


@pytest.mark.unit
class TestDateRangeMode:
    """date_range must parse and validate the window correctly."""

    def test_valid_date_range(self, make_dbutils):
        dbutils = make_dbutils({
            "backfill_mode": "date_range",
            "start_date": "2026-01-01",
            "end_date": "2026-01-10",
        })

        config = get_backfill_config(dbutils)

        assert config["mode"] == "date_range"
        assert config["is_backfill"] is True
        assert config["start_date"] == datetime(2026, 1, 1)
        assert config["end_date"] == datetime(2026, 1, 10)

    def test_missing_start_date_raises(self, make_dbutils):
        dbutils = make_dbutils({"backfill_mode": "date_range"})

        with pytest.raises(ValueError, match="start_date is required"):
            get_backfill_config(dbutils)

    def test_bad_start_date_format_raises(self, make_dbutils):
        dbutils = make_dbutils({
            "backfill_mode": "date_range",
            "start_date": "01-01-2026",  # wrong format (DD-MM-YYYY)
        })

        with pytest.raises(ValueError, match="Invalid start_date format"):
            get_backfill_config(dbutils)

    def test_start_after_end_raises(self, make_dbutils):
        dbutils = make_dbutils({
            "backfill_mode": "date_range",
            "start_date": "2026-02-01",
            "end_date": "2026-01-01",
        })

        with pytest.raises(ValueError, match="cannot be after end_date"):
            get_backfill_config(dbutils)

    def test_range_too_large_raises(self, make_dbutils):
        dbutils = make_dbutils({
            "backfill_mode": "date_range",
            "start_date": "2020-01-01",
            "end_date": "2026-01-01",  # ~6 years, exceeds default max_days=365
        })

        with pytest.raises(ValueError, match="Date range too large"):
            get_backfill_config(dbutils)

    def test_range_too_large_can_be_overridden(self, make_dbutils):
        dbutils = make_dbutils({
            "backfill_mode": "date_range",
            "start_date": "2024-01-01",
            "end_date": "2026-01-01",
        })

        # Explicitly raising the cap should allow it
        config = get_backfill_config(dbutils, max_days=5000)

        assert config["mode"] == "date_range"
        assert config["is_backfill"] is True


@pytest.mark.unit
class TestFullRefreshSafety:
    """full_refresh is dangerous and must be explicitly enabled."""

    def test_full_refresh_blocked_by_default(self, make_dbutils):
        dbutils = make_dbutils({"backfill_mode": "full_refresh"})

        with pytest.raises(ValueError, match="full_refresh mode is disabled"):
            get_backfill_config(dbutils)

    def test_full_refresh_allowed_when_opted_in(self, make_dbutils):
        dbutils = make_dbutils({"backfill_mode": "full_refresh"})

        config = get_backfill_config(dbutils, allow_full_refresh=True)

        assert config["mode"] == "full_refresh"
        assert config["is_backfill"] is True
