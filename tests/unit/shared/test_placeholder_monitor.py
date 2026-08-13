"""
==============================================================
Unit Tests: Placeholder exposure monitor
Target: superstore_placeholder_monitor.placeholder_count_sql
==============================================================

Severity tiers replaced an absent dimension row with a present one carrying
'Unknown'. That is better only because it is visible, and visible only if
something measures it.

The orphaned-fact counters cannot: they now read 0 permanently, because tiers
made the condition they detect impossible. This monitor is their successor, and
these tests pin the shape of what it asks.

The SQL is built by a pure function precisely so it can be tested without a
catalog -- the same reason the Silver DQ rules were extracted.
==============================================================
"""

import pytest

from superstore_placeholder_monitor import PLACEHOLDER, placeholder_count_sql

ATTRS = ["customer_name", "segment", "region"]


@pytest.mark.unit
class TestPlaceholderCountSql:

    def _sql(self, **kw):
        return placeholder_count_sql(
            "superstore_catalog", "dev_gold", "dim_customers", ATTRS, **kw
        )

    def test_every_attribute_is_checked(self):
        # A column left out is a blind spot that reports zero forever.
        sql = self._sql()
        for column in ATTRS:
            assert f"{column} = 'Unknown'" in sql

    def test_attributes_are_combined_with_or_not_and(self):
        # A row is uninformative if ANY attribute is a placeholder. AND would
        # only count rows where everything is unknown -- a much smaller and far
        # more reassuring number.
        sql = self._sql()
        assert " OR " in sql
        assert " AND " not in sql

    def test_counts_rows_once_not_fields(self):
        # The question is how much of the dimension is uninformative, so a row
        # with three placeholders counts once.
        sql = self._sql()
        assert "THEN 1 ELSE 0" in sql
        assert sql.count("SUM(CASE WHEN") == 1

    def test_restricted_to_current_scd2_version_by_default(self):
        # Historic versions legitimately hold what was known at the time;
        # counting them would inflate the figure and hide a real trend.
        assert "WHERE is_current = true" in self._sql()

    def test_current_only_can_be_disabled(self):
        sql = self._sql(current_only=False)
        assert "is_current" not in sql

    def test_reports_the_denominator_too(self):
        # A bare count is unreadable: 77 of 87,522 and 77 of 100 are different
        # situations.
        sql = self._sql()
        assert "COUNT(*) AS current_rows" in sql
        assert "placeholder_rows" in sql

    def test_table_is_fully_qualified(self):
        assert "superstore_catalog.dev_gold.dim_customers" in self._sql()

    def test_placeholder_token_is_configurable(self):
        sql = self._sql(placeholder="N/A")
        assert "= 'N/A'" in sql
        assert "Unknown" not in sql

    def test_default_token_matches_what_gold_substitutes(self):
        # If these drift the monitor reports a confident zero while the
        # dimension fills with placeholders.
        from superstore_gold_dimension_framework import DIMENSION_PLACEHOLDER

        assert PLACEHOLDER == DIMENSION_PLACEHOLDER

    def test_empty_attribute_list_raises(self):
        # Failing loudly beats a monitor that always says everything is fine.
        with pytest.raises(ValueError, match="silently reports zero forever"):
            placeholder_count_sql("c", "s", "dim_customers", [])
