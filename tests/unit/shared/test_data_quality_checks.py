"""
==============================================================
Unit Tests: Data quality check rows
Target: superstore_data_quality_checks.reconciliation_check_row
==============================================================

The reconciliation result becomes a row that a SQL Alert queries. These tests
pin that row's shape: an alert reading `passed = false` must mean exactly what
the reconciliation query meant by "not balanced", and the values must be typed
consistently so appends never fail on a schema mismatch.
==============================================================
"""

import json
from datetime import datetime, timezone

import pytest

from superstore_data_quality_checks import (
    CHECK_ORPHANED_FACTS,
    CHECK_PLACEHOLDER_EXPOSURE,
    CHECK_RECONCILIATION,
    orphaned_facts_check_row,
    placeholder_exposure_check_row,
    reconciliation_check_row,
)

AT = datetime(2026, 10, 3, tzinfo=timezone.utc)


def _result(bronze=10, silver=6, quarantine=1, audit=2, superseded=1, balanced=True):
    return {
        "bronze_rows": bronze, "silver_rows": silver, "quarantine_rows": quarantine,
        "audit_rows": audit, "superseded_rows": superseded,
        "accounted_rows": silver + quarantine + audit + superseded,
        "balanced": balanced,
    }


@pytest.mark.unit
def test_balanced_result_passes():
    row = reconciliation_check_row("prod", "run-1", "customers", _result(), AT)
    assert row["passed"] is True
    assert row["check_name"] == CHECK_RECONCILIATION
    assert row["subject"] == "customers"
    assert row["env"] == "prod" and row["master_run_id"] == "run-1"


@pytest.mark.unit
def test_unbalanced_result_fails_and_keeps_both_sides():
    # 10 in Bronze, 9 accounted for: one row no rule explains.
    row = reconciliation_check_row(
        "prod", "run-1", "orders", _result(superseded=0, balanced=False), AT)
    assert row["passed"] is False
    assert row["expected_value"] == 10.0
    assert row["observed_value"] == 9.0


@pytest.mark.unit
def test_passed_comes_from_the_query_not_a_recomputation():
    # The definition of "balanced" lives in reconciliation_sql only. Even if the
    # numbers looked equal, the query's verdict wins.
    row = reconciliation_check_row("prod", "r", "sales", _result(balanced=False), AT)
    assert row["passed"] is False


@pytest.mark.unit
def test_values_are_floats_so_appends_never_mismatch():
    row = reconciliation_check_row("prod", "r", "products", _result(), AT)
    assert isinstance(row["observed_value"], float)
    assert isinstance(row["expected_value"], float)


@pytest.mark.unit
def test_details_carry_every_term_for_the_reader():
    row = reconciliation_check_row("prod", "r", "customers", _result(), AT)
    details = json.loads(row["details"])
    assert set(details) == {
        "bronze_rows", "silver_rows", "quarantine_rows", "audit_rows",
        "superseded_rows", "accounted_rows",
    }
    assert all(isinstance(v, int) for v in details.values())


@pytest.mark.unit
def test_accepts_numeric_strings_from_spark_or_sql_api():
    # Values can arrive as strings (e.g. through the SQL statement API).
    result = {k: str(v) for k, v in _result().items() if k != "balanced"}
    result["balanced"] = True
    row = reconciliation_check_row("prod", "r", "customers", result, AT)
    assert row["expected_value"] == 10.0


# ---------------------------------------------------------------- orphaned facts

@pytest.mark.unit
def test_zero_orphans_pass():
    row = orphaned_facts_check_row("prod", "r", "facts_sales", "dim_products", 0, AT)
    assert row["passed"] is True
    assert row["check_name"] == CHECK_ORPHANED_FACTS
    assert row["subject"] == "facts_sales->dim_products"
    assert row["expected_value"] == 0.0


@pytest.mark.unit
def test_any_orphan_fails():
    # Structurally impossible since severity tiers, so even one means a new cause.
    row = orphaned_facts_check_row("prod", "r", "facts_orders", "dim_customers", 1, AT)
    assert row["passed"] is False
    assert row["observed_value"] == 1.0
    assert json.loads(row["details"])["orphaned_facts"] == 1


# ----------------------------------------------------------- placeholder exposure

def _exposure(current=200, placeholders=10):
    return {"current_rows": current, "placeholder_rows": placeholders,
            "placeholder_pct": round(100 * placeholders / current, 4)}


@pytest.mark.unit
def test_placeholder_exposure_is_informational_at_any_level():
    # Placeholders are normal; only growth across runs is a signal, and that is
    # the alert's comparison. A row never fails on its own.
    for placeholders in (0, 10, 150):
        row = placeholder_exposure_check_row("prod", "r", "dim_customers",
                                             _exposure(placeholders=placeholders), AT)
        assert row["passed"] is True
        assert row["expected_value"] is None
        assert row["check_name"] == CHECK_PLACEHOLDER_EXPOSURE


@pytest.mark.unit
def test_placeholder_exposure_records_the_percentage():
    row = placeholder_exposure_check_row("prod", "r", "dim_customers", _exposure(), AT)
    assert row["observed_value"] == 5.0
    assert json.loads(row["details"]) == {
        "current_rows": 200, "placeholder_rows": 10, "placeholder_pct": 5.0}
