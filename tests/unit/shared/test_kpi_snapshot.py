"""
==============================================================
Unit Tests: KPI run snapshot rows
Target: superstore_kpi_snapshot.snapshot_row, measures_sql
==============================================================

The snapshot row is what the dashboards compare run against run. These tests
pin its shape: values typed consistently so the MERGE never fails on a schema
mismatch, and an empty environment recorded as "no data" rather than zero.
==============================================================
"""

from datetime import date, datetime, timezone

import pytest

from superstore_kpi_snapshot import TABLE_SCHEMA, measures_sql, snapshot_row

AT = datetime(2026, 10, 11, 6, 35, tzinfo=timezone.utc)
STARTED = datetime(2026, 10, 11, 6, 0, tzinfo=timezone.utc)


def _measures(**overrides):
    m = {
        "run_started_at": STARTED, "raw_rows_ingested": 100_200_010,
        "data_from": date(2025, 1, 1), "data_through": date(2026, 9, 30),
        "revenue_30d": 2_240_000_000, "profit_30d": 161_360_000,
        "revenue_total": 35_330_000_000, "profit_total": 2_140_000_000,
        "orders_total": 48_400_000, "customers_total": 147_435,
    }
    m.update(overrides)
    return m


@pytest.mark.unit
def test_row_carries_identity_and_every_measure():
    row = snapshot_row("prod", "run-1", "PERIODIC", _measures(), AT)
    assert row["master_run_id"] == "run-1"
    assert row["env"] == "prod"
    assert row["snapshot_at"] == AT
    assert row["run_started_at"] == STARTED
    assert row["data_from"] == date(2025, 1, 1)
    assert row["data_through"] == date(2026, 9, 30)
    assert row["revenue_total"] == 35_330_000_000.0
    assert row["customers_total"] == 147_435


@pytest.mark.unit
def test_row_has_exactly_the_table_columns():
    # A key the schema does not know (or a missing one) fails createDataFrame
    # at run time, after the pipeline has done all its work.
    columns = [c.strip().split()[0] for c in TABLE_SCHEMA.split(",")]
    row = snapshot_row("dev", "run-1", "ONE_TIME", _measures(), AT)
    assert sorted(row) == sorted(columns)


@pytest.mark.unit
def test_measures_are_typed_for_the_schema():
    # Decimals from Spark and ints from Python must land as the declared types,
    # or the MERGE fails on a mismatch.
    from decimal import Decimal
    row = snapshot_row("dev", "r", "ONE_TIME",
                       _measures(revenue_30d=Decimal("10.50"), orders_total=Decimal("7")), AT)
    assert isinstance(row["revenue_30d"], float) and row["revenue_30d"] == 10.5
    assert isinstance(row["orders_total"], int) and row["orders_total"] == 7


@pytest.mark.unit
def test_missing_measures_stay_null_not_zero():
    # An empty environment has no revenue; recording 0 would make the next
    # real run look like growth from nothing.
    empty = {k: None for k in _measures()}
    row = snapshot_row("dev", "r", "ONE_TIME", empty, AT)
    assert row["revenue_total"] is None
    assert row["customers_total"] is None
    assert row["data_through"] is None


@pytest.mark.unit
@pytest.mark.parametrize("given, expected", [
    ("PERIODIC", "periodic"), ("one_time", "one_time"), ("", "unknown"), (None, "unknown"),
])
def test_trigger_type_is_normalised(given, expected):
    assert snapshot_row("dev", "r", given, _measures(), AT)["trigger_type"] == expected


@pytest.mark.unit
def test_snapshot_at_must_be_a_datetime():
    with pytest.raises(TypeError):
        snapshot_row("dev", "r", "PERIODIC", _measures(), "2026-10-11")


@pytest.mark.unit
def test_measures_sql_reads_this_environment_only():
    sql = measures_sql("cat", "prod_semantic_layer", "prod_metrics", "run-1")
    assert "cat.prod_semantic_layer.metrics_daily_kpi" in sql
    assert "cat.prod_semantic_layer.customer_kpi" in sql
    assert "cat.prod_metrics.bronze_layer_entity_metrics" in sql
    assert "master_run_id = 'run-1'" in sql
    # Same exclusion as the customer dashboard, so the numbers agree.
    assert "region <> 'Unknown'" in sql and "segment <> 'Unknown'" in sql


@pytest.mark.unit
def test_all_time_totals_use_the_customer_tiles_definition():
    # The customer dashboard's tiles sum customer_kpi without the 'Unknown'
    # placeholder rows. Totals taken from metrics_daily_kpi included them, so
    # the comparison table and the tiles disagreed on profit.
    sql = measures_sql("cat", "dev_semantic_layer", "dev_metrics", "run-1")
    customers_cte = sql.split("customers AS (")[1].split("),")[0]
    for measure in ("revenue_total", "profit_total", "orders_total", "customers_total"):
        assert measure in customers_cte
    assert "FROM cat.dev_semantic_layer.customer_kpi" in customers_cte
