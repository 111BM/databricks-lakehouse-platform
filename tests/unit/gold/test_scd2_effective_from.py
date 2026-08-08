"""
==============================================================
Unit Tests: Gold DIMENSION SCD2 effective_from
Target: superstore_gold_dimension_framework.prepare_scd2_columns
==============================================================

WHY THIS FILE EXISTS

prepare_scd2_columns sets effective_from, and merge_into_gold_table_scd2 closes
an old version with:

    effective_to = src.effective_from - INTERVAL 1 SECOND

So effective_from has one load-bearing property: it must ADVANCE between
versions of the same entity. If it is constant, every closed row ends one
second before its own start — a negative-length interval that no point-in-time
query can match. The row still looks closed (is_current=false, effective_to
populated), which is why the integration test passed for so long.

That is exactly what happened: the customers dimension used to derive
effective_from from the customer's FIRST ORDER DATE, which is the same value
for every version of that customer.

These tests guard the property directly, so the regression cannot come back
quietly. They are unit tests rather than integration checks because the failure
is in a pure column expression, not in the merge.
==============================================================
"""

from datetime import datetime, timedelta

import pytest

from superstore_gold_dimension_framework import prepare_scd2_columns

# prepare_scd2_columns takes logging args; values are irrelevant to the logic.
_LOG = dict(master_run_id="m1", layer_run_id="l1", layer="GOLD")

_ENTITY_COLS = ["customer_id", "segment"]


def _silver(spark, rows):
    """rows: list of (customer_id, segment, silver_ingestion_ts)."""
    return spark.createDataFrame(rows, ["customer_id", "segment", "silver_ingestion_ts"])


@pytest.mark.unit
@pytest.mark.gold
class TestEffectiveFrom:

    def test_effective_from_is_the_silver_ingestion_timestamp(self, spark):
        ts = datetime(2026, 5, 1, 9, 0, 0)
        df = _silver(spark, [("C1", "Consumer", ts)])

        out = prepare_scd2_columns(
            silver_df=df,
            dim_type="customers",
            entity_columns=_ENTITY_COLS,
            hash_column="row_hash",
            **_LOG,
        )

        assert out.first()["effective_from"] == ts

    def test_same_rule_for_every_dimension_type(self, spark):
        # The customers dimension used to be special-cased onto the first order
        # date. Every dim_type must now resolve identically.
        ts = datetime(2026, 5, 1, 9, 0, 0)
        rows = [("C1", "Consumer", ts)]

        got = {
            dim_type: prepare_scd2_columns(
                silver_df=_silver(spark, rows),
                dim_type=dim_type,
                entity_columns=_ENTITY_COLS,
                hash_column="row_hash",
                **_LOG,
            ).first()["effective_from"]
            for dim_type in ("customers", "products", "anything_else")
        }

        assert set(got.values()) == {ts}, got

    def test_effective_from_advances_between_versions(self, spark):
        # THE property the merge depends on. Two versions of one customer,
        # observed at different times, must carry different effective_from
        # values — otherwise closing the first produces an inverted interval.
        early = datetime(2026, 5, 1, 9, 0, 0)
        later = datetime(2026, 6, 1, 9, 0, 0)
        df = _silver(spark, [("C1", "Consumer", early), ("C1", "Corporate", later)])

        out = prepare_scd2_columns(
            silver_df=df,
            dim_type="customers",
            entity_columns=_ENTITY_COLS,
            hash_column="row_hash",
            **_LOG,
        )

        values = [r["effective_from"] for r in out.orderBy("effective_from").collect()]
        assert values == [early, later]
        assert values[0] != values[1], "effective_from must differ between versions"

    def test_closing_a_version_would_produce_a_valid_interval(self, spark):
        # Mirrors what merge_into_gold_table_scd2 does when it closes an old
        # version: effective_to = incoming effective_from - 1 second. With a
        # constant effective_from this went negative; assert it cannot.
        early = datetime(2026, 5, 1, 9, 0, 0)
        later = datetime(2026, 6, 1, 9, 0, 0)

        out = prepare_scd2_columns(
            silver_df=_silver(spark, [("C1", "Consumer", early), ("C1", "Corporate", later)]),
            dim_type="customers",
            entity_columns=_ENTITY_COLS,
            hash_column="row_hash",
            **_LOG,
        )
        old_from, new_from = [r["effective_from"] for r in out.orderBy("effective_from").collect()]

        closed_effective_to = new_from - timedelta(seconds=1)
        assert closed_effective_to > old_from, (
            f"closed interval is inverted: {old_from} -> {closed_effective_to}"
        )

    def test_hash_column_is_added(self, spark):
        # Guards the other half of the function: change detection depends on it.
        out = prepare_scd2_columns(
            silver_df=_silver(spark, [("C1", "Consumer", datetime(2026, 5, 1))]),
            dim_type="customers",
            entity_columns=_ENTITY_COLS,
            hash_column="row_hash",
            **_LOG,
        )

        assert "row_hash" in out.columns
        assert out.first()["row_hash"] is not None
