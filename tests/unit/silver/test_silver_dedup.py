"""
==============================================================
Unit Tests: Silver Deduplication ("latest record wins")
Target: silver_transformations.deduplicate_latest_wins
==============================================================

This REPLACES the old fake test in tests/Unit_tests/silver/test_silver_quality.py,
which re-typed the Window/row_number logic *inside the test* and therefore
protected nothing.

This version imports the ACTUAL function that bronze_to_silver_prod now calls.
Break the real dedup and these go red.

Runs on Spark session from conftest.py (works on Databricks serverless & local).
==============================================================
"""

from datetime import datetime

import pytest

# Real production function (also called by superstore_silver_module.bronze_to_silver_prod)
from superstore_silver_transformations import deduplicate_latest_wins


def _ts(day):
    """Helper: a deterministic timestamp on the given day in Jan 2026."""
    return datetime(2026, 1, day, 12, 0, 0)


@pytest.mark.unit
class TestDeduplicateLatestWins:

    def test_latest_record_wins(self, spark):
        # Same customer twice; the newer bronze_ingestion_ts must win.
        df = spark.createDataFrame(
            [
                ("CG-12520", "Claire OLD", _ts(1)),
                ("CG-12520", "Claire NEW", _ts(2)),
            ],
            ["customer_id", "customer_name", "bronze_ingestion_ts"],
        )

        winners, losers = deduplicate_latest_wins(df, ["customer_id"])

        assert winners.count() == 1
        assert losers.count() == 1
        assert winners.first()["customer_name"] == "Claire NEW"
        assert losers.first()["customer_name"] == "Claire OLD"

    def test_no_duplicates_all_win(self, spark):
        # Three distinct keys -> three winners, zero losers.
        df = spark.createDataFrame(
            [
                ("A", "x", _ts(1)),
                ("B", "y", _ts(1)),
                ("C", "z", _ts(1)),
            ],
            ["customer_id", "val", "bronze_ingestion_ts"],
        )

        winners, losers = deduplicate_latest_wins(df, ["customer_id"])

        assert winners.count() == 3
        assert losers.count() == 0

    def test_business_key_preserved(self, spark):
        df = spark.createDataFrame(
            [
                ("CG-12520", "old", _ts(1)),
                ("CG-12520", "new", _ts(3)),
                ("CG-12520", "mid", _ts(2)),
            ],
            ["customer_id", "val", "bronze_ingestion_ts"],
        )

        winners, losers = deduplicate_latest_wins(df, ["customer_id"])

        assert winners.count() == 1
        assert winners.first()["customer_id"] == "CG-12520"
        assert winners.first()["val"] == "new"  # latest of the three
        assert losers.count() == 2

    def test_composite_business_key(self, spark):
        # sales grain is (order_id, product_id); dedup must respect both.
        df = spark.createDataFrame(
            [
                ("O1", "P1", "100", _ts(1)),
                ("O1", "P1", "200", _ts(2)),  # newer -> wins
                ("O1", "P2", "300", _ts(1)),  # different product -> separate group
            ],
            ["order_id", "product_id", "sales", "bronze_ingestion_ts"],
        )

        winners, losers = deduplicate_latest_wins(df, ["order_id", "product_id"])

        assert winners.count() == 2  # (O1,P1) and (O1,P2)
        assert losers.count() == 1
        o1p1 = winners.filter("order_id = 'O1' AND product_id = 'P1'").first()
        assert o1p1["sales"] == "200"

    def test_row_num_helper_column_is_dropped(self, spark):
        # The internal "row_num" must not leak into outputs.
        df = spark.createDataFrame(
            [("A", _ts(1)), ("A", _ts(2))],
            ["customer_id", "bronze_ingestion_ts"],
        )

        winners, losers = deduplicate_latest_wins(df, ["customer_id"])

        # Compute columns once to avoid multiple RPC calls
        winner_cols = winners.columns
        loser_cols = losers.columns
        
        assert "row_num" not in winner_cols
        assert "row_num" not in loser_cols

    def test_custom_order_column(self, spark):
        # The order column is configurable (e.g. silver_ingestion_ts).
        df = spark.createDataFrame(
            [
                ("A", "old", _ts(1)),
                ("A", "new", _ts(5)),
            ],
            ["customer_id", "val", "silver_ingestion_ts"],
        )

        winners, _ = deduplicate_latest_wins(
            df, ["customer_id"], order_col="silver_ingestion_ts"
        )

        assert winners.first()["val"] == "new"

    def test_empty_dataframe(self, spark):
        # Edge case: empty input should return empty winners and losers.
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("bronze_ingestion_ts", TimestampType(), True),
        ])
        df = spark.createDataFrame([], schema)

        winners, losers = deduplicate_latest_wins(df, ["customer_id"])

        assert winners.count() == 0
        assert losers.count() == 0
