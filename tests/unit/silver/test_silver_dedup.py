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

# Real production functions. bronze_to_silver_prod calls classify_duplicates()
# directly (so its metrics pass and both output branches share one window
# evaluation); deduplicate_latest_wins() is the split wrapper over it.
from superstore_silver_transformations import (
    classify_duplicates,
    deduplicate_latest_wins,
)


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


@pytest.mark.unit
class TestClassifyDuplicates:
    """
    classify_duplicates() is what bronze_to_silver_prod actually calls. It owns
    the dedup window, and the Silver metrics pass derives deduplicated_rows,
    duplicate_rows and the pre-dedup total from row_num in ONE aggregation
    rather than counting the two branches separately (serverless forbids
    cache()/persist(), so each extra count replays the whole lineage).

    These tests pin the row_num contract that derivation depends on.
    """

    def test_row_num_ranks_latest_first(self, spark):
        df = spark.createDataFrame(
            [
                ("A", "old", _ts(1)),
                ("A", "new", _ts(3)),
                ("A", "mid", _ts(2)),
            ],
            ["customer_id", "val", "bronze_ingestion_ts"],
        )

        by_val = {
            r["val"]: r["row_num"]
            for r in classify_duplicates(df, ["customer_id"]).collect()
        }

        assert by_val == {"new": 1, "mid": 2, "old": 3}

    def test_winner_is_row_num_one_per_group(self, spark):
        # Two groups -> exactly one row_num == 1 each.
        df = spark.createDataFrame(
            [
                ("A", _ts(1)),
                ("A", _ts(2)),
                ("B", _ts(1)),
            ],
            ["customer_id", "bronze_ingestion_ts"],
        )

        classified = classify_duplicates(df, ["customer_id"])

        assert classified.filter("row_num = 1").count() == 2  # one per key
        assert classified.filter("row_num > 1").count() == 1

    def test_split_is_exhaustive(self, spark):
        """
        THE INVARIANT the Silver metrics pass relies on: every row is either a
        winner (row_num == 1) or a duplicate (row_num > 1), never neither and
        never both. bronze_to_silver_prod derives its pre-dedup total as
        deduplicated_rows + duplicate_rows, so a gap here would silently corrupt
        the metrics table rather than fail loudly.
        """
        df = spark.createDataFrame(
            [
                ("A", _ts(1)), ("A", _ts(2)), ("A", _ts(3)),
                ("B", _ts(1)),
                ("C", _ts(1)), ("C", _ts(2)),
            ],
            ["customer_id", "bronze_ingestion_ts"],
        )

        classified = classify_duplicates(df, ["customer_id"])

        winners = classified.filter("row_num = 1").count()
        losers = classified.filter("row_num > 1").count()

        assert winners == 3           # A, B, C
        assert losers == 3            # 2 extra A + 1 extra C
        assert winners + losers == 6  # == total rows in
        assert classified.filter("row_num IS NULL").count() == 0

    def test_row_num_is_never_null(self, spark):
        # row_number() is 1-based and total, even for single-row groups.
        df = spark.createDataFrame(
            [("solo", _ts(1))], ["customer_id", "bronze_ingestion_ts"]
        )

        classified = classify_duplicates(df, ["customer_id"])

        assert classified.first()["row_num"] == 1

    def test_agrees_with_deduplicate_latest_wins(self, spark):
        # The wrapper must stay a pure split of the classified frame -- if these
        # ever diverge, the unit tests above stop protecting production.
        df = spark.createDataFrame(
            [
                ("A", "old", _ts(1)),
                ("A", "new", _ts(2)),
                ("B", "only", _ts(1)),
            ],
            ["customer_id", "val", "bronze_ingestion_ts"],
        )

        classified = classify_duplicates(df, ["customer_id"])
        winners, losers = deduplicate_latest_wins(df, ["customer_id"])

        assert winners.count() == classified.filter("row_num = 1").count()
        assert losers.count() == classified.filter("row_num > 1").count()
        assert sorted(r["val"] for r in winners.collect()) == ["new", "only"]

    def test_composite_key_row_num(self, spark):
        df = spark.createDataFrame(
            [
                ("O1", "P1", _ts(1)),
                ("O1", "P1", _ts(2)),
                ("O1", "P2", _ts(1)),
            ],
            ["order_id", "product_id", "bronze_ingestion_ts"],
        )

        classified = classify_duplicates(df, ["order_id", "product_id"])

        assert classified.filter("row_num = 1").count() == 2
        assert classified.filter("row_num > 1").count() == 1


@pytest.mark.unit
class TestDedupDeterminism:
    """
    Duplicates that arrive in the same batch share one bronze_ingestion_ts, so
    ordering by that column alone leaves the window tied and row_number()
    resolves it by shuffle order -- the same input could produce different
    Silver contents on a re-run.

    classify_duplicates appends a content hash as a secondary sort key. These
    tests pin the property that buys: the winner is a function of the row
    CONTENT, not of the order the rows happened to arrive in.

    Note what is deliberately NOT asserted: that the hash picks the
    *business-latest* row. It cannot -- no entity carries a change timestamp.
    The guarantee is reproducibility only.
    """

    def _winner(self, spark, rows):
        df = spark.createDataFrame(
            rows, ["customer_id", "customer_name", "bronze_ingestion_ts"]
        )
        winners, _ = deduplicate_latest_wins(df, ["customer_id"])
        return winners.first()["customer_name"]

    def test_tied_timestamp_winner_is_independent_of_input_order(self, spark):
        # THE REGRESSION TEST. Same two rows, same timestamp, opposite input
        # order -- previously the window tied and either could win.
        forward = [
            ("CG-12520", "Claire A", _ts(1)),
            ("CG-12520", "Claire B", _ts(1)),
        ]
        reversed_rows = list(reversed(forward))

        assert self._winner(spark, forward) == self._winner(spark, reversed_rows)

    def test_tied_timestamp_winner_is_stable_across_evaluations(self, spark):
        # Serverless forbids cache(), so the window is re-evaluated on every
        # action. Two evaluations of the same frame must agree.
        df = spark.createDataFrame(
            [
                ("A", "one", _ts(1)),
                ("A", "two", _ts(1)),
                ("A", "three", _ts(1)),
            ],
            ["customer_id", "val", "bronze_ingestion_ts"],
        )

        first = deduplicate_latest_wins(df, ["customer_id"])[0].first()["val"]
        second = deduplicate_latest_wins(df, ["customer_id"])[0].first()["val"]

        assert first == second

    def test_timestamp_still_outranks_the_tiebreak(self, spark):
        # The hash is a SECONDARY key: a newer row must win regardless of how
        # its content hashes. Run both name orderings so the assertion cannot
        # pass by luck of the hash.
        for older, newer in [("aaa", "zzz"), ("zzz", "aaa")]:
            df = spark.createDataFrame(
                [
                    ("A", older, _ts(1)),
                    ("A", newer, _ts(2)),
                ],
                ["customer_id", "customer_name", "bronze_ingestion_ts"],
            )

            winners, _ = deduplicate_latest_wins(df, ["customer_id"])

            assert winners.first()["customer_name"] == newer

    def test_identical_rows_still_produce_one_winner(self, spark):
        # Rows identical in every column tie even after the tiebreak. That is
        # harmless -- they are interchangeable -- but exactly one must survive,
        # or the reconciliation invariant breaks.
        df = spark.createDataFrame(
            [
                ("A", "same", _ts(1)),
                ("A", "same", _ts(1)),
            ],
            ["customer_id", "val", "bronze_ingestion_ts"],
        )

        winners, losers = deduplicate_latest_wins(df, ["customer_id"])

        assert winners.count() == 1
        assert losers.count() == 1

    def test_explicit_tiebreak_cols_are_honoured(self, spark):
        # bronze_to_silver_prod passes business_columns explicitly. Columns
        # outside that list must not influence the ordering.
        rows = [
            ("A", "content", "meta-x", _ts(1)),
            ("A", "content", "meta-y", _ts(1)),
        ]
        df = spark.createDataFrame(
            rows, ["customer_id", "val", "ignored_col", "bronze_ingestion_ts"]
        )

        classified = classify_duplicates(
            df, ["customer_id"], tiebreak_cols=["val"]
        )

        # "val" is identical in both rows, so restricting the tiebreak to it
        # leaves them tied -- and still exactly one winner.
        assert classified.filter("row_num = 1").count() == 1
        assert classified.filter("row_num > 1").count() == 1

    def test_no_tiebreak_columns_available_is_not_an_error(self, spark):
        # Degenerate frame: nothing to hash beyond the key and the timestamp.
        # The window falls back to order_col alone, which is fine because any
        # tied rows are by construction identical.
        df = spark.createDataFrame(
            [("A", _ts(1)), ("A", _ts(1))],
            ["customer_id", "bronze_ingestion_ts"],
        )

        winners, losers = deduplicate_latest_wins(df, ["customer_id"])

        assert winners.count() == 1
        assert losers.count() == 1
