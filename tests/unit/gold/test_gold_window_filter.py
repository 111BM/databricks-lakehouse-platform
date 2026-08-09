"""
==============================================================
Unit Tests: Gold dimension window selection
Target: superstore_gold_dimension_framework.apply_window_filter
==============================================================

WHY THIS FILE EXISTS

Silver tables carry no ingestion_date column, so a windowed run (backfill or
replay) derives one before filtering. Which timestamp it derives from decides
whether the run selects anything at all.

It derived from silver_ingestion_ts -- when the row was WRITTEN to Silver --
while Silver itself windows on bronze_ingestion_ts, when the data was ingested.
A replay always rewrites Silver with a timestamp of now, so the rewritten rows
could never fall inside a historical window. Silver updated, Gold selected zero
rows, and the job reported success.

Measured on dev: window 2026-08-08 selected 0 of 51,511 Silver product rows on
the old clock, and 51,511 of 51,511 on the new one.

The selection is now a pure function so these cases can be tested without a
catalog. While it was welded inside the I/O path no test could reach it, which
is why a defect this total went unnoticed.
==============================================================
"""

from datetime import date, datetime

import pytest
from pyspark.sql.functions import col

from superstore_gold_dimension_framework import WINDOW_TS_COL, apply_window_filter

COLUMNS = ["product_id", "bronze_ingestion_ts", "silver_ingestion_ts"]


def _ts(day, hour=12):
    """A timestamp as a STRING, cast inside Spark.

    Deliberately not a Python datetime: createDataFrame converts naive
    datetimes from the DRIVER's timezone while to_date renders in the SESSION
    timezone (conftest pins UTC). On a developer machine in any other zone that
    shifts midnight rows into the previous day, so the same test passes in CI
    and fails locally. Strings cast in Spark have no driver timezone to cross.
    """
    return f"2026-08-{day:02d} {hour:02d}:00:00"


@pytest.mark.unit
class TestApplyWindowFilter:

    def _frame(self, spark, rows):
        return (
            spark.createDataFrame(rows, COLUMNS)
            .withColumn("bronze_ingestion_ts", col("bronze_ingestion_ts").cast("timestamp"))
            .withColumn("silver_ingestion_ts", col("silver_ingestion_ts").cast("timestamp"))
        )

    def test_replayed_row_is_selected_despite_a_later_silver_write(self, spark):
        """
        THE REGRESSION TEST.

        A replay re-derives Silver, stamping silver_ingestion_ts with now while
        bronze_ingestion_ts keeps the original ingestion date. Selecting on the
        Silver clock misses the row entirely; selecting on the Bronze clock
        finds it. This is the exact shape of the dev data that exposed the bug.
        """
        df = self._frame(spark, [("P1", _ts(8), _ts(9))])  # bronze 08-08, silver 08-09

        out = apply_window_filter(df, date(2026, 8, 8), date(2026, 8, 8))

        assert out.count() == 1, "a replayed row must still fall in its Bronze window"

    def test_selecting_on_the_silver_clock_would_miss_it(self, spark):
        # Pins WHY the default matters: same row, same window, other column.
        df = self._frame(spark, [("P1", _ts(8), _ts(9))])

        on_silver = apply_window_filter(
            df, date(2026, 8, 8), date(2026, 8, 8), window_ts_col="silver_ingestion_ts"
        )

        assert on_silver.count() == 0

    def test_default_clock_is_bronze_ingestion_ts(self, spark):
        assert WINDOW_TS_COL == "bronze_ingestion_ts"

    def test_rows_outside_the_window_are_excluded(self, spark):
        df = self._frame(spark, [
            ("P1", _ts(7), _ts(9)),   # before
            ("P2", _ts(8), _ts(9)),   # inside
            ("P3", _ts(9), _ts(9)),   # after
        ])

        out = apply_window_filter(df, date(2026, 8, 8), date(2026, 8, 8))

        assert [r["product_id"] for r in out.collect()] == ["P2"]

    def test_bounds_are_inclusive_at_both_ends(self, spark):
        # Exclusive bounds would silently strand a boundary day on every replay.
        df = self._frame(spark, [
            ("start", _ts(8), _ts(9)),
            ("middle", _ts(9), _ts(9)),
            ("end", _ts(10), _ts(9)),
        ])

        out = apply_window_filter(df, date(2026, 8, 8), date(2026, 8, 10))

        assert sorted(r["product_id"] for r in out.collect()) == ["end", "middle", "start"]

    def test_time_of_day_does_not_affect_day_membership(self, spark):
        # to_date() truncates: a row at 23:59 belongs to that day, not the next.
        df = self._frame(spark, [
            ("early", _ts(8, 0), _ts(9)),
            ("late", _ts(8, 23), _ts(9)),
        ])

        out = apply_window_filter(df, date(2026, 8, 8), date(2026, 8, 8))

        assert out.count() == 2

    def test_derived_ingestion_date_column_is_present(self, spark):
        # Retained deliberately; downstream selects by name so it is inert.
        df = self._frame(spark, [("P1", _ts(8), _ts(9))])

        out = apply_window_filter(df, date(2026, 8, 8), date(2026, 8, 8))

        assert "ingestion_date" in out.columns
        assert out.first()["ingestion_date"] == date(2026, 8, 8)

    def test_accepts_datetime_bounds_as_well_as_date(self, spark):
        # backfill_config carries datetimes, not dates.
        df = self._frame(spark, [("P1", _ts(8), _ts(9))])

        out = apply_window_filter(df, datetime(2026, 8, 8), datetime(2026, 8, 8))

        assert out.count() == 1

    def test_window_matching_nothing_returns_empty_not_everything(self, spark):
        # A window that selects nothing must select NOTHING -- failing open here
        # would silently re-derive the whole dimension.
        df = self._frame(spark, [("P1", _ts(8), _ts(9))])

        out = apply_window_filter(df, date(2026, 1, 1), date(2026, 1, 31))

        assert out.count() == 0
