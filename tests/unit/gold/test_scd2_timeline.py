"""
==============================================================
Unit Tests: Gold DIMENSION SCD2 Timeline
Target: superstore_gold_dimension_framework.compute_scd2_timeline
==============================================================

compute_scd2_timeline is already a PURE function (DataFrame in -> DataFrame
out, only logging as a side effect), so we test it directly — no refactor
needed.

It builds the SCD2 validity timeline per business key:
  - orders versions by effective_from
  - effective_to = (next version's effective_from) - 1 microsecond
  - is_current = True for the newest version (no next row)

These invariants are the heart of your dimension history. Break them and
these tests go red.
==============================================================
"""

from datetime import datetime, timedelta

import pytest


from superstore_gold_dimension_framework import compute_scd2_timeline

# compute_scd2_timeline takes logging args; values are irrelevant to the logic.
_LOG = dict(master_run_id="m1", layer_run_id="l1", layer="GOLD")


def _df(spark, rows):
    # rows: list of (customer_id, hash, effective_from datetime)
    return spark.createDataFrame(rows, ["customer_id", "row_hash", "effective_from"])


@pytest.mark.unit
class TestScd2Timeline:

    def test_single_version_is_current(self, spark):
        df = _df(spark, [("C1", "h1", datetime(2026, 1, 1))])

        out = compute_scd2_timeline(df, "customer_id", "row_hash", **_LOG)

        row = out.first()
        assert row["is_current"] is True
        assert row["effective_to"] is None  # open-ended

    def test_two_versions_close_old_open_new(self, spark):
        df = _df(spark, [
            ("C1", "h1", datetime(2026, 1, 1)),   # old version
            ("C1", "h2", datetime(2026, 1, 5)),   # new version
        ])

        out = compute_scd2_timeline(df, "customer_id", "row_hash", **_LOG)
        by_hash = {r["row_hash"]: r for r in out.collect()}

        # Old version: closed, not current, effective_to = next - 1 microsecond
        assert by_hash["h1"]["is_current"] is False
        assert by_hash["h1"]["effective_to"] == datetime(2026, 1, 5) - timedelta(microseconds=1)

        # New version: open, current
        assert by_hash["h2"]["is_current"] is True
        assert by_hash["h2"]["effective_to"] is None

    def test_exactly_one_current_per_key(self, spark):
        df = _df(spark, [
            ("C1", "h1", datetime(2026, 1, 1)),
            ("C1", "h2", datetime(2026, 1, 5)),
            ("C1", "h3", datetime(2026, 1, 9)),
        ])

        out = compute_scd2_timeline(df, "customer_id", "row_hash", **_LOG)

        current = out.filter("is_current = true")
        assert current.count() == 1
        assert current.first()["row_hash"] == "h3"  # newest wins

    def test_multiple_keys_are_independent(self, spark):
        df = _df(spark, [
            ("C1", "a1", datetime(2026, 1, 1)),
            ("C1", "a2", datetime(2026, 1, 5)),
            ("C2", "b1", datetime(2026, 1, 3)),  # only one version
        ])

        out = compute_scd2_timeline(df, "customer_id", "row_hash", **_LOG)
        current = {r["customer_id"]: r["row_hash"] for r in out.filter("is_current = true").collect()}

        assert current["C1"] == "a2"
        assert current["C2"] == "b1"
        # each key has exactly one current row
        assert out.filter("is_current = true").count() == 2

