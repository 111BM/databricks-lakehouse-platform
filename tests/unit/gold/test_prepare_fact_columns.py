"""
==============================================================
Unit Tests: Gold FACTS column preparation
Target: superstore_gold_facts_framework.prepare_fact_columns
==============================================================

prepare_fact_columns is pure (no table I/O — adds a hash + timestamp and
selects columns), so we test it directly.

What it must do:
  - add a deterministic SHA-256 hash over the entity columns
    (the fact idempotency key)
  - add a gold_ingestion_ts audit column
  - output exactly: entity_columns + [hash, *meta_columns, gold_ingestion_ts]

The hash test is important: it's the same `concat_ws("||", coalesce(...))`
formula your whole pipeline relies on for idempotent MERGEs.
==============================================================
"""

import pytest

# Real production function (path wired in pytest.ini pythonpath)
from superstore_gold_facts_framework import prepare_fact_columns

_LOG = dict(master_run_id="m1", layer_run_id="l1", layer="GOLD")
_HASH = "gold_facts_orders_hash_id"


def _fact_df(spark, rows):
    # rows: (order_id, sales, silver_ingestion_ts)
    return spark.createDataFrame(rows, ["order_id", "sales", "silver_ingestion_ts"])


@pytest.mark.unit
class TestPrepareFactColumns:

    def test_output_columns_exact(self, spark):
        df = _fact_df(spark, [("O1", "100.0", "2026-01-01")])

        out = prepare_fact_columns(
            df, master_run_id="m1",
            entity_columns=["order_id", "sales"],
            hash_column=_HASH,
            meta_columns=["silver_ingestion_ts"],
            layer_run_id="l1", layer="GOLD",
        )

        assert set(out.columns) == {
            "order_id", "sales", _HASH, "silver_ingestion_ts", "gold_ingestion_ts"
        }

    def test_hash_is_present_and_non_null(self, spark):
        df = _fact_df(spark, [("O1", "100.0", "2026-01-01")])

        out = prepare_fact_columns(
            df, "m1", ["order_id", "sales"], _HASH, ["silver_ingestion_ts"], "l1", "GOLD"
        )

        assert out.first()[_HASH] is not None
        assert len(out.first()[_HASH]) == 64  # SHA-256 hex length

    def test_identical_rows_get_identical_hash(self, spark):
        # Two rows with the same entity-column values -> same hash (deterministic)
        df = _fact_df(spark, [
            ("O1", "100.0", "2026-01-01"),
            ("O1", "100.0", "2026-02-02"),  # only meta differs -> hash must match
        ])

        out = prepare_fact_columns(
            df, "m1", ["order_id", "sales"], _HASH, ["silver_ingestion_ts"], "l1", "GOLD"
        )
        hashes = [r[_HASH] for r in out.collect()]

        assert hashes[0] == hashes[1]

    def test_different_rows_get_different_hash(self, spark):
        df = _fact_df(spark, [
            ("O1", "100.0", "2026-01-01"),
            ("O2", "999.0", "2026-01-01"),
        ])

        out = prepare_fact_columns(
            df, "m1", ["order_id", "sales"], _HASH, ["silver_ingestion_ts"], "l1", "GOLD"
        )
        hashes = [r[_HASH] for r in out.collect()]

        assert hashes[0] != hashes[1]
