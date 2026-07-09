"""
==============================================================
Unit Tests: Silver hash-id generation
Target: silver_transformations.row_hash
==============================================================

This REPLACES the old fake hash tests in
tests/Unit_tests/silver/test_silver_quality.py, which re-typed
`sha2(concat_ws(...))` inside the test and proved nothing.

row_hash() is now the single source of truth used by bronze_to_silver_prod
for all three Silver hashes (silver_/quarantine_/duplicates_). These tests
import and call that real function.

The hash is the backbone of idempotency: same business key -> same hash ->
MERGE updates instead of duplicating. So we assert determinism, null-safety,
and collision-resistance.
==============================================================
"""

import pytest
from pyspark.sql.functions import col

# Real production function (path wired in pytest.ini pythonpath)
from superstore_silver_transformations import row_hash


@pytest.mark.unit
class TestRowHash:

    def test_deterministic_same_input_same_hash(self, spark):
        df = spark.createDataFrame(
            [("CG-12520", "Claire"), ("CG-12520", "Claire")],
            ["customer_id", "customer_name"],
        )

        out = df.withColumn("h", row_hash(["customer_id", "customer_name"]))
        hashes = [r["h"] for r in out.collect()]

        assert hashes[0] == hashes[1]
        assert len(hashes[0]) == 64  # SHA-256 hex

    def test_different_keys_different_hash(self, spark):
        df = spark.createDataFrame(
            [("CG-12520",), ("DV-13045",), ("SO-20335",)],
            ["customer_id"],
        )

        out = df.withColumn("h", row_hash(["customer_id"]))
        distinct = out.select("h").distinct().count()

        assert distinct == 3  # no collisions

    def test_null_safe_never_null(self, spark):
        # A null in a hashed column must NOT produce a null hash.
        # Explicit schema: Spark can't infer type for an all-null column.
        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
        ])
        df = spark.createDataFrame([("CG-12520", None)], schema)

        out = df.withColumn("h", row_hash(["customer_id", "customer_name"]))

        assert out.first()["h"] is not None

    def test_separator_prevents_collision(self, spark):
        # ("a","bc") and ("ab","c") must hash differently thanks to the "||" sep
        df = spark.createDataFrame(
            [("a", "bc"), ("ab", "c")],
            ["k1", "k2"],
        )

        out = df.withColumn("h", row_hash(["k1", "k2"]))
        hashes = [r["h"] for r in out.collect()]

        assert hashes[0] != hashes[1]

    def test_column_order_matters(self, spark):
        # hashing [k1,k2] vs [k2,k1] should differ for asymmetric values
        df = spark.createDataFrame([("x", "y")], ["k1", "k2"])

        h_ab = df.withColumn("h", row_hash(["k1", "k2"])).first()["h"]
        h_ba = df.withColumn("h", row_hash(["k2", "k1"])).first()["h"]

        assert h_ab != h_ba
