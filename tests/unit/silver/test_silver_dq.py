"""
==============================================================
Unit Tests: Silver Data Quality routing
Targets:
  silver_transformations.clean_string_columns
  silver_transformations.add_error_columns
  silver_transformations.add_is_valid
==============================================================

This is the most important logic in the Silver layer: it decides which rows
are clean (-> Silver) and which are dirty (-> quarantine). It was previously
welded inside bronze_to_silver_prod and untested. It's now extracted into
pure functions that production calls, and tested here.

Rules covered:
  - column cleaning (trim + strip quotes)
  - null business keys      -> error
  - regex format mismatch   -> error
  - invalid categorical     -> error
  - is_valid = no errors
==============================================================
"""

import pytest
from pyspark.sql.functions import array_contains, col

# Add src directory to Python path for imports

from superstore_silver_transformations import (
    add_error_columns,
    add_is_valid,
    clean_string_columns,
)


# -----------------------------------------------------------------
# clean_string_columns
# -----------------------------------------------------------------
@pytest.mark.unit
class TestCleanStringColumns:

    def test_trims_whitespace(self, spark):
        df = spark.createDataFrame([("  CG-12520  ", "  Claire  ")],
                                   ["customer_id", "customer_name"])

        out = clean_string_columns(df, ["customer_id", "customer_name"]).first()

        assert out["customer_id"] == "CG-12520"
        assert out["customer_name"] == "Claire"

    def test_strips_double_quotes(self, spark):
        df = spark.createDataFrame([('"CG-12520"', '"Claire"')],
                                   ["customer_id", "customer_name"])

        out = clean_string_columns(df, ["customer_id", "customer_name"]).first()

        assert out["customer_id"] == "CG-12520"
        assert '"' not in out["customer_name"]

    def test_leaves_non_targeted_columns_untouched(self, spark):
        df = spark.createDataFrame([("  x  ", "  y  ")], ["a", "b"])

        # only clean column 'a'
        out = clean_string_columns(df, ["a"]).first()

        assert out["a"] == "x"
        assert out["b"] == "  y  "  # untouched


# -----------------------------------------------------------------
# add_error_columns + add_is_valid
# -----------------------------------------------------------------
@pytest.mark.unit
class TestDataQualityRouting:

    def _validate(self, df, business_columns, **kw):
        return add_is_valid(add_error_columns(df, business_columns, **kw))

    def test_clean_row_is_valid(self, spark):
        df = spark.createDataFrame([("CG-12520", "Consumer")],
                                   ["customer_id", "segment"])

        out = self._validate(df, ["customer_id", "segment"]).first()

        assert out["is_valid"] is True
        # error_columns has only nulls (no failures)
        assert all(e is None for e in out["error_columns"])

    def test_null_business_key_is_invalid(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("segment", StringType(), True),
        ])
        df = spark.createDataFrame([(None, "Consumer")], schema)

        out = self._validate(df, ["customer_id", "segment"]).first()

        assert out["is_valid"] is False
        assert "customer_id" in [e for e in out["error_columns"] if e is not None]

    def test_regex_failure_is_invalid(self, spark):
        df = spark.createDataFrame([("BADID", "Consumer")],
                                   ["customer_id", "segment"])

        out = self._validate(
            df, ["customer_id", "segment"],
            regex_cols={"customer_id": r"^[A-Z]{2}-\d+$"},  # expects e.g. CG-12520
        ).first()

        assert out["is_valid"] is False
        assert "customer_id" in [e for e in out["error_columns"] if e is not None]

    def test_regex_pass_is_valid(self, spark):
        df = spark.createDataFrame([("CG-12520", "Consumer")],
                                   ["customer_id", "segment"])

        out = self._validate(
            df, ["customer_id", "segment"],
            regex_cols={"customer_id": r"^[A-Z]{2}-\d+$"},
        ).first()

        assert out["is_valid"] is True

    def test_invalid_categorical_is_invalid(self, spark):
        df = spark.createDataFrame([("CG-12520", "NotASegment")],
                                   ["customer_id", "segment"])

        out = self._validate(
            df, ["customer_id", "segment"],
            categorical_allowed_vals={"segment": ["Consumer", "Corporate", "Home Office"]},
        ).first()

        assert out["is_valid"] is False
        assert "segment" in [e for e in out["error_columns"] if e is not None]

    def test_valid_categorical_passes(self, spark):
        df = spark.createDataFrame([("CG-12520", "Corporate")],
                                   ["customer_id", "segment"])

        out = self._validate(
            df, ["customer_id", "segment"],
            categorical_allowed_vals={"segment": ["Consumer", "Corporate", "Home Office"]},
        ).first()

        assert out["is_valid"] is True

    def test_multiple_failures_all_recorded(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("segment", StringType(), True),
        ])
        # null id AND invalid segment -> both errors
        df = spark.createDataFrame([(None, "NotASegment")], schema)

        out = self._validate(
            df, ["customer_id", "segment"],
            categorical_allowed_vals={"segment": ["Consumer", "Corporate", "Home Office"]},
        ).first()

        errors = [e for e in out["error_columns"] if e is not None]
        assert out["is_valid"] is False
        assert "customer_id" in errors
        assert "segment" in errors

    def test_good_and_bad_rows_split_correctly(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("segment", StringType(), True),
        ])
        df = spark.createDataFrame([
            ("CG-12520", "Consumer"),     # good
            (None, "Consumer"),           # bad: null id
            ("AA-10480", "NotASegment"),  # bad: segment
        ], schema)

        validated = self._validate(
            df, ["customer_id", "segment"],
            categorical_allowed_vals={"segment": ["Consumer", "Corporate", "Home Office"]},
        )

        assert validated.filter(col("is_valid") == True).count() == 1
        assert validated.filter(col("is_valid") == False).count() == 2
