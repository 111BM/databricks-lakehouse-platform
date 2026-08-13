"""
==============================================================
Unit Tests: Gold dimension attribute substitution
Target: superstore_gold_dimension_framework.substitute_untrusted_attributes
==============================================================

The second half of the severity-tier design. Silver decides which violations
are fatal and records the rest in repaired_columns WITHOUT altering the value,
so Silver stays diffable against Bronze. Gold fills the gap when it builds the
dimension, which is what makes completeness structural: a dimension row cannot
be written with a null attribute, because the step that writes it fills them.

Kimball's rule is the reason -- a dimension attribute is never NULL. Nulls
behave badly in group-bys, joins and BI tools, and push a COALESCE into every
consumer.

Null is not the only untrustworthy state, and an early version of this missed
that: a categorical violation like segment='Premium' is PRESENT and wrong, so a
coalesce leaves it alone and it reaches the dimension looking legitimate. Dev
could not surface it -- every invalid customer value there happened to be null --
and the qa integration test caught it. Substitution is therefore driven by
Silver's repaired_columns flags, not by null-ness.
==============================================================
"""

import pytest
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from superstore_gold_dimension_framework import (
    DIMENSION_PLACEHOLDER,
    substitute_untrusted_attributes,
)

ATTRIBUTES = ["customer_name", "segment", "region"]


@pytest.mark.unit
class TestSubstituteMissingAttributes:

    def _frame(self, spark, rows):
        schema = StructType(
            [StructField("customer_id", StringType(), True)]
            + [StructField(c, StringType(), True) for c in ATTRIBUTES]
        )
        return spark.createDataFrame(rows, schema)

    def test_null_attribute_becomes_the_placeholder(self, spark):
        df = self._frame(spark, [("CG-12520", "Claire", None, "South")])

        out = substitute_untrusted_attributes(df, ATTRIBUTES).first()

        assert out["segment"] == DIMENSION_PLACEHOLDER

    def test_placeholder_is_unknown(self, spark):
        assert DIMENSION_PLACEHOLDER == "Unknown"

    def test_present_values_are_untouched(self, spark):
        df = self._frame(spark, [("CG-12520", "Claire", "Consumer", "South")])

        out = substitute_untrusted_attributes(df, ATTRIBUTES).first()

        assert (out["customer_name"], out["segment"], out["region"]) == (
            "Claire", "Consumer", "South",
        )

    def test_every_listed_attribute_is_filled(self, spark):
        # The dimension must contain no nulls at all, not merely fewer.
        df = self._frame(spark, [("CG-12520", None, None, None)])

        out = substitute_untrusted_attributes(df, ATTRIBUTES).first()

        assert all(out[c] == DIMENSION_PLACEHOLDER for c in ATTRIBUTES)

    def test_business_key_is_left_alone_when_not_listed(self, spark):
        # Keys are the fatal tier and cannot be null here. Excluding them means
        # this can never manufacture a dimension member from a missing key.
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("segment", StringType(), True),
        ])
        df = spark.createDataFrame([(None, None)], schema)

        out = substitute_untrusted_attributes(df, ["segment"]).first()

        assert out["customer_id"] is None
        assert out["segment"] == DIMENSION_PLACEHOLDER

    def test_non_string_columns_are_skipped_not_coerced(self, spark):
        # A numeric attribute must not be turned into the string "Unknown".
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("order_count", IntegerType(), True),
        ])
        df = spark.createDataFrame([("CG-12520", None, None)], schema)

        out = substitute_untrusted_attributes(df, ["segment", "order_count"])

        row = out.first()
        assert row["segment"] == DIMENSION_PLACEHOLDER
        assert row["order_count"] is None
        assert dict(out.dtypes)["order_count"] == "int"

    def test_column_not_present_in_the_frame_is_ignored(self, spark):
        df = self._frame(spark, [("CG-12520", "Claire", "Consumer", "South")])

        out = substitute_untrusted_attributes(df, ATTRIBUTES + ["nonexistent"])

        assert out.count() == 1

    def test_row_count_is_never_changed(self, spark):
        # Substitution repairs values; it must never add or drop a row.
        df = self._frame(spark, [
            ("CG-1", "A", None, "South"),
            ("CG-2", None, "Consumer", None),
            ("CG-3", "C", "Corporate", "West"),
        ])

        assert substitute_untrusted_attributes(df, ATTRIBUTES).count() == 3


@pytest.mark.unit
class TestInvalidValuesAreAlsoSubstituted:
    """
    THE REGRESSION SUITE for the defect the qa integration test found.

    substitute_untrusted_attributes originally used coalesce, which only replaces
    NULL. A categorical violation such as segment='Premium' is present and wrong,
    so it flowed into dim_customers unchanged and read as a legitimate segment in
    every report -- arguably worse than the quarantine it replaced.

    Silver already names the failing column in repaired_columns. Substitution is
    driven by that, so both flavours of untrustworthy value are covered.
    """

    def _frame(self, spark, rows):
        from pyspark.sql.types import ArrayType
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("region", StringType(), True),
            StructField("repaired_columns", ArrayType(StringType()), True),
        ])
        return spark.createDataFrame(rows, schema)

    def test_invalid_but_present_value_is_substituted(self, spark):
        # coalesce would leave 'Premium' untouched; the flag must drive it.
        df = self._frame(spark, [("AA-10480", "Premium", "North", ["segment", "region"])])

        out = substitute_untrusted_attributes(df, ["segment", "region"]).first()

        assert out["segment"] == DIMENSION_PLACEHOLDER
        assert out["region"] == DIMENSION_PLACEHOLDER

    def test_valid_values_survive_even_when_the_row_has_other_repairs(self, spark):
        # Only the flagged columns are replaced -- a repair elsewhere must not
        # blank an attribute that was fine.
        df = self._frame(spark, [("CG-1", "Consumer", "North", ["region"])])

        out = substitute_untrusted_attributes(df, ["segment", "region"]).first()

        assert out["segment"] == "Consumer"
        assert out["region"] == DIMENSION_PLACEHOLDER

    def test_nulls_are_still_substituted(self, spark):
        # The original behaviour must not regress.
        df = self._frame(spark, [("CG-2", None, "South", [None])])

        out = substitute_untrusted_attributes(df, ["segment", "region"]).first()

        assert out["segment"] == DIMENSION_PLACEHOLDER
        assert out["region"] == "South"

    def test_frames_without_the_flag_column_fall_back_to_nulls_only(self, spark):
        # Entities with no severity map carry no repaired_columns; substitution
        # must still work rather than raising on a missing column.
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
        ])
        df = spark.createDataFrame([("P1", None)], schema)

        out = substitute_untrusted_attributes(df, ["category"]).first()

        assert out["category"] == DIMENSION_PLACEHOLDER
