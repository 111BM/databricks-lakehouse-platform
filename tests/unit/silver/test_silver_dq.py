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
  - silver config YAML parses into the shapes these functions expect
==============================================================
"""

import re
from pathlib import Path

import pytest
import yaml
from pyspark.sql.functions import array_contains, col

SILVER_CONFIG_PATH = (
    Path(__file__).resolve().parents[3]
    / "configs" / "superstore_silver_config" / "superstore_silver_config.yaml"
)

# Add src directory to Python path for imports

from superstore_silver_transformations import (
    add_error_columns,
    add_is_valid,
    clean_string_columns,
    standardize_values,
)


# -----------------------------------------------------------------
# silver config parsing
# -----------------------------------------------------------------
@pytest.mark.unit
class TestSilverConfigParsing:
    """
    The DQ functions above receive regex_cols / categorical_allowed_vals
    straight from the silver config YAML. A mis-indented key inside a
    block scalar silently swallows a sibling rule (this happened to
    orders.regex_cols.ship_date), so validate the parsed shapes here.
    """

    @pytest.fixture(scope="class")
    def table_configs(self):
        with open(SILVER_CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return {t["bronze_table"]: t for t in cfg["silver_table_configs"]}

    def test_orders_has_regex_for_both_date_columns(self, table_configs):
        regex_cols = table_configs["orders"]["regex_cols"]
        assert set(regex_cols) == {"order_date", "ship_date"}

    def test_products_category_mapping_keys_are_strings_not_booleans(self, table_configs):
        """
        YAML 1.1 reads a bare OFF as the boolean False. Unquoted, this mapping
        loads, deploys and runs without error while never matching a single one
        of the 16,837 "OFF" rows it exists to translate.

        Pins the parsed type, because the failure is invisible at every other
        layer -- the config looks right and the pipeline reports success.
        """
        mapping = table_configs["products"]["value_standardization"]["category"]

        assert all(isinstance(k, str) for k in mapping), (
            f"non-string keys parsed from YAML: "
            f"{[(k, type(k).__name__) for k in mapping if not isinstance(k, str)]}"
        )
        assert set(mapping) == {"OFF", "TEC", "FUR"}

    def test_every_standardized_value_is_an_allowed_category(self, table_configs):
        # A mapping that translates into a value the rules still reject would
        # quarantine the row anyway -- silently, and for a different reason.
        products = table_configs["products"]
        mapping = products["value_standardization"]["category"]
        allowed = set(products["categorical_allowed_vals"]["category"])

        assert set(mapping.values()) <= allowed, (
            f"maps to values outside the allowed set: {set(mapping.values()) - allowed}"
        )

    def test_all_regex_patterns_compile(self, table_configs):
        for table, tcfg in table_configs.items():
            for column, pattern in (tcfg.get("regex_cols") or {}).items():
                re.compile(pattern)

    def test_no_regex_pattern_swallows_a_sibling_key(self, table_configs):
        # A key mis-indented into a block scalar shows up as "name: |"
        # inside the previous pattern string.
        for table, tcfg in table_configs.items():
            for column, pattern in (tcfg.get("regex_cols") or {}).items():
                assert not re.search(r"^\s*\w+:\s*\|\s*$", pattern, re.MULTILINE), (
                    f"{table}.regex_cols.{column} contains an embedded YAML key"
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

    def test_is_valid_is_never_null_so_the_split_is_exhaustive(self, spark):
        """
        THE INVARIANT bronze_to_silver_prod relies on: is_valid is strictly
        True/False, so grouping by it partitions the frame and read_rows can be
        derived as good_count + dirty_count instead of costing its own count()
        pass (serverless forbids cache()/persist(), so every extra action
        replays the whole clean -> DQ -> date-parse lineage).

        add_is_valid computes size(filter(error_columns, x -> x is not null)) == 0.
        error_columns is always a non-null array, so size() never returns null.
        A row that is neither valid nor invalid would silently under-report
        read_rows in the metrics table rather than fail.
        """
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("segment", StringType(), True),
        ])
        # Deliberately mixes clean rows, null keys, bad categoricals and an
        # all-null row -- the most likely source of a null is_valid.
        df = spark.createDataFrame([
            ("CG-12520", "Consumer"),
            (None, "Consumer"),
            ("AA-10480", "NotASegment"),
            (None, None),
        ], schema)

        validated = self._validate(
            df, ["customer_id", "segment"],
            categorical_allowed_vals={"segment": ["Consumer", "Corporate", "Home Office"]},
        )

        good = validated.filter(col("is_valid") == True).count()
        dirty = validated.filter(col("is_valid") == False).count()

        assert validated.filter(col("is_valid").isNull()).count() == 0
        assert good + dirty == 4  # == total rows in; no row falls through

    def test_all_clean_rows_still_yield_a_countable_split(self, spark):
        # Edge case for the derivation: when nothing is dirty, the groupBy returns
        # a single bucket and dirty_count must fall back to 0, not KeyError.
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField("customer_id", StringType(), True)])
        df = spark.createDataFrame([("A",), ("B",)], schema)

        validated = self._validate(df, ["customer_id"])
        buckets = {
            r["is_valid"]: r["count"]
            for r in validated.groupBy("is_valid").count().collect()
        }

        assert buckets.get(True, 0) == 2
        assert buckets.get(False, 0) == 0


@pytest.mark.unit
class TestValueStandardization:
    """
    standardize_values runs BEFORE the DQ rules, translating known source
    dialects into the canonical vocabulary.

    It exists because the products feed emits three-letter category codes
    ("OFF") alongside full labels ("Office Supplies"). Validating first treated
    the codes as invalid and quarantined 50,264 products, whose facts then
    contributed nothing to any mart. The codes are the same fact in a different
    dialect, so translating beats rejecting -- and beats substituting
    "Unknown", which would return the revenue while destroying a category that
    was never unknown.
    """

    def _products(self, spark, rows):
        return spark.createDataFrame(rows, ["product_id", "category"])

    MAPPING = {"category": {"OFF": "Office Supplies", "TEC": "Technology", "FUR": "Furniture"}}
    ALLOWED = {"category": ["Furniture", "Office Supplies", "Technology"]}

    def test_codes_become_canonical_labels(self, spark):
        df = self._products(spark, [("P1", "OFF"), ("P2", "TEC"), ("P3", "FUR")])

        out = standardize_values(df, self.MAPPING)

        assert sorted(r["category"] for r in out.collect()) == [
            "Furniture", "Office Supplies", "Technology",
        ]

    def test_standardized_rows_then_pass_validation(self, spark):
        # THE POINT. A row that was quarantined must now reach Silver, with its
        # real category rather than a placeholder.
        df = self._products(spark, [("P1", "OFF")])

        out = add_is_valid(add_error_columns(
            standardize_values(df, self.MAPPING),
            ["product_id", "category"], {}, self.ALLOWED,
        ))

        row = out.first()
        assert row["is_valid"] is True
        assert row["category"] == "Office Supplies"

    def test_already_canonical_values_are_untouched(self, spark):
        df = self._products(spark, [("P1", "Office Supplies"), ("P2", "Technology")])

        out = standardize_values(df, self.MAPPING)

        assert [r["category"] for r in out.collect()] == ["Office Supplies", "Technology"]

    def test_unmapped_values_are_left_to_fail_validation(self, spark):
        # Standardization must not become a catch-all that hides bad data. An
        # unrecognised value passes through untouched and is still quarantined.
        df = self._products(spark, [("P1", "Nordwest")])

        out = add_is_valid(add_error_columns(
            standardize_values(df, self.MAPPING),
            ["product_id", "category"], {}, self.ALLOWED,
        ))

        row = out.first()
        assert row["is_valid"] is False
        assert row["category"] == "Nordwest"  # not coerced to a nearest match

    def test_nulls_are_not_mapped(self, spark):
        # A null category is genuinely missing, not a dialect. It stays null and
        # stays invalid -- recovering it is the severity-tier work, not this.
        # Explicit schema: Spark cannot infer a type for an all-null column.
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
        ])
        df = spark.createDataFrame([("P1", None)], schema)

        out = standardize_values(df, self.MAPPING)

        assert out.first()["category"] is None

    def test_empty_or_missing_mapping_is_a_no_op(self, spark):
        df = self._products(spark, [("P1", "OFF")])

        for mapping in (None, {}, {"nonexistent_col": {"a": "b"}}):
            assert standardize_values(df, mapping).first()["category"] == "OFF"

    def test_mapping_is_exact_match_not_prefix_or_case_insensitive(self, spark):
        # "off" and "OFFICE" must not be swept up by the "OFF" rule.
        df = self._products(spark, [("P1", "off"), ("P2", "OFFICE")])

        out = standardize_values(df, self.MAPPING)

        assert [r["category"] for r in out.collect()] == ["off", "OFFICE"]
