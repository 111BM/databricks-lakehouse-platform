"""
==============================================================
Unit Tests: Bronze Column Sanitization
Target: bronze_ingest_superstore_module_01.sanitize_column
==============================================================

Migrated from tests/Unit_tests/bronze/test_bronze_ingest.py.

What changed vs the original:
  - No hard-coded /Workspace sys.path  -> imports resolve via pytest.ini,
    so this runs on a laptop and in GitHub CI (the original crashed at
    collection with `ModuleNotFoundError: No module named 'bronze'`).
  - Pure-logic only: no Spark, no fixtures needed -> runs in milliseconds.

This is a REAL unit test: it imports and calls the actual production
function. Break sanitize_column in the pipeline and this goes red.
==============================================================
"""

import pytest

# Real production function (path wired in pytest.ini pythonpath)
from bronze_ingest_superstore_module_01 import sanitize_column


@pytest.mark.unit
class TestColumnSanitization:
    """Sanitization rules: lowercase, spaces->underscores, strip non-alnum,
    prefix 'col_' when a name doesn't start with a letter."""

    def test_lowercase_conversion(self):
        assert sanitize_column("Customer ID") == "customer_id"
        assert sanitize_column("PRODUCT_NAME") == "product_name"

    def test_space_to_underscore(self):
        assert sanitize_column("Ship Mode") == "ship_mode"
        assert sanitize_column("Customer Name") == "customer_name"

    def test_hyphen_removed(self):
        # hyphen is non-alphanumeric -> stripped (not converted to underscore)
        assert sanitize_column("Sub-Category") == "subcategory"

    def test_special_chars_removed(self):
        assert sanitize_column("Amount ($)") == "amount_"
        assert sanitize_column("Price@#$%") == "price"

    def test_numeric_prefix_handling(self):
        # names not starting with a letter get a 'col_' prefix (Delta-safe)
        assert sanitize_column("123abc") == "col_123abc"
        assert sanitize_column("2024_sales") == "col_2024_sales"

    @pytest.mark.parametrize("raw,expected", [
        # Every actual Superstore source column
        ("Row ID", "row_id"),
        ("Order ID", "order_id"),
        ("Order Date", "order_date"),
        ("Ship Date", "ship_date"),
        ("Ship Mode", "ship_mode"),
        ("Customer ID", "customer_id"),
        ("Customer Name", "customer_name"),
        ("Segment", "segment"),
        ("Country", "country"),
        ("City", "city"),
        ("State", "state"),
        ("Postal Code", "postal_code"),
        ("Region", "region"),
        ("Product ID", "product_id"),
        ("Category", "category"),
        ("Sub-Category", "subcategory"),
        ("Product Name", "product_name"),
        ("Sales", "sales"),
        ("Quantity", "quantity"),
        ("Discount", "discount"),
        ("Profit", "profit"),
        # already-clean name is unchanged (idempotent)
        ("customer_id", "customer_id"),
    ])
    def test_all_superstore_columns(self, raw, expected):
        assert sanitize_column(raw) == expected

    def test_is_idempotent(self):
        # sanitizing an already-sanitized name must not change it
        once = sanitize_column("Sub-Category")
        assert sanitize_column(once) == once
