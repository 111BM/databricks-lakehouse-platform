"""
==============================================================
Unit Tests: Reconciliation SQL
Target: superstore_reconciliation.reconciliation_sql
==============================================================

The platform claimed `bronze == silver + quarantine + audit`. That holds after a
full re-derivation and is only a BOUND under incremental loading: Silver holds
one row per key, Bronze one per arrival, and `classify_duplicates` only ranks
rows within the batch it is given. A key arriving again in a LATER run is merged
over the top and the superseded version lands in no bucket.

The invariant gains a derived fourth term rather than a fourth table, because
Bronze already retains every arrival:

    superseded = SUM(arrivals per key - 1) - audit_rows

These tests pin the shape of that derivation. The SQL is built by a pure
function so the arithmetic can be checked without a catalog.
==============================================================
"""

import pytest

from superstore_reconciliation import reconciliation_sql


def _sql(entity="customers", keys=None):
    return reconciliation_sql(
        "superstore_catalog", "dev_bronze", "dev_silver",
        "dev_quarantine", "dev_audit", entity, keys or ["customer_id"],
    )


@pytest.mark.unit
class TestReconciliationSql:

    def test_all_four_tables_are_fully_qualified(self):
        sql = _sql()
        for t in (
            "superstore_catalog.dev_bronze.customers",
            "superstore_catalog.dev_silver.customers",
            "superstore_catalog.dev_quarantine.customers_dirty",
            "superstore_catalog.dev_audit.customers_duplicates",
        ):
            assert t in sql, f"missing {t}"

    def test_superseded_subtracts_audit_from_extra_arrivals(self):
        # The whole correction in one line: extra arrivals already explained by
        # the audit table must not be counted twice.
        assert "extra_arrivals - audit_rows AS superseded_rows" in _sql()

    def test_extra_arrivals_counts_arrivals_beyond_the_first(self):
        # arrivals - 1 per key: the surviving version is in Silver, the rest are
        # either audited or superseded.
        sql = _sql()
        assert "SUM(arrivals - 1)" in sql
        assert "COUNT(*) AS arrivals" in sql

    def test_null_business_keys_are_excluded_from_the_per_key_count(self):
        # A null key cannot be "another arrival of" anything, and those rows are
        # already quarantined as fatal. Counting them would double-count.
        assert "WHERE customer_id IS NOT NULL" in _sql()

    def test_balance_check_includes_the_superseded_term(self):
        # A balance that omitted superseded would be the OLD invariant, which is
        # the bug this module exists to correct.
        sql = _sql()
        assert "silver_rows + quarantine_rows + audit_rows + (extra_arrivals - audit_rows)" in sql
        assert "AS balanced" in sql

    def test_composite_keys_are_grouped_together(self):
        # sales is keyed on (order_id, product_id); grouping on one alone would
        # invent repeat arrivals that do not exist.
        sql = _sql("sales", ["order_id", "product_id"])
        assert "GROUP BY order_id, product_id" in sql
        assert "order_id IS NOT NULL AND product_id IS NOT NULL" in sql

    def test_entity_is_labelled_in_the_output(self):
        assert "'customers' AS entity" in _sql()

    def test_empty_business_keys_raises(self):
        # Without a key there is no notion of a repeat arrival, so the term
        # would silently read zero -- a reassuring answer to a question the
        # query never asked.
        with pytest.raises(ValueError, match="silently read zero"):
            reconciliation_sql("c", "b", "s", "q", "a", "customers", [])


@pytest.mark.unit
class TestReconciliationArithmetic:
    """
    Execute the generated SQL against real frames so the arithmetic is checked,
    not merely the string. These reproduce, in a handful of rows, both states
    measured on dev: incremental (short) and re-derived (exact).
    """

    def _setup(self, spark, bronze, silver, quarantine, audit):
        for name, rows, cols in (
            ("bronze_customers", bronze, ["customer_id"]),
            ("silver_customers", silver, ["customer_id"]),
            ("quarantine_customers_dirty", quarantine, ["customer_id"]),
            ("audit_customers_duplicates", audit, ["customer_id"]),
        ):
            from pyspark.sql.types import StringType, StructField, StructType
            schema = StructType([StructField(c, StringType(), True) for c in cols])
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)

        return (
            reconciliation_sql("x", "y", "z", "p", "q", "customers", ["customer_id"])
            .replace("x.y.customers", "bronze_customers")
            .replace("x.z.customers", "silver_customers")
            .replace("x.p.customers_dirty", "quarantine_customers_dirty")
            .replace("x.q.customers_duplicates", "audit_customers_duplicates")
        )

    def test_incremental_state_is_short_and_superseded_absorbs_it(self, spark):
        # Key A arrived three times across separate runs: one row survives in
        # Silver, none were audited (they were never in the same batch), so two
        # are superseded. The old three-term sum would be 1 against bronze 3.
        sql = self._setup(
            spark,
            bronze=[("A",), ("A",), ("A",)],
            silver=[("A",)],
            quarantine=[],
            audit=[],
        )
        r = spark.sql(sql).first()

        assert r["bronze_rows"] == 3
        assert r["superseded_rows"] == 2
        assert r["accounted_rows"] == 3
        assert r["balanced"] is True

    def test_re_derived_state_puts_the_same_rows_in_audit_instead(self, spark):
        # After a replay the two extras are audited, so superseded falls to 0
        # and the original three-term invariant holds. Same bronze, same total.
        sql = self._setup(
            spark,
            bronze=[("A",), ("A",), ("A",)],
            silver=[("A",)],
            quarantine=[],
            audit=[("A",), ("A",)],
        )
        r = spark.sql(sql).first()

        assert r["superseded_rows"] == 0
        assert r["accounted_rows"] == 3
        assert r["balanced"] is True

    def test_null_key_rows_are_accounted_by_quarantine_not_superseded(self, spark):
        sql = self._setup(
            spark,
            bronze=[("A",), (None,), (None,)],
            silver=[("A",)],
            quarantine=[(None,), (None,)],
            audit=[],
        )
        r = spark.sql(sql).first()

        assert r["superseded_rows"] == 0
        assert r["quarantine_rows"] == 2
        assert r["balanced"] is True

    def test_a_genuinely_unexplained_row_fails_the_balance(self, spark):
        # The check must still be capable of failing: a bronze row belonging to
        # no bucket and to no repeat arrival is a real defect.
        sql = self._setup(
            spark,
            bronze=[("A",), ("B",)],
            silver=[("A",)],
            quarantine=[],
            audit=[],
        )
        r = spark.sql(sql).first()

        assert r["balanced"] is False, "B is unexplained; this must not balance"


@pytest.mark.unit
class TestAbsentTables:
    """
    Quarantine and audit tables are created on first write, so an entity that
    has never produced a dirty or duplicate row has none. Absent means zero, not
    an error — the integration test seeds no dirty products, and requiring the
    table failed the whole check for a reason unrelated to reconciliation.
    """

    def test_absent_quarantine_contributes_zero_and_is_not_referenced(self):
        sql = reconciliation_sql(
            "c", "b", "s", "q", "a", "products", ["product_id"],
            quarantine_exists=False,
        )
        assert "c.q.products_dirty" not in sql
        assert "0                                           AS quarantine_rows" in sql

    def test_absent_quarantine_leaves_arrivals_unadjusted(self):
        # With nothing quarantined there is nothing to subtract from arrivals.
        sql = reconciliation_sql(
            "c", "b", "s", "q", "a", "products", ["product_id"],
            quarantine_exists=False,
        )
        assert "b.arrivals AS arrivals" in sql
        assert "quarantined_per_key" not in sql

    def test_absent_audit_contributes_zero(self):
        sql = reconciliation_sql(
            "c", "b", "s", "q", "a", "products", ["product_id"],
            audit_exists=False,
        )
        assert "c.a.products_duplicates" not in sql

    def test_present_by_default(self):
        # The common case must still reference both tables.
        sql = reconciliation_sql("c", "b", "s", "q", "a", "customers", ["customer_id"])
        assert "c.q.customers_dirty" in sql
        assert "c.a.customers_duplicates" in sql
