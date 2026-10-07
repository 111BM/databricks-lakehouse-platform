"""
==============================================================
Unit Tests: Liquid Clustering convergence decisions
Target: superstore_liquid_clustering
==============================================================

The pipeline converges every table's layout on every run, so the decision runs
far more often than any change does. Two mistakes matter:

  - planning a change for a table already correct, which would re-issue an
    ALTER on every run (or, for a partitioned table, re-convert it);
  - missing a still-partitioned table, which would leave the small-file
    problem in place while the config claims clustering.

The SQL itself is verified against real tables in dev; see
docs/LIQUID_CLUSTERING_MIGRATION.md.
==============================================================
"""

import pytest

from superstore_liquid_clustering import (
    MAX_CLUSTERING_KEYS,
    PLAN_ALTER_CLUSTER_BY,
    PLAN_CONVERT_PARTITIONED,
    PLAN_NOTHING,
    missing_cluster_columns,
    plan_clustering,
    validate_cluster_columns,
)


class TestPlanClustering:
    def test_already_clustered_on_declared_keys_does_nothing(self):
        assert plan_clustering([], ["bronze_ingestion_ts", "customer_id"],
                               ["bronze_ingestion_ts", "customer_id"]) == PLAN_NOTHING

    def test_unclustered_unpartitioned_table_gets_keys(self):
        # A table the pipeline has just created, or a Gold fact table whose
        # keys were declared and never applied.
        assert plan_clustering([], [], ["customer_id"]) == PLAN_ALTER_CLUSTER_BY

    def test_changed_keys_are_reapplied(self):
        assert plan_clustering([], ["ingestion_date"], ["bronze_ingestion_ts"]) == PLAN_ALTER_CLUSTER_BY

    def test_key_order_is_part_of_the_layout(self):
        assert plan_clustering([], ["state", "region"], ["region", "state"]) == PLAN_ALTER_CLUSTER_BY

    @pytest.mark.parametrize("partitions", [["bronze_ingestion_ts"], ["ingestion_date"], ["region"]])
    def test_partitioned_table_is_converted(self, partitions):
        assert plan_clustering(partitions, [], ["region", "state"]) == PLAN_CONVERT_PARTITIONED

    def test_partitioned_wins_even_if_keys_look_right(self):
        # Partitioning must go first: ALTER TABLE CLUSTER BY is refused on a
        # partitioned table.
        assert plan_clustering(["region"], ["region"], ["region"]) == PLAN_CONVERT_PARTITIONED

    def test_none_from_describe_detail_counts_as_empty(self):
        assert plan_clustering(None, None, ["customer_id"]) == PLAN_ALTER_CLUSTER_BY


class TestValidateClusterColumns:
    def test_valid_keys_returned_as_list(self):
        assert validate_cluster_columns(("a", "b")) == ["a", "b"]

    def test_empty_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            validate_cluster_columns([])

    def test_missing_config_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            validate_cluster_columns(None)

    def test_more_than_four_rejected(self):
        keys = [f"c{i}" for i in range(MAX_CLUSTERING_KEYS + 1)]
        with pytest.raises(ValueError, match="at most 4"):
            validate_cluster_columns(keys)

    def test_four_accepted(self):
        keys = [f"c{i}" for i in range(MAX_CLUSTERING_KEYS)]
        assert validate_cluster_columns(keys) == keys

    def test_duplicates_rejected(self):
        with pytest.raises(ValueError, match="duplicate"):
            validate_cluster_columns(["order_id", "order_id"])


class TestMissingClusterColumns:
    def test_all_present(self):
        assert missing_cluster_columns(["order_id", "product_id"],
                                       ["order_id", "product_id", "sales"]) == []

    def test_reports_missing_in_declared_order(self):
        # The real case: facts_sales declared customer_id, which it does not have.
        assert missing_cluster_columns(["customer_id", "product_id", "region"],
                                       ["order_id", "product_id"]) == ["customer_id", "region"]
