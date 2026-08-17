"""
==============================================================
Unit Tests: Schema drift detection
Target: superstore_schema_drift
==============================================================

The behaviour under test is a detector for a condition that has never occurred
on this platform -- the source is a static CSV, so no column has ever appeared
or vanished. That makes these tests the only thing standing between a working
detector and one that reports "no drift" forever because it is comparing the
wrong two sets.

The specific trap: `superstore_raw` carries six pipeline-added metadata columns
and Auto Loader's `col__rescued_data`. Counting any of those as source columns
would report permanent drift on every run, and the usual response to a monitor
that is always red is to stop reading it.
==============================================================
"""

from datetime import datetime

import pytest

from superstore_schema_drift import (
    DRIFT_MISSING,
    DRIFT_NEW,
    declared_columns,
    detect_drift,
    drift_rows,
    drift_summary,
    is_pipeline_column,
    source_columns,
)

# The real shape of superstore_raw, verified against prod_bronze on 2026-08-16:
# 21 source columns, one rescue column, six pipeline metadata columns.
RAW_COLUMNS = [
    "row_id", "order_id", "order_date", "ship_date", "ship_mode",
    "customer_id", "customer_name", "segment", "country", "city", "state",
    "postal_code", "region", "product_id", "category", "sub_category",
    "product_name", "sales", "quantity", "discount", "profit",
    "col__rescued_data",
    "bronze_ingestion_ts", "ingestion_date", "source_file_path",
    "source_file_name", "source_file_size_bytes", "source_file_modification_time",
]

ENTITIES = {
    "customers": {"columns": ["customer_id", "customer_name", "segment", "region"]},
    "products": {"columns": ["product_id", "category", "sub_category", "product_name"]},
    "orders": {"columns": ["order_id", "order_date", "ship_date", "customer_id"]},
    "sales": {"columns": ["order_id", "product_id", "sales", "quantity", "discount", "profit"]},
}


@pytest.mark.unit
class TestPipelineColumnExclusion:

    def test_metadata_columns_are_not_source_columns(self):
        # These are added after ingestion. Treating them as source data would
        # report drift on every run forever.
        for column in ("bronze_ingestion_ts", "ingestion_date", "source_file_name"):
            assert is_pipeline_column(column)

    def test_rescue_column_is_matched_on_suffix_not_exact_name(self):
        # Auto Loader names it, not us -- it is `col__rescued_data` here, but
        # the prefix depends on reader configuration.
        assert is_pipeline_column("col__rescued_data")
        assert is_pipeline_column("_rescued_data")

    def test_real_source_columns_are_not_excluded(self):
        for column in ("customer_id", "sales", "product_name"):
            assert not is_pipeline_column(column)

    def test_source_columns_strips_exactly_the_seven_non_source_columns(self):
        got = source_columns(RAW_COLUMNS)
        assert len(got) == 21
        assert "col__rescued_data" not in got
        assert "bronze_ingestion_ts" not in got
        assert "customer_id" in got


@pytest.mark.unit
class TestDeclaredColumns:

    def test_union_across_entities(self):
        got = declared_columns(ENTITIES)
        assert "customer_id" in got and "profit" in got

    def test_a_column_claimed_by_one_entity_counts_as_known(self):
        # A union, not an intersection. `sales` is declared only by the sales
        # entity; reporting it as drift for the other three would bury the
        # signal in noise.
        assert "sales" in declared_columns(ENTITIES)

    def test_entity_without_columns_key_raises(self):
        # Silently treating it as empty would shrink the "known" set and make
        # unrelated columns look new.
        with pytest.raises(ValueError, match="no 'columns' key"):
            declared_columns({"customers": {"table_name": "customers"}})


@pytest.mark.unit
class TestDetectDrift:

    def test_todays_real_schema_reports_no_drift(self):
        # The regression guard that matters most: against the actual prod
        # schema and a config declaring all 21 source columns, the answer must
        # be silence. A detector that cries wolf on a healthy pipeline gets
        # muted, and then the real event is invisible.
        entities = {"all": {"columns": sorted(source_columns(RAW_COLUMNS))}}
        drift = detect_drift(RAW_COLUMNS, entities)
        assert drift["new"] == [] and drift["missing"] == []

    def test_a_knowingly_ignored_column_is_not_drift(self):
        # row_id is the case that forced this third state to exist: present in
        # every source file, declared by no entity, dropped on every run since
        # the platform was built. Reporting it forever would train the reader
        # to ignore the table.
        entities = {"c": {"columns": ["customer_id"]}}
        drift = detect_drift(["row_id", "customer_id"], entities, ignored_source_columns=["row_id"])
        assert drift["new"] == []
        assert drift["ignored"] == ["row_id"]

    def test_ignoring_a_column_does_not_hide_a_genuinely_new_one(self):
        # The list must silence exactly what it names and nothing else.
        entities = {"c": {"columns": ["customer_id"]}}
        drift = detect_drift(
            ["row_id", "customer_id", "discount_reason"],
            entities,
            ignored_source_columns=["row_id"],
        )
        assert drift["new"] == ["discount_reason"]

    def test_ignoring_a_column_cannot_mask_it_going_MISSING(self):
        # A declared column that vanishes must still be fatal even if someone
        # also lists it as ignored -- otherwise the ignore list becomes a way to
        # switch off the loud, safe failure mode.
        entities = {"c": {"columns": ["customer_id", "segment"]}}
        drift = detect_drift(["customer_id"], entities, ignored_source_columns=["segment"])
        assert "segment" in drift["missing"]

    def test_the_real_config_and_real_prod_schema_agree(self):
        # Runs the detector against the SHIPPED config rather than a fixture.
        # This is what found row_id in the first place, and it fails if someone
        # adds an entity column that no longer arrives, or removes row_id from
        # the ignore list without declaring it.
        import pathlib

        import yaml

        cfg_path = (
            pathlib.Path(__file__).resolve().parents[3]
            / "configs" / "superstore_bronze_config" / "superstore_bronze_config.yaml"
        )
        cfg = yaml.safe_load(open(cfg_path))
        drift = detect_drift(
            RAW_COLUMNS,
            cfg["bronze_entities"],
            ignored_source_columns=cfg.get("ignored_source_columns", []),
        )
        assert drift["new"] == [], f"undeclared source columns: {drift['new']}"
        assert drift["missing"] == [], f"declared but absent: {drift['missing']}"

    def test_a_new_source_column_is_reported(self):
        drift = detect_drift(RAW_COLUMNS + ["discount_reason"], ENTITIES)
        assert "discount_reason" in drift["new"]

    def test_a_removed_source_column_is_reported_as_missing(self):
        # `segment` is declared by customers. Drop it from the source and the
        # entity split's select() will raise -- this is the pre-flight warning.
        observed = [c for c in RAW_COLUMNS if c != "segment"]
        assert "segment" in detect_drift(observed, ENTITIES)["missing"]

    def test_undeclared_source_columns_show_as_new(self):
        # ENTITIES declares 15 of the 21 source columns, so the six it ignores
        # are legitimately reported. This is the honest behaviour: the model IS
        # ignoring them.
        drift = detect_drift(RAW_COLUMNS, ENTITIES)
        assert "country" in drift["new"]
        assert drift["missing"] == []

    def test_output_is_sorted_for_stable_history(self):
        # An unsorted set would order rows differently each run and make the
        # metrics table's history unreadable.
        drift = detect_drift(RAW_COLUMNS + ["zzz_late", "aaa_early"], ENTITIES)
        assert drift["new"] == sorted(drift["new"])

    def test_metadata_columns_never_appear_as_drift(self):
        drift = detect_drift(RAW_COLUMNS, ENTITIES)
        for column in ("bronze_ingestion_ts", "col__rescued_data", "source_file_name"):
            assert column not in drift["new"]


@pytest.mark.unit
class TestDriftRows:

    def test_no_drift_produces_no_rows(self):
        # Not a synthetic "all clear" row -- absence of rows is the healthy
        # state and writing one per run would bloat the table with nothing.
        assert drift_rows({"new": [], "missing": []}, "run1", "prod") == []

    def test_one_row_per_column_not_per_run(self):
        # So the table can answer "when did this column first appear", which is
        # the question actually asked months later.
        rows = drift_rows({"new": ["a", "b"], "missing": ["c"]}, "run1", "prod")
        assert len(rows) == 3
        assert {r["column_name"] for r in rows} == {"a", "b", "c"}

    def test_statuses_are_tagged_correctly(self):
        rows = drift_rows({"new": ["a"], "missing": ["c"]}, "run1", "prod")
        by_col = {r["column_name"]: r["drift_status"] for r in rows}
        assert by_col["a"] == DRIFT_NEW
        assert by_col["c"] == DRIFT_MISSING

    def test_run_and_env_are_carried(self):
        row = drift_rows({"new": ["a"], "missing": []}, "run42", "qa")[0]
        assert row["master_run_id"] == "run42"
        assert row["env"] == "qa"

    def test_detected_at_is_injectable(self):
        ts = datetime(2026, 8, 16, 9, 32)
        row = drift_rows({"new": ["a"], "missing": []}, "r", "prod", detected_at=ts)[0]
        assert row["detected_at"] == ts


@pytest.mark.unit
class TestDriftSummary:

    def test_stable_schema_says_so_explicitly(self):
        assert "SCHEMA_STABLE" in drift_summary({"new": [], "missing": []})

    def test_names_the_columns_not_just_counts(self):
        # "2 new" means nothing to someone reading a log at 3am.
        s = drift_summary({"new": ["discount_reason"], "missing": []})
        assert "discount_reason" in s

    def test_states_the_consequence_of_each_kind(self):
        s = drift_summary({"new": ["a"], "missing": ["b"]})
        assert "dropped at the entity split" in s
        assert "will" in s and "fail" in s
