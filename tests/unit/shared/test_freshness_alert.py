"""
==============================================================
Unit Tests: Freshness alert definition
Target: resources/superstore_freshness_alert.alert.yml
==============================================================

The alert was deployed, scheduled, evaluated -- and structurally incapable of
firing for the condition it exists to detect.

An aggregate with no GROUP BY returns exactly one row whatever the WHERE clause
matches. With no successful run that row holds NULL, and `NULL > 216` is NULL,
which is not TRUE. So a pipeline that had never once succeeded reported OK.

`empty_result_state: TRIGGERED` did not save it: that covers zero ROWS, and this
query cannot produce zero rows. The documentation claimed it covered the case
anyway, which is why nobody looked.

These tests pin the two properties that make the guard work. They are cheap and
they read the shipped YAML rather than a copy, because the defect was in the
deployed definition and a test over a paraphrase would have passed.
==============================================================
"""

import re
from pathlib import Path

import pytest
import yaml

ALERT_PATH = (
    Path(__file__).resolve().parents[3]
    / "resources" / "superstore_freshness_alert.alert.yml"
)


@pytest.fixture(scope="module")
def alert():
    with open(ALERT_PATH) as f:
        cfg = yaml.safe_load(f)
    return cfg["resources"]["alerts"]["superstore_freshness"]


@pytest.fixture(scope="module")
def query(alert):
    return alert["query_text"]


@pytest.mark.unit
class TestNullGuard:

    def test_query_coalesces_the_aggregate(self, query):
        # Without this the "never succeeded" case returns NULL and reports OK.
        assert "COALESCE" in query.upper()

    def test_sentinel_exceeds_the_threshold(self, alert, query):
        # The pair is the invariant, not either half. Raising the threshold past
        # the sentinel would silently restore the original defect while leaving
        # a COALESCE in place that looks like it is doing something.
        threshold = alert["evaluation"]["threshold"]["value"]["double_value"]
        sentinels = [float(n) for n in re.findall(r"\b(\d{5,})\b", query)]

        assert sentinels, "no sentinel literal found in the COALESCE"
        assert max(sentinels) > threshold, (
            f"sentinel {max(sentinels)} does not exceed threshold {threshold}; "
            "a run that never succeeded would report OK"
        )

    def test_comparison_is_greater_than(self, alert):
        # The sentinel only works upward. Flipping this operator inverts the
        # whole design without touching the query.
        assert alert["evaluation"]["comparison_operator"] == "GREATER_THAN"


@pytest.mark.unit
class TestWhatCountsAsAlive:

    def test_only_successful_runs_refresh_the_clock(self, query):
        # A job can report SUCCESS while every entity inside it fails; that
        # happened here before the exception handling was fixed. Dropping this
        # filter would let such a run look like proof of life.
        assert "run_status = 'SUCCESS'" in query

    def test_measured_from_silver_metrics_not_the_jobs_api(self, query):
        assert "silver_layer_metrics" in query

    def test_schema_is_parameterised_not_pinned_to_prod(self, query):
        # Hardcoding prod_metrics would make the dev and qa copies monitor
        # PRODUCTION, so a healthy prod silences all three while dev sits broken.
        assert "${var.schema}_metrics" in query
        assert "prod_metrics" not in query


@pytest.mark.unit
class TestNotification:

    def test_empty_result_is_treated_as_unhealthy(self, alert):
        # Unreachable for this query shape, kept for a future one that groups.
        assert alert["evaluation"]["empty_result_state"] == "TRIGGERED"

    def test_recovery_closes_the_incident(self, alert):
        assert alert["evaluation"]["notification"]["notify_on_ok"] is True

    def test_retrigger_is_capped_at_daily(self, alert):
        # Any noisier and it becomes a channel people mute, after which the next
        # real alert is invisible.
        assert alert["evaluation"]["notification"]["retrigger_seconds"] == 86400

    def test_someone_is_actually_subscribed(self, alert):
        # An alert with no subscribers evaluates forever and tells nobody.
        subs = alert["evaluation"]["notification"]["subscriptions"]
        assert subs, "no subscriptions -- the alert would fire into the void"
