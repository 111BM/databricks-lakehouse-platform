"""
==============================================================
Unit Tests: Alert routing
Target: superstore_alerting
==============================================================

Three monitors were built during defect work and every one wrote only to driver
logs. That is how the largest defect of the week stayed hidden: 5% of revenue
absent from product reporting, correctly counted and logged by the mart, found
only because someone ran the query by hand.

These tests pin the three rules that make routing useful rather than noisy:

  - an alert carries the measurement that triggered it, not just a verdict
  - normal conditions do not alert, or the channel gets muted and the real alert
    goes with it
  - an unconfigured webhook is reported as unrouted, never as success
==============================================================
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from superstore_alerting import (
    ALERTABLE,
    build_payload,
    get_webhook_url,
    route_alert,
    should_alert,
)


@pytest.mark.unit
class TestBuildPayload:

    def test_measurements_reach_the_message(self):
        # An alert saying "reconciliation failed" without the counts sends the
        # reader to a notebook. One carrying them does not.
        p = build_payload(
            "ERROR", "Reconciliation failed", "customers does not balance",
            {"bronze_rows": 1010456, "accounted_rows": 1010040},
        )
        text = json.dumps(p)

        assert "1010456" in text
        assert "1010040" in text

    def test_severity_changes_the_icon(self):
        assert ":rotating_light:" in json.dumps(build_payload("ERROR", "t", "s"))
        assert ":warning:" in json.dumps(build_payload("WARN", "t", "s"))

    def test_fallback_text_is_populated(self):
        # Slack uses `text` for notifications; blocks alone show an empty push.
        p = build_payload("ERROR", "Reconciliation failed", "customers")
        assert p["text"]
        assert "Reconciliation failed" in p["text"]

    def test_works_with_no_fields(self):
        p = build_payload("WARN", "t", "s")
        assert p["blocks"][0]["text"]["text"]

    def test_payload_is_json_serialisable(self):
        # It is POSTed as JSON; a non-serialisable field would fail at runtime.
        json.dumps(build_payload("ERROR", "t", "s", {"a": 1, "b": "x"}))


@pytest.mark.unit
class TestShouldAlert:

    def test_alerts_on_a_real_defect(self):
        assert should_alert("reconciliation_unbalanced", 1) is True
        assert should_alert("orphaned_facts", 42) is True

    def test_does_not_alert_when_the_measurement_is_zero(self):
        assert should_alert("orphaned_facts", 0) is False

    def test_unknown_conditions_raise_rather_than_alerting(self):
        # Keeps what is alertable an explicit list. A typo'd condition should
        # fail loudly at wiring time, not silently alert or silently not.
        with pytest.raises(ValueError, match="unknown alert condition"):
            should_alert("superseded_rows_present", 416)

    def test_normal_conditions_are_deliberately_absent_from_ALERTABLE(self):
        # superseded > 0 is how incremental loading works; placeholder rows are
        # normal for an entity with no better value. Alerting on either produces
        # an always-red channel, and an always-red channel is a muted one.
        assert "superseded_rows" not in ALERTABLE
        assert "placeholder_rows" not in ALERTABLE


@pytest.mark.unit
class TestRouteAlert:

    def _logger(self):
        return MagicMock()

    def test_unconfigured_webhook_returns_false_and_says_so(self):
        # The critical case: an unrouted workspace must not look like a healthy
        # one. Returning quietly would make them identical.
        logged = {}

        def fake_log(logger, level, message, **kw):
            logged["level"] = level
            logged["message"] = message
            logged["routed"] = kw.get("alert_routed")

        with patch("superstore_logger.log_event", fake_log):
            ok = route_alert(self._logger(), None, "ERROR", "t", "s")

        assert ok is False
        assert logged["level"] == "WARN"
        assert "NOT ROUTED" in logged["message"]
        assert logged["routed"] is False

    def test_delivery_failure_never_raises(self):
        # A webhook outage must not turn a healthy pipeline red -- that trains
        # people to ignore red runs, which costs more than the missed alert.
        with patch("superstore_logger.log_event", lambda *a, **k: None):
            with patch("urllib.request.urlopen", side_effect=OSError("boom")):
                ok = route_alert(self._logger(), "https://example.invalid", "ERROR", "t", "s")

        assert ok is False

    def test_delivery_failure_is_logged_at_error(self):
        levels = []
        with patch("superstore_logger.log_event",
                   lambda logger, level, message, **kw: levels.append(level)):
            with patch("urllib.request.urlopen", side_effect=OSError("boom")):
                route_alert(self._logger(), "https://example.invalid", "ERROR", "t", "s")

        assert "ERROR" in levels, "a failed delivery must be loud, not swallowed"

    def test_successful_delivery_returns_true(self):
        response = MagicMock()
        response.status = 200
        response.__enter__ = lambda s: s
        response.__exit__ = lambda s, *a: None

        with patch("superstore_logger.log_event", lambda *a, **k: None):
            with patch("urllib.request.urlopen", return_value=response):
                ok = route_alert(self._logger(), "https://hooks.example/x", "ERROR", "t", "s")

        assert ok is True

    def test_non_2xx_is_not_treated_as_delivered(self):
        response = MagicMock()
        response.status = 500
        response.__enter__ = lambda s: s
        response.__exit__ = lambda s, *a: None

        with patch("superstore_logger.log_event", lambda *a, **k: None):
            with patch("urllib.request.urlopen", return_value=response):
                ok = route_alert(self._logger(), "https://hooks.example/x", "ERROR", "t", "s")

        assert ok is False


@pytest.mark.unit
class TestGetWebhookUrl:

    def test_missing_secret_returns_none_rather_than_raising(self):
        dbutils = MagicMock()
        dbutils.secrets.get.side_effect = Exception("no such secret")

        assert get_webhook_url(dbutils) is None

    def test_returns_the_secret_when_present(self):
        dbutils = MagicMock()
        dbutils.secrets.get.return_value = "https://hooks.example/abc"

        assert get_webhook_url(dbutils) == "https://hooks.example/abc"
