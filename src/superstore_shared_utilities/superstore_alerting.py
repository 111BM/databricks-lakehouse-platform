"""
==============================================================
Module: Alert routing — making the monitors audible
==============================================================

Purpose
-------
Deliver the data-quality monitors somewhere a person will see, instead of only
into driver logs.

Why this exists
---------------
The platform grew three monitors during defect work, and every one of them wrote
a WARN or ERROR into logs that nothing reads:

  - orphaned facts per mart (superseded by the placeholder monitor)
  - placeholder exposure per dimension (superstore_placeholder_monitor)
  - reconciliation balance per entity (superstore_reconciliation)

That is how the largest defect of the week was found: 5% of revenue absent from
product reporting, correctly counted and logged by the mart, and discovered only
because someone ran the query by hand. Instrumentation nobody hears is the same
failure as no instrumentation, one level up — the difference is that it feels
like coverage.

Design rules, each learned the hard way
--------------------------------------
1. **Alerting must never fail the pipeline.** A webhook outage turning a good run
   red would train people to ignore red runs.

2. **But silence must be distinguishable from success.** If no webhook is
   configured, say so in the log. Returning quietly would mean an unconfigured
   channel and a healthy pipeline look identical — precisely the "reports success
   while doing nothing" shape this project keeps finding.

3. **Only alert on conditions that are actually wrong.** A positive `superseded`
   count is normal under incremental loading. Placeholder rows are normal for
   entities with no better value. Alerting on those trains people to mute the
   channel, after which the real alert is invisible too.

The webhook is an optional secret, following `bronze_source_acquisition`: a
workspace that has not configured one still runs, and logs that it is unrouted.
==============================================================
"""

import json
import urllib.error
import urllib.request

SECRET_SCOPE = "superstore"
SECRET_KEY = "slack_webhook_url"

# Conditions worth waking someone for. Everything else is logged and left alone.
ALERTABLE = {
    "reconciliation_unbalanced": "A Bronze row exists that no rule explains",
    "orphaned_facts": "Facts reference a dimension member that does not exist",
    "silver_entity_failed": "A Silver entity failed; downstream layers read stale data",
}


def get_webhook_url(dbutils, scope: str = SECRET_SCOPE, key: str = SECRET_KEY):
    """
    Read the optional webhook secret. Returns None when unconfigured.

    Optional on purpose: the pipeline must run in a workspace that has never set
    up alerting. The caller is expected to LOG that it is unrouted rather than
    treat None as success — see `route_alert`.
    """
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        return None


def build_payload(severity: str, title: str, summary: str, fields: dict = None) -> dict:
    """
    Build the Slack message body. Pure — no network, no dbutils.

    Kept separate so the thing most likely to be wrong (what the message says,
    and whether the numbers reach it) can be unit tested without a webhook.

    `fields` are rendered as `key: value` lines rather than dropped into the
    summary, so an alert carries the measurement that triggered it. An alert
    saying "reconciliation failed" without the counts sends the reader to a
    notebook; one carrying `bronze=1010456, accounted=1010040` does not.
    """
    icon = {"ERROR": ":rotating_light:", "WARN": ":warning:"}.get(severity.upper(), ":information_source:")

    lines = [f"{icon} *{title}*", summary]
    for k, v in (fields or {}).items():
        lines.append(f"• `{k}`: {v}")

    return {
        "text": f"{icon} {title} — {summary}",  # notification fallback
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            }
        ],
    }


def should_alert(condition: str, value) -> bool:
    """
    Decide whether a measured condition deserves an alert.

    Deliberately conservative. `superseded > 0` and `placeholder_rows > 0` are
    NORMAL — the first is how incremental loading works, the second is an entity
    with genuinely no better value. Alerting on either would produce a channel
    that is always red, which is a channel nobody reads.

    Args:
        condition: key from ALERTABLE.
        value: the measurement. Truthy non-zero means alert.
    """
    if condition not in ALERTABLE:
        raise ValueError(
            f"unknown alert condition {condition!r}: add it to ALERTABLE with a "
            f"description, so what is alertable stays an explicit list rather "
            f"than whatever a caller happened to pass"
        )
    return bool(value)


def route_alert(
    logger,
    webhook_url,
    severity: str,
    title: str,
    summary: str,
    fields: dict = None,
    layer: str = "Alerting",
    master_run_id: str = None,
    timeout_secs: int = 10,
) -> bool:
    """
    Send an alert, and never let alerting break the pipeline.

    Returns True only if the webhook accepted it. False covers both "not
    configured" and "delivery failed", and each is logged distinctly — a caller
    that treated them the same would be unable to tell an unrouted workspace from
    a broken one.
    """
    from superstore_logger import log_event

    if not webhook_url:
        log_event(
            logger, "WARN",
            f"alert NOT ROUTED (no webhook configured): {title} — {summary}",
            alert_title=title, alert_severity=severity, alert_routed=False,
            master_run_id=master_run_id, layer=layer, **(fields or {}),
        )
        return False

    payload = build_payload(severity, title, summary, fields)
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_secs) as response:
            ok = 200 <= response.status < 300
    except (urllib.error.URLError, OSError, ValueError) as exc:
        # Swallowed on purpose, and logged loudly. A webhook outage must not turn
        # a healthy pipeline red -- that trains people to ignore red runs, which
        # costs more than the missed alert.
        log_event(
            logger, "ERROR",
            f"alert DELIVERY FAILED: {title} — {exc}",
            alert_title=title, alert_severity=severity, alert_routed=False,
            master_run_id=master_run_id, layer=layer, **(fields or {}),
        )
        return False

    log_event(
        logger, "INFO" if ok else "ERROR",
        f"alert {'routed' if ok else 'rejected by webhook'}: {title}",
        alert_title=title, alert_severity=severity, alert_routed=ok,
        master_run_id=master_run_id, layer=layer, **(fields or {}),
    )
    return ok
