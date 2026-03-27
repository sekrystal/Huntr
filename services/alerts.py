from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from core.config import Settings, get_settings
from core.models import AgentRun, AlertEvent, Lead, RunDigest
from core.time import utcnow
from services.autonomy import build_autonomy_health, list_connector_health


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _record_alert(
    session: Session,
    alert_key: str,
    category: str,
    severity: str,
    status: str,
    summary: str,
    details_json: dict[str, Any] | None = None,
) -> AlertEvent:
    event = AlertEvent(
        alert_key=alert_key,
        category=category,
        severity=severity,
        status=status,
        summary=summary,
        details_json=_json_safe(details_json or {}),
    )
    session.add(event)
    session.flush()
    return event


def _rate_limited(session: Session, alert_key: str, settings: Settings) -> bool:
    cutoff = utcnow() - timedelta(seconds=settings.alert_window_seconds)
    counted_statuses = ["logged", "sent", "failed", "disabled"]
    recent_same_key = session.scalar(
        select(func.count(AlertEvent.id)).where(
            AlertEvent.alert_key == alert_key,
            AlertEvent.created_at >= cutoff,
            AlertEvent.status.in_(counted_statuses),
        )
    ) or 0
    recent_total = session.scalar(
        select(func.count(AlertEvent.id)).where(
            AlertEvent.created_at >= cutoff,
            AlertEvent.status.in_(counted_statuses),
        )
    ) or 0
    return recent_same_key > 0 or recent_total >= settings.alert_max_per_window


def _send_slack(settings: Settings, summary: str, details_json: dict[str, Any]) -> tuple[str, str | None]:
    if not settings.alerts_enabled:
        return "disabled", None
    if not settings.slack_webhook_url:
        return "logged", "No Slack webhook configured."
    try:
        response = requests.post(
            settings.slack_webhook_url,
            json={"text": f"[Opportunity Scout] {summary}\n```{details_json}```"},
            timeout=10,
        )
        response.raise_for_status()
        return "sent", None
    except Exception as exc:  # pragma: no cover
        return "failed", str(exc)


def evaluate_alerts(session: Session, settings: Settings | None = None) -> list[AlertEvent]:
    settings = settings or get_settings()
    health = build_autonomy_health(session, settings=settings)
    connectors = list_connector_health(session)
    latest_digest = session.scalar(select(RunDigest).order_by(RunDigest.created_at.desc()).limit(1))
    alerts: list[tuple[str, str, str, str, dict[str, Any]]] = []

    if not settings.autonomy_enabled:
        alerts.append(
            (
                "autonomy_disabled",
                "kill_switch",
                "warning",
                "Global autonomy kill switch is active.",
                {"autonomy_enabled": False},
            )
        )
    else:
        greenhouse = next((row for row in connectors if row.connector_name == "greenhouse"), None)
        if greenhouse:
            if greenhouse.circuit_state == "open":
                alerts.append(
                    (
                        "greenhouse_circuit_open",
                        "connector",
                        "critical",
                        "Greenhouse circuit breaker is open.",
                        greenhouse.model_dump(),
                    )
                )
            if greenhouse.status == "degraded" and greenhouse.last_success_at:
                age_seconds = int((utcnow() - greenhouse.last_success_at).total_seconds())
                if age_seconds >= settings.alert_greenhouse_degraded_seconds:
                    alerts.append(
                        (
                            "greenhouse_degraded_too_long",
                            "connector",
                            "warning",
                            "Greenhouse has remained degraded beyond the configured threshold.",
                            {"age_seconds": age_seconds, **greenhouse.model_dump()},
                        )
                    )
            if not greenhouse.last_success_at or (
                utcnow() - greenhouse.last_success_at
            ).total_seconds() >= settings.alert_no_successful_fetch_seconds:
                alerts.append(
                    (
                        "greenhouse_no_recent_success",
                        "connector",
                        "critical",
                        "Greenhouse has not had a successful fetch within the configured threshold.",
                        greenhouse.model_dump(),
                    )
                )

        duplicate_rows = session.execute(
            select(Lead.company_name, Lead.primary_title, func.count(Lead.id))
            .group_by(Lead.company_name, Lead.primary_title, Lead.lead_type, Lead.listing_id, Lead.signal_id)
            .having(func.count(Lead.id) > 1)
        ).all()
        duplicate_count = len(duplicate_rows)
        if duplicate_count >= settings.alert_duplicate_lead_threshold and duplicate_count > 0:
            alerts.append(
                (
                    "duplicate_lead_threshold",
                    "data_quality",
                    "critical",
                    "Duplicate lead count exceeded the configured threshold.",
                    {"duplicate_rows": [list(row) for row in duplicate_rows[:10]]},
                )
            )

        visible_stale_count = session.scalar(
            select(func.count(Lead.id)).where(Lead.hidden.is_(False), Lead.freshness_label.in_(["stale", "unknown"]))
        ) or 0
        if visible_stale_count >= settings.alert_visible_stale_threshold and visible_stale_count > 0:
            alerts.append(
                (
                    "visible_stale_rows",
                    "data_quality",
                    "critical",
                    "Visible stale or unknown-freshness leads exceeded the configured threshold.",
                    {"visible_stale_count": visible_stale_count},
                )
            )

        if latest_digest is None or (
            latest_digest.created_at and (utcnow() - latest_digest.created_at).total_seconds() >= settings.alert_empty_digest_seconds
        ):
            alerts.append(
                (
                    "digest_missing_or_stale",
                    "autonomy",
                    "warning",
                    "No recent run digest was recorded within the configured threshold.",
                    {"latest_digest_at": latest_digest.created_at.isoformat() if latest_digest else None},
                )
            )

    if not settings.greenhouse_enabled:
        alerts.append(
            (
                "greenhouse_disabled",
                "kill_switch",
                "warning",
                "Greenhouse connector kill switch is active.",
                {"greenhouse_enabled": False},
            )
        )

    latest_worker_failure = session.scalar(
        select(AgentRun)
        .where(AgentRun.agent_name == "Worker", AgentRun.status == "failed")
        .order_by(AgentRun.created_at.desc())
        .limit(1)
    )
    if latest_worker_failure and (
        utcnow() - latest_worker_failure.created_at
    ).total_seconds() <= settings.alert_window_seconds:
        alerts.append(
            (
                "worker_loop_failure",
                "worker",
                "critical",
                "Worker loop failed recently.",
                {
                    "created_at": latest_worker_failure.created_at.isoformat(),
                    "summary": latest_worker_failure.summary,
                },
            )
        )

    recorded: list[AlertEvent] = []
    for alert_key, category, severity, summary, details_json in alerts:
        if _rate_limited(session, alert_key, settings):
            recorded.append(_record_alert(session, alert_key, category, severity, "rate_limited", summary, details_json))
            continue
        status, error = _send_slack(settings, summary, details_json)
        if error:
            details_json = {**details_json, "alert_error": error}
        recorded.append(_record_alert(session, alert_key, category, severity, status, summary, details_json))
    session.flush()
    return recorded
