from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import ConnectorHealth
from services.activity import log_agent_failure


def get_or_create_connector_health(session: Session, connector_name: str) -> ConnectorHealth:
    row = session.scalar(select(ConnectorHealth).where(ConnectorHealth.connector_name == connector_name))
    if row:
        return row
    row = ConnectorHealth(connector_name=connector_name, status="unknown", circuit_state="closed")
    session.add(row)
    session.flush()
    return row


def connector_circuit_open(row: ConnectorHealth) -> bool:
    return row.circuit_state == "open" and row.disabled_until is not None and row.disabled_until > datetime.utcnow()


def _bounded_score(value: float) -> float:
    return max(0.0, min(round(value, 2), 1.0))


def _compute_trust_score(row: ConnectorHealth) -> float:
    if row.circuit_state == "open":
        return 0.0

    score = 0.2
    if row.last_mode == "live":
        score += 0.35
    elif row.last_mode == "demo":
        score -= 0.05

    if row.status == "healthy":
        score += 0.25
    elif row.status == "recovering":
        score += 0.12
    elif row.status == "degraded":
        score -= 0.08
    elif row.status == "failed":
        score -= 0.3

    score += min(row.recent_successes, 5) * 0.04
    score -= min(row.recent_failures, 5) * 0.08
    score -= min(row.consecutive_failures, 3) * 0.08

    if row.last_item_count <= 0:
        score -= 0.1
    if row.last_failure_classification in {"partial_failure", "quarantined_rows", "source_empty"}:
        score -= 0.12
    elif row.last_failure_classification == "recovering_transport":
        score -= 0.06
    if row.last_freshness_lag_seconds and row.last_freshness_lag_seconds > 7 * 86400:
        score -= 0.12
    elif row.last_freshness_lag_seconds and row.last_freshness_lag_seconds > 2 * 86400:
        score -= 0.05

    if row.quarantine_count > 0:
        denominator = max(row.last_item_count + row.quarantine_count, 1)
        score -= min(0.2, row.quarantine_count / denominator)

    return _bounded_score(score)


def _extract_lag_seconds(items: list[dict[str, Any]], date_fields: list[str]) -> int | None:
    parsed_times = []
    for item in items[:50]:
        for field in date_fields:
            value = item.get(field)
            if not value:
                continue
            if isinstance(value, datetime):
                parsed_times.append(value)
                break
            if isinstance(value, str):
                try:
                    parsed_times.append(datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None))
                    break
                except ValueError:
                    continue
    if not parsed_times:
        return None
    newest = max(parsed_times)
    return max(int((datetime.utcnow() - newest).total_seconds()), 0)


def record_connector_success(
    session: Session,
    connector_name: str,
    items: list[dict[str, Any]],
    mode: str,
    date_fields: list[str],
    note: str | None = None,
    failure_classification: str | None = None,
    quarantine_count: int = 0,
) -> ConnectorHealth:
    row = get_or_create_connector_health(session, connector_name)
    was_unhealthy = row.status in {"failed", "circuit_open", "degraded", "recovering"} or row.consecutive_failures > 0
    row.recent_successes += 1
    row.recent_failures = max(row.recent_failures - 1, 0)
    row.consecutive_failures = 0
    row.circuit_state = "closed"
    row.disabled_until = None
    row.last_success_at = datetime.utcnow()
    row.last_error = note
    row.last_failure_classification = failure_classification
    row.last_mode = mode
    row.last_item_count = len(items)
    row.quarantine_count = quarantine_count
    row.last_freshness_lag_seconds = _extract_lag_seconds(items, date_fields)
    if mode != "live":
        row.status = "degraded"
    elif failure_classification in {"partial_failure", "quarantined_rows", "source_empty"}:
        row.status = "degraded"
    elif was_unhealthy and row.recent_successes <= 1:
        row.status = "recovering"
    else:
        row.status = "healthy"
    row.trust_score = _compute_trust_score(row)
    row.approved_for_unattended = (
        row.last_mode == "live"
        and row.status == "healthy"
        and row.circuit_state == "closed"
        and row.trust_score >= 0.8
    )
    session.flush()
    return row


def record_connector_failure(
    session: Session,
    connector_name: str,
    error_summary: str,
    classification: str | None = None,
    cooldown_minutes: int = 15,
    failure_threshold: int = 3,
) -> ConnectorHealth:
    row = get_or_create_connector_health(session, connector_name)
    row.consecutive_failures += 1
    row.recent_failures += 1
    row.recent_successes = 0
    row.last_failure_at = datetime.utcnow()
    row.last_error = error_summary[:500]
    row.last_failure_classification = classification
    row.status = "failed"
    row.approved_for_unattended = False
    if row.consecutive_failures >= failure_threshold:
        row.circuit_state = "open"
        row.disabled_until = datetime.utcnow() + timedelta(minutes=cooldown_minutes)
        row.status = "circuit_open"
    row.trust_score = _compute_trust_score(row)
    session.flush()
    return row


def run_connector_fetch(
    session: Session,
    connector_name: str,
    fetcher: Callable[[], tuple[list[dict[str, Any]], bool]],
    date_fields: list[str],
    retries: int = 2,
    backoff_seconds: float = 0.25,
) -> tuple[list[dict[str, Any]], bool, ConnectorHealth]:
    row = get_or_create_connector_health(session, connector_name)
    if connector_circuit_open(row):
        row.status = "circuit_open"
        row.approved_for_unattended = False
        row.trust_score = _compute_trust_score(row)
        session.flush()
        log_agent_failure(
            session,
            f"Connector:{connector_name}",
            "fetch",
            f"{connector_name} circuit breaker is open until {row.disabled_until}.",
            metadata_json={"connector_name": connector_name, "circuit_state": row.circuit_state},
        )
        return [], False, row

    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            items, live_used = fetcher()
            note = None
            failure_classification = None
            quarantine_count = 0
            connector_obj = getattr(fetcher, "__self__", None)
            if connector_obj is None and hasattr(fetcher, "func"):
                connector_obj = getattr(fetcher.func, "__self__", None)
            if connector_obj is not None:
                note = getattr(connector_obj, "last_error", None)
                failure_classification = getattr(connector_obj, "last_failure_classification", None)
                quarantine_count = getattr(connector_obj, "last_quarantine_count", 0)
            mode = "live" if live_used else "demo"
            row = record_connector_success(
                session,
                connector_name,
                items,
                mode=mode,
                date_fields=date_fields,
                note=note,
                failure_classification=failure_classification,
                quarantine_count=quarantine_count,
            )
            return items, live_used, row
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff_seconds * (2**attempt))
    classification = getattr(last_exc, "classification", None)
    row = record_connector_failure(
        session,
        connector_name,
        str(last_exc) if last_exc else "unknown connector failure",
        classification=classification,
    )
    log_agent_failure(
        session,
        f"Connector:{connector_name}",
        "fetch",
        f"{connector_name} fetch failed: {row.last_error}",
        metadata_json={
            "connector_name": connector_name,
            "consecutive_failures": row.consecutive_failures,
            "classification": classification,
            "circuit_state": row.circuit_state,
        },
    )
    return [], False, row
