from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import Settings, get_settings
from core.models import ConnectorHealth
from services.connectors_health import get_or_create_connector_health


CONNECTOR_CONFIG_KEYS = {
    "greenhouse": "GREENHOUSE_BOARD_TOKENS",
    "ashby": "ASHBY_ORG_KEYS",
    "x_search": "X_BEARER_TOKEN",
    "search_web": "SEARCH_DISCOVERY_ENABLED",
}


def connector_blocked_reason(
    connector_name: str,
    row: ConnectorHealth | None,
    settings: Settings | None = None,
) -> str | None:
    settings = settings or get_settings()

    if connector_name == "greenhouse":
        if not settings.greenhouse_enabled:
            return "disabled"
        if not settings.greenhouse_tokens:
            return "missing_tokens"
    elif connector_name == "ashby":
        if not settings.ashby_orgs:
            return "missing_tokens"
    elif connector_name == "x_search":
        if not settings.x_bearer_token:
            return "missing_tokens"
    elif connector_name == "search_web":
        if not settings.search_discovery_enabled:
            return "disabled"

    if not row:
        return None

    if row.last_failure_classification == "config_error":
        return "config_error"
    if row.circuit_state == "open":
        if row.disabled_until and row.disabled_until > datetime.utcnow():
            return "cooldown"
        return "circuit_open"
    return None


def reset_connector_health(session: Session, connector_name: str) -> ConnectorHealth:
    row = session.scalar(select(ConnectorHealth).where(ConnectorHealth.connector_name == connector_name))
    if row is None:
        row = get_or_create_connector_health(session, connector_name)
    row.status = "unknown"
    row.consecutive_failures = 0
    row.recent_successes = 0
    row.recent_failures = 0
    row.trust_score = 0.0
    row.circuit_state = "closed"
    row.disabled_until = None
    row.last_failure_at = None
    row.last_error = None
    row.last_failure_classification = None
    row.last_mode = None
    row.last_item_count = 0
    row.quarantine_count = 0
    row.approved_for_unattended = False
    row.last_freshness_lag_seconds = None
    session.flush()
    return row
