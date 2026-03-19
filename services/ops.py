from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from core.config import Settings, get_settings
from core.models import SourceQuery, WatchlistItem


def get_runtime_connector_set(settings: Settings | None = None) -> tuple[str, set[str], set[str]]:
    settings = settings or get_settings()
    if settings.demo_mode:
        return "demo", {"greenhouse", "ashby", "x_search"}, set()
    enabled_connectors = {"greenhouse"} if settings.greenhouse_enabled else set()
    strict_live = set(enabled_connectors)
    return "live", enabled_connectors, strict_live


def autonomy_enabled(settings: Settings | None = None) -> bool:
    settings = settings or get_settings()
    return settings.autonomy_enabled


def can_create_generated_queries_today(session: Session, settings: Settings | None = None, requested: int = 1) -> int:
    settings = settings or get_settings()
    since = datetime.utcnow() - timedelta(days=1)
    existing = session.scalar(
        select(func.count(SourceQuery.id)).where(
            SourceQuery.status == "generated",
            SourceQuery.created_at >= since,
        )
    ) or 0
    remaining = max(settings.max_generated_queries_per_day - existing, 0)
    return min(remaining, requested)


def can_add_watchlist_items_today(session: Session, settings: Settings | None = None, requested: int = 1) -> int:
    settings = settings or get_settings()
    since = datetime.utcnow() - timedelta(days=1)
    existing = session.scalar(
        select(func.count(WatchlistItem.id)).where(WatchlistItem.created_at >= since)
    ) or 0
    remaining = max(settings.max_watchlist_additions_per_day - existing, 0)
    return min(remaining, requested)
