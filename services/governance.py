from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import SourceQueryStat, WatchlistItem


def evaluate_learning_governance(session: Session) -> dict[str, int]:
    now = datetime.utcnow()
    promoted_queries = 0
    suppressed_queries = 0
    expired_queries = 0
    rolled_back_queries = 0
    promoted_watchlist = 0
    suppressed_watchlist = 0
    expired_watchlist = 0
    changed_items: list[str] = []

    query_rows = session.scalars(select(SourceQueryStat)).all()
    for row in query_rows:
        used_recently = row.last_run_at and row.last_run_at >= now - timedelta(days=7)
        positive_score = row.likes + row.saves + (2 * row.applies)
        negative_score = row.dislikes

        if row.status in {"proposed", "generated"} and positive_score >= 2:
            row.status = "active"
            row.decision_reason = "Promoted after positive feedback and lead generation."
            row.last_promoted_at = now
            promoted_queries += 1
            changed_items.append(f"query:{row.query_text}")
        elif row.status in {"proposed", "generated"} and negative_score >= 2 and positive_score == 0:
            row.status = "suppressed"
            row.decision_reason = "Suppressed after repeated low-yield or disliked results."
            row.last_suppressed_at = now
            suppressed_queries += 1
            changed_items.append(f"query:{row.query_text}")
        elif row.status == "active" and negative_score >= positive_score + 2:
            row.status = "rolled_back"
            row.decision_reason = "Rolled back after negative feedback outweighed positive signal."
            row.last_suppressed_at = now
            rolled_back_queries += 1
            changed_items.append(f"query:{row.query_text}")
        elif row.status in {"proposed", "generated"} and not used_recently and row.created_at <= now - timedelta(days=14):
            row.status = "expired"
            row.decision_reason = "Expired after no recent use or evidence."
            expired_queries += 1
            changed_items.append(f"query:{row.query_text}")
        row.last_evaluated_at = now

    watchlist_rows = session.scalars(select(WatchlistItem)).all()
    for row in watchlist_rows:
        age_days = (now - row.updated_at).days
        if row.status == "proposed" and row.confidence == "high":
            row.status = "active"
            row.decision_reason = row.decision_reason or "Promoted automatically from high-confidence evidence."
            row.last_promoted_at = now
            promoted_watchlist += 1
            changed_items.append(f"{row.item_type}:{row.value}")
        elif row.status == "proposed" and age_days >= 14:
            row.status = "expired"
            row.decision_reason = "Expired after sitting unused in proposed state."
            expired_watchlist += 1
            changed_items.append(f"{row.item_type}:{row.value}")
        elif row.status == "active" and "Muted" in (row.source_reason or ""):
            row.status = "rolled_back"
            row.decision_reason = "Rolled back after explicit negative user feedback."
            row.last_suppressed_at = now
            suppressed_watchlist += 1
            changed_items.append(f"{row.item_type}:{row.value}")
        row.last_evaluated_at = now

    session.flush()
    return {
        "promoted_queries": promoted_queries,
        "suppressed_queries": suppressed_queries,
        "expired_queries": expired_queries,
        "rolled_back_queries": rolled_back_queries,
        "promoted_watchlist": promoted_watchlist,
        "suppressed_watchlist": suppressed_watchlist,
        "expired_watchlist": expired_watchlist,
        "changed_items": changed_items,
    }
