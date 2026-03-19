from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import get_settings
from core.models import Application, CandidateProfile, FollowUpTask, SourceQueryStat, WatchlistItem
from core.schemas import FollowUpTaskResponse, LearningViewResponse, QueryLearningRow, WatchlistItemResponse
from services.ops import can_add_watchlist_items_today


def get_or_create_query_stat(session: Session, source_type: str, query_text: str, status: str = "active") -> SourceQueryStat:
    stat = session.scalar(
        select(SourceQueryStat).where(
            SourceQueryStat.source_type == source_type,
            SourceQueryStat.query_text == query_text,
        )
    )
    if stat:
        if status and stat.status != status:
            stat.status = status
        return stat
    stat = SourceQueryStat(source_type=source_type, query_text=query_text, status=status)
    session.add(stat)
    session.flush()
    return stat


def increment_query_stat(session: Session, source_type: str, query_text: str, field_name: str, delta: int = 1) -> None:
    stat = get_or_create_query_stat(session, source_type=source_type, query_text=query_text)
    setattr(stat, field_name, getattr(stat, field_name) + delta)
    stat.last_run_at = datetime.utcnow()


def mark_query_status(session: Session, query_text: str, source_type: str, status: str) -> None:
    stat = get_or_create_query_stat(session, source_type=source_type, query_text=query_text, status=status)
    stat.status = status


def add_watchlist_item(
    session: Session,
    item_type: str,
    value: str,
    source_reason: str,
    confidence: str = "medium",
    status: str = "proposed",
) -> bool:
    existing = session.scalar(select(WatchlistItem).where(WatchlistItem.item_type == item_type, WatchlistItem.value == value))
    if existing:
        changed = (
            existing.source_reason != source_reason
            or existing.confidence != confidence
            or existing.status != status
        )
        existing.source_reason = source_reason
        existing.confidence = confidence
        existing.status = status
        return changed
    if can_add_watchlist_items_today(session, requested=1) <= 0:
        return False
    session.add(
        WatchlistItem(
            item_type=item_type,
            value=value,
            source_reason=source_reason,
            confidence=confidence,
            status=status,
        )
    )
    return True


def generate_follow_up_tasks(session: Session, follow_up_days: int = 7) -> int:
    created = 0
    applications = session.scalars(select(Application)).all()
    for application in applications:
        if not application.date_applied or application.current_status in {"offer", "rejected", "archived"}:
            continue
        due_at = application.date_applied + timedelta(days=follow_up_days)
        if due_at > datetime.utcnow():
            continue
        existing = session.scalar(
            select(FollowUpTask).where(
                FollowUpTask.application_id == application.id,
                FollowUpTask.status == "open",
            )
        )
        if existing:
            existing.notes = existing.notes or "Follow up on this application."
            continue
        session.add(
            FollowUpTask(
                application_id=application.id,
                task_type="follow_up",
                due_at=due_at,
                status="open",
                notes="No update since application. Consider a follow-up message.",
            )
        )
        created += 1
    session.flush()
    return created


def next_action_for_application(session: Session, application_id: int) -> tuple[str | None, bool]:
    task = session.scalar(
        select(FollowUpTask).where(
            FollowUpTask.application_id == application_id,
            FollowUpTask.status == "open",
        ).order_by(FollowUpTask.due_at.asc())
    )
    if not task:
        return None, False
    return task.notes or "Follow up on this application.", task.due_at <= datetime.utcnow()


def build_learning_view(session: Session, profile: CandidateProfile) -> LearningViewResponse:
    settings = get_settings()
    learning = (profile.extracted_summary_json or {}).get("learning", {})
    query_rows = session.scalars(
        select(SourceQueryStat).order_by(
            SourceQueryStat.applies.desc(),
            SourceQueryStat.likes.desc(),
            SourceQueryStat.leads_generated.desc(),
            SourceQueryStat.updated_at.desc(),
        ).limit(8)
    ).all()
    watchlist_rows = session.scalars(select(WatchlistItem).order_by(WatchlistItem.updated_at.desc()).limit(12)).all()
    follow_up_rows = session.scalars(
        select(FollowUpTask).where(FollowUpTask.status == "open").order_by(FollowUpTask.due_at.asc())
    ).all()

    follow_ups: list[FollowUpTaskResponse] = []
    for task in follow_up_rows:
        application = session.get(Application, task.application_id)
        if not application:
            continue
        follow_ups.append(
            FollowUpTaskResponse(
                application_id=application.id,
                company_name=application.company_name,
                title=application.title,
                task_type=task.task_type,
                due_at=task.due_at,
                status=task.status,
                notes=task.notes,
            )
        )

    return LearningViewResponse(
        top_queries=[
            QueryLearningRow(
                query_text=row.query_text,
                source_type=row.source_type,
                status=row.status,
                decision_reason=row.decision_reason,
                leads_generated=row.leads_generated,
                likes=row.likes,
                saves=row.saves,
                applies=row.applies,
                dislikes=row.dislikes,
                last_run_at=row.last_run_at,
            )
            for row in query_rows
        ],
        generated_queries=learning.get("generated_queries", [])[-settings.learning_max_generated_queries_total :],
        suppressed_queries=[row.query_text for row in query_rows if row.status == "suppressed"][:8],
        inferred_title_families=list((learning.get("role_family_weights") or {}).keys())[:5],
        inferred_domains=list((learning.get("domain_weights") or {}).keys())[:5],
        watchlist_items=[
            WatchlistItemResponse(
                item_type=row.item_type,
                value=row.value,
                source_reason=row.source_reason,
                confidence=row.confidence,
                status=row.status,
                decision_reason=row.decision_reason,
            )
            for row in watchlist_rows
        ],
        follow_up_tasks=follow_ups,
    )
