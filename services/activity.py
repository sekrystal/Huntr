from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import get_settings
from core.models import AgentActivity, AgentRun, Lead
from core.schemas import AgentActivityResponse


def append_lead_agent_trace(
    lead: Lead,
    agent_name: str,
    action: str,
    summary: str,
    change_state: Optional[str] = None,
) -> None:
    evidence = dict(lead.evidence_json or {})
    history = list(evidence.get("agent_actions", []))
    history.append(
        {
            "timestamp": datetime.utcnow().isoformat(),
            "agent": agent_name,
            "action": action,
            "summary": summary,
        }
    )
    evidence["agent_actions"] = history[-12:]
    if change_state is not None:
        existing_state = evidence.get("change_state")
        if existing_state == "new" and change_state in {"updated", "reranked"}:
            evidence["change_state"] = "new"
        else:
            evidence["change_state"] = change_state
    lead.evidence_json = evidence
    lead.last_agent_action = f"{agent_name}: {action}"


def log_agent_activity(
    session: Session,
    agent_name: str,
    action: str,
    result_summary: str,
    target_type: Optional[str] = None,
    target_count: Optional[int] = None,
    target_entity: Optional[str] = None,
) -> AgentActivity:
    settings = get_settings()
    if settings.activity_dedupe_window_seconds > 0:
        cutoff = datetime.utcnow().timestamp() - settings.activity_dedupe_window_seconds
        recent = session.scalars(
            select(AgentActivity)
            .where(
                AgentActivity.agent_name == agent_name,
                AgentActivity.action == action,
            )
            .order_by(AgentActivity.created_at.desc())
            .limit(1)
        ).first()
        if recent and recent.result_summary == result_summary and recent.target_entity == target_entity:
            if recent.created_at.timestamp() >= cutoff:
                return recent
    activity = AgentActivity(
        agent_name=agent_name,
        action=action,
        target_type=target_type,
        target_count=target_count,
        target_entity=target_entity,
        result_summary=result_summary,
    )
    session.add(activity)
    session.flush()
    return activity


def list_agent_activities(session: Session, limit: int = 100) -> list[AgentActivityResponse]:
    records = session.scalars(
        select(AgentActivity).order_by(AgentActivity.created_at.desc(), AgentActivity.id.desc()).limit(limit)
    ).all()
    return [
        AgentActivityResponse(
            id=record.id,
            timestamp=record.created_at,
            agent_name=record.agent_name,
            action=record.action,
            target_type=record.target_type,
            target_count=record.target_count,
            target_entity=record.target_entity,
            result_summary=record.result_summary,
        )
        for record in records
    ]


def log_agent_run(
    session: Session,
    agent_name: str,
    action: str,
    summary: str,
    affected_count: int,
    status: str = "ok",
    metadata_json: Optional[dict] = None,
) -> AgentRun:
    run = AgentRun(
        agent_name=agent_name,
        action=action,
        status=status,
        summary=summary,
        affected_count=affected_count,
        metadata_json=metadata_json or {},
    )
    session.add(run)
    session.flush()
    return run


def log_agent_failure(
    session: Session,
    agent_name: str,
    action: str,
    error_summary: str,
    metadata_json: Optional[dict] = None,
) -> AgentRun:
    log_agent_activity(
        session,
        agent_name=agent_name,
        action=f"{action} failed",
        target_type="failure",
        target_count=0,
        result_summary=error_summary,
    )
    return log_agent_run(
        session,
        agent_name=agent_name,
        action=action,
        summary=error_summary,
        affected_count=0,
        status="failed",
        metadata_json=metadata_json or {},
    )
