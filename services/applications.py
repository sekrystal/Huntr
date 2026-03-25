from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import Application, Lead
from core.schemas import ApplicationStatusUpdate
from services.activity import append_lead_agent_trace, log_agent_activity


STATUS_ORDER = [
    "saved",
    "applied",
    "recruiter screen",
    "hiring manager",
    "interview loop",
    "final round",
    "offer",
    "accepted",
    "rejected",
    "withdrawn",
    "archived",
]

TERMINAL_OUTCOME_BY_STATUS = {
    "accepted": "accepted",
    "rejected": "rejected",
    "withdrawn": "withdrawn",
}


def _resolved_outcome_code(application: Application, payload: ApplicationStatusUpdate) -> str | None:
    if payload.outcome_code is not None:
        return payload.outcome_code
    if payload.current_status == "archived":
        return application.outcome_code
    return TERMINAL_OUTCOME_BY_STATUS.get(payload.current_status)


def get_or_create_application(session: Session, lead: Lead) -> Application:
    existing = session.scalar(select(Application).where(Application.lead_id == lead.id))
    if existing:
        return existing
    application = Application(
        lead_id=lead.id,
        company_name=lead.company_name,
        title=lead.primary_title,
        current_status="saved",
        date_saved=datetime.utcnow(),
    )
    session.add(application)
    session.flush()
    return application


def save_for_later(session: Session, lead: Lead) -> Application:
    application = get_or_create_application(session, lead)
    if not application.date_saved:
        application.date_saved = datetime.utcnow()
    if application.current_status not in STATUS_ORDER[1:]:
        application.current_status = "saved"
    return application


def mark_applied(session: Session, lead: Lead, date_applied: datetime | None = None) -> Application:
    application = get_or_create_application(session, lead)
    if not application.date_saved:
        application.date_saved = datetime.utcnow()
    application.date_applied = date_applied or datetime.utcnow()
    application.current_status = "applied"
    return application


def update_application_status(session: Session, payload: ApplicationStatusUpdate) -> Application:
    application = session.scalar(select(Application).where(Application.lead_id == payload.lead_id))
    if not application:
        lead = session.get(Lead, payload.lead_id)
        if not lead:
            raise ValueError(f"Lead {payload.lead_id} not found")
        application = get_or_create_application(session, lead)
    application.current_status = payload.current_status
    application.status_reason_code = payload.status_reason_code
    if payload.notes is not None:
        application.notes = payload.notes
    if payload.date_applied is not None:
        application.date_applied = payload.date_applied
    elif payload.current_status == "applied" and application.date_applied is None:
        application.date_applied = datetime.utcnow()
    application.outcome_code = _resolved_outcome_code(application, payload)
    application.outcome_reason_code = payload.outcome_reason_code
    lead = session.get(Lead, payload.lead_id)
    if lead:
        append_lead_agent_trace(
            lead,
            "Tracker",
            "updated application status",
            f"Tracker set status to {payload.current_status}",
            change_state="updated",
        )
        log_agent_activity(
            session,
            agent_name="Tracker",
            action="updated application status",
            target_type="lead",
            target_count=1,
            target_entity=f"{lead.company_name} / {lead.primary_title}",
            result_summary=f"Tracker set {lead.company_name} / {lead.primary_title} to {payload.current_status}.",
        )
    session.flush()
    return application
