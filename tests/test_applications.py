from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Application, Base, Lead
from core.schemas import ApplicationStatusUpdate
from services.applications import update_application_status


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def _lead() -> Lead:
    return Lead(
        lead_type="listing",
        company_name="DemoCo",
        primary_title="Chief of Staff",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "operations"},
        evidence_json={"source_type": "greenhouse"},
    )


def test_application_tracker_supports_richer_lifecycle_states() -> None:
    session = _session()
    lead = _lead()
    session.add(lead)
    session.flush()

    applied_at = datetime(2026, 3, 20, 15, 0, 0)
    application = update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="applied",
            date_applied=applied_at,
            notes="Submitted through ATS",
        ),
    )
    assert application.current_status == "applied"
    assert application.date_applied == applied_at

    application = update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="recruiter screen",
            status_reason_code="awaiting_scheduler_confirmation",
            notes="Recruiter requested times for a first call",
        ),
    )
    assert application.current_status == "recruiter screen"
    assert application.status_reason_code == "awaiting_scheduler_confirmation"
    assert application.notes == "Recruiter requested times for a first call"
    assert application.outcome_code is None
    assert application.outcome_reason_code is None
    assert application.date_applied == applied_at

    application = update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="final round",
            status_reason_code="onsite_completed",
        ),
    )
    assert application.current_status == "final round"
    assert application.status_reason_code == "onsite_completed"


def test_application_tracker_persists_outcomes_and_reason_codes() -> None:
    session = _session()
    lead = _lead()
    session.add(lead)
    session.flush()

    update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="applied",
            date_applied=datetime(2026, 3, 21, 10, 30, 0),
        ),
    )

    application = update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="rejected",
            status_reason_code="panel_decline",
            outcome_reason_code="insufficient_b2b_saas_depth",
            notes="Strong operator profile, but the panel wanted deeper pricing experience.",
        ),
    )
    session.commit()

    stored = session.query(Application).filter(Application.lead_id == lead.id).one()
    assert stored.current_status == "rejected"
    assert stored.status_reason_code == "panel_decline"
    assert stored.outcome_code == "rejected"
    assert stored.outcome_reason_code == "insufficient_b2b_saas_depth"
    assert stored.notes == "Strong operator profile, but the panel wanted deeper pricing experience."
