from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Application, Base, Lead
from core.schemas import FeedbackRequest
from services.feedback import submit_feedback
from services.profile import get_candidate_profile


def test_feedback_generates_learning_and_queries() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    profile = get_candidate_profile(session)
    lead = Lead(
        lead_type="listing",
        company_name="DemoCo",
        primary_title="Deployment Strategist",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "go_to_market"},
        evidence_json={"company_domain": "demo.ai", "source_type": "ashby", "source_queries": ["deployment strategist hiring"], "snippets": ["customer deployments and systems"]},
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="more_like_this"))
    learning = profile.extracted_summary_json["learning"]
    assert learning["title_weights"]["deployment strategist"] > 0
    assert learning["generated_queries"]


def test_save_and_applied_create_application_state() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    lead = Lead(
        lead_type="listing",
        company_name="DemoCo",
        primary_title="Chief of Staff",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "operations"},
        evidence_json={"company_domain": "demo.ai", "source_type": "ashby", "source_queries": [], "snippets": ["ops"]},
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="save"))
    application = session.query(Application).filter(Application.lead_id == lead.id).one()
    assert application.current_status == "saved"
    assert application.date_saved is not None

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="applied"))
    assert application.current_status == "applied"
    assert application.date_applied is not None
