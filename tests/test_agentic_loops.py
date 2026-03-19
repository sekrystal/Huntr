from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from core.models import Application, Base, Lead, Signal, SourceQueryStat
from core.schemas import FeedbackRequest
from services.feedback import submit_feedback
from services.investigations import upsert_investigation
from services.learning import generate_follow_up_tasks
from services.profile import get_candidate_profile


def test_positive_feedback_updates_query_stats() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    get_candidate_profile(session)
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
        evidence_json={"company_domain": "demo.ai", "source_type": "x", "source_queries": ["deployment strategist hiring"], "snippets": ["customer deployments"]},
    )
    session.add(lead)
    session.flush()

    submit_feedback(session, FeedbackRequest(lead_id=lead.id, action="applied"))
    stat = session.scalar(select(SourceQueryStat).where(SourceQueryStat.query_text == "deployment strategist hiring"))
    assert stat is not None
    assert stat.applies >= 1


def test_follow_up_task_generated_for_stale_application() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    application = Application(
        lead_id=1,
        company_name="DemoCo",
        title="Chief of Staff",
        date_saved=datetime.utcnow() - timedelta(days=10),
        date_applied=datetime.utcnow() - timedelta(days=8),
        current_status="applied",
    )
    session.add(application)
    session.flush()

    created = generate_follow_up_tasks(session, follow_up_days=7)
    assert created == 1


def test_unresolved_signal_creates_investigation() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    signal = Signal(
        source_type="x",
        source_url="https://x.com/demo/status/1",
        raw_text="Hiring a chief of staff at a stealth startup.",
        company_guess="Stealth Startup",
        role_guess="chief of staff",
        hiring_confidence=0.55,
    )
    session.add(signal)
    session.flush()

    investigation = upsert_investigation(
        session,
        signal=signal,
        status="open",
        confidence=signal.hiring_confidence,
        note="Needs another resolver pass.",
        next_check_at=datetime.utcnow() + timedelta(hours=6),
    )
    assert investigation.status == "open"
    assert investigation.signal_id == signal.id
