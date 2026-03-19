from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import SessionLocal, engine, init_db
from core.models import Application, Base, Lead
from core.schemas import FeedbackRequest
from services.activity import log_agent_activity
from services.feedback import submit_feedback
from services.pipeline import run_critic_agent, run_fit_agent, run_query_evolution_agent, run_ranker_agent, run_tracker_agent
from services.profile import ingest_resume
from services.sync import sync_all
from datetime import datetime, timedelta


def main() -> None:
    Base.metadata.drop_all(bind=engine)
    init_db()
    with SessionLocal() as session:
        ingest_resume(
            session,
            filename="demo_resume.txt",
            raw_text=(
                "Senior operator with 7+ years in early-stage AI and developer tools companies. "
                "Led hiring, planning, business operations, customer deployments, and chief of staff style work "
                "across San Francisco, New York, and remote teams. Interested in seed and Series A startups."
            ),
        )
        result = sync_all(session, include_rechecks=True)
        log_agent_activity(
            session,
            agent_name="Scout",
            action="seeded baseline demo data",
            target_type="records",
            target_count=result.signals_ingested + result.listings_ingested,
            result_summary="Scout seeded the baseline listings and weak signals for the demo workbench.",
        )
        log_agent_activity(
            session,
            agent_name="Resolver",
            action="built initial lead set",
            target_type="leads",
            target_count=result.leads_created + result.leads_updated,
            result_summary="Resolver linked seed signals to listings where possible and created the initial lead set.",
        )
        session.flush()

        mercor_lead = session.query(Lead).filter(Lead.company_name == "Mercor", Lead.primary_title == "Deployment Strategist").first()
        vercel_lead = session.query(Lead).filter(Lead.company_name == "Vercel").first()
        granola_lead = session.query(Lead).filter(Lead.company_name == "Granola", Lead.primary_title == "Chief of Staff").first()
        if mercor_lead:
            submit_feedback(session, FeedbackRequest(lead_id=mercor_lead.id, action="applied"))
            submit_feedback(session, FeedbackRequest(lead_id=mercor_lead.id, action="more_like_this"))
        if vercel_lead:
            submit_feedback(session, FeedbackRequest(lead_id=vercel_lead.id, action="save"))
            submit_feedback(session, FeedbackRequest(lead_id=vercel_lead.id, action="like"))
        if granola_lead:
            submit_feedback(session, FeedbackRequest(lead_id=granola_lead.id, action="save"))

        mercor_application = session.query(Application).filter(Application.company_name == "Mercor").first()
        if mercor_application and mercor_application.date_applied:
            mercor_application.date_applied = datetime.utcnow() - timedelta(days=9)
            mercor_application.updated_at = mercor_application.date_applied
            mercor_application.notes = "Seeded stale application for follow-up task generation."

        result = sync_all(session, include_rechecks=True)
        run_fit_agent(session)
        run_ranker_agent(session)
        run_critic_agent(session)
        run_tracker_agent(session)
        run_query_evolution_agent(session)
        for lead in session.query(Lead).all():
            evidence = dict(lead.evidence_json or {})
            evidence["change_state"] = ""
            lead.evidence_json = evidence
        session.commit()
        print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
