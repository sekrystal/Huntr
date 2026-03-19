from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from core.db import get_db, reset_sqlite_db
from core.schemas import (
    AgentActivitiesResponse,
    AgentRunRequest,
    AgentRunResponse,
    AutonomyStatusResponse,
    InvestigationsResponse,
    LearningViewResponse,
)
from scripts.seed_demo_data import main as seed_demo_main
from services.activity import list_agent_activities, log_agent_failure
from services.autonomy import build_autonomy_health, build_daily_digest, build_latest_run_digest, list_connector_health
from services.investigations import list_investigations
from services.learning import build_learning_view
from services.pipeline import (
    run_critic_agent,
    run_fit_agent,
    run_full_pipeline,
    run_query_evolution_agent,
    run_ranker_agent,
    run_resolver_agent,
    run_scout_agent,
    run_tracker_agent,
)
from services.profile import get_candidate_profile


router = APIRouter()


@router.get("/agent-activity", response_model=AgentActivitiesResponse)
def get_agent_activity(db: Session = Depends(get_db)) -> AgentActivitiesResponse:
    return AgentActivitiesResponse(items=list_agent_activities(db))


@router.get("/investigations", response_model=InvestigationsResponse)
def get_investigations(db: Session = Depends(get_db)) -> InvestigationsResponse:
    return InvestigationsResponse(items=list_investigations(db))


@router.get("/learning", response_model=LearningViewResponse)
def get_learning(db: Session = Depends(get_db)) -> LearningViewResponse:
    return build_learning_view(db, get_candidate_profile(db))


@router.get("/autonomy-status", response_model=AutonomyStatusResponse)
def get_autonomy_status(db: Session = Depends(get_db)) -> AutonomyStatusResponse:
    return AutonomyStatusResponse(
        health=build_autonomy_health(db),
        digest=build_latest_run_digest(db),
        daily_digest=build_daily_digest(db),
        connector_health=list_connector_health(db),
    )


@router.post("/agents/run", response_model=AgentRunResponse)
def run_agent(payload: AgentRunRequest, db: Session = Depends(get_db)) -> AgentRunResponse:
    try:
        if payload.agent == "scout":
            response = run_scout_agent(db)
            db.commit()
            return response
        if payload.agent == "resolver":
            response = run_resolver_agent(db)
            db.commit()
            return response
        if payload.agent == "fit":
            response = run_fit_agent(db)
            db.commit()
            return response
        if payload.agent == "ranker":
            response = run_ranker_agent(db)
            db.commit()
            return response
        if payload.agent == "critic":
            response = run_critic_agent(db)
            db.commit()
            return response
        if payload.agent == "tracker":
            response = run_tracker_agent(db)
            db.commit()
            return response
        if payload.agent == "learning":
            response = run_query_evolution_agent(db)
            db.commit()
            return response
        if payload.agent == "full_pipeline":
            response = run_full_pipeline(db)
            db.commit()
            return response

        db.close()
        reset_sqlite_db()
        seed_demo_main()
        return AgentRunResponse(agent="reset_demo", summary="Reset demo data, recreated the schema, and reseeded fresh records.")
    except Exception as exc:
        db.rollback()
        log_agent_failure(db, payload.agent.title(), f"run {payload.agent}", f"{payload.agent} failed: {exc}")
        db.commit()
        raise HTTPException(status_code=500, detail=f"{payload.agent} failed: {exc}") from exc
