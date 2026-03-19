from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from core.db import get_db
from core.schemas import ApplicationStatusUpdate
from services.applications import update_application_status
from services.pipeline import run_critic_agent, run_ranker_agent


router = APIRouter()


@router.post("/applications/status")
def set_application_status(payload: ApplicationStatusUpdate, db: Session = Depends(get_db)) -> dict:
    try:
        application = update_application_status(db, payload)
        run_ranker_agent(db)
        run_critic_agent(db)
        db.commit()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {
        "status": "ok",
        "lead_id": application.lead_id,
        "current_status": application.current_status,
    }
