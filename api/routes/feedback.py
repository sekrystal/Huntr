from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from core.db import get_db
from core.schemas import FeedbackRequest
from services.feedback import submit_feedback


router = APIRouter()


@router.post("/feedback")
def create_feedback(request: FeedbackRequest, db: Session = Depends(get_db)) -> dict[str, str]:
    try:
        submit_feedback(db, request)
        db.commit()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"status": "ok"}
