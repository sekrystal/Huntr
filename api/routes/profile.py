from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.db import get_db
from core.schemas import CandidateProfilePayload, ResumeUploadRequest, ResumeUploadResponse
from services.pipeline import run_critic_agent, run_ranker_agent
from services.profile import build_learning_summary, get_candidate_profile, ingest_resume, profile_to_payload, update_candidate_profile


router = APIRouter()


@router.get("/candidate-profile", response_model=CandidateProfilePayload)
def read_candidate_profile(db: Session = Depends(get_db)) -> CandidateProfilePayload:
    return profile_to_payload(get_candidate_profile(db))


@router.post("/candidate-profile", response_model=CandidateProfilePayload)
def write_candidate_profile(payload: CandidateProfilePayload, db: Session = Depends(get_db)) -> CandidateProfilePayload:
    profile = update_candidate_profile(db, payload)
    run_ranker_agent(db)
    run_critic_agent(db)
    db.commit()
    return profile_to_payload(profile)


@router.post("/resume", response_model=ResumeUploadResponse)
def upload_resume(payload: ResumeUploadRequest, db: Session = Depends(get_db)) -> ResumeUploadResponse:
    result = ingest_resume(db, payload.filename, payload.raw_text)
    run_ranker_agent(db)
    run_critic_agent(db)
    db.commit()
    return result


@router.get("/profile-learning")
def profile_learning(db: Session = Depends(get_db)) -> dict:
    profile = get_candidate_profile(db)
    return build_learning_summary(profile).model_dump()
