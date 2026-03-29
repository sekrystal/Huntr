from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.db import get_db
from core.schemas import SearchRunResponse
from services.search_runs import get_latest_search_run


router = APIRouter()


@router.get("/search-runs/latest", response_model=Optional[SearchRunResponse])
def latest_search_run(db: Session = Depends(get_db)) -> Optional[SearchRunResponse]:
    return get_latest_search_run(db)
