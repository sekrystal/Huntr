from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from core.db import get_db
from core.schemas import LeadsResponse
from services.sync import list_leads


router = APIRouter()


@router.get("/leads", response_model=LeadsResponse)
def get_leads(
    freshness_window_days: Optional[int] = Query(default=14),
    include_hidden: bool = Query(default=False),
    include_unqualified: bool = Query(default=False),
    lead_type: Optional[str] = Query(default=None),
    only_saved: bool = Query(default=False),
    only_applied: bool = Query(default=False),
    status: Optional[str] = Query(default=None),
    include_signal_only: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> LeadsResponse:
    return LeadsResponse(
        items=list_leads(
            session=db,
            freshness_window_days=freshness_window_days if freshness_window_days not in {0, -1} else None,
            include_hidden=include_hidden,
            include_unqualified=include_unqualified,
            lead_type=lead_type,
            only_saved=only_saved,
            only_applied=only_applied,
            status=status,
            include_signal_only=include_signal_only,
        )
    )


@router.get("/opportunities", response_model=LeadsResponse)
def get_opportunities_alias(
    freshness_window_days: Optional[int] = Query(default=14),
    include_hidden: bool = Query(default=False),
    include_unqualified: bool = Query(default=False),
    lead_type: Optional[str] = Query(default=None),
    only_saved: bool = Query(default=False),
    only_applied: bool = Query(default=False),
    status: Optional[str] = Query(default=None),
    include_signal_only: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> LeadsResponse:
    return get_leads(
        freshness_window_days=freshness_window_days,
        include_hidden=include_hidden,
        include_unqualified=include_unqualified,
        lead_type=lead_type,
        only_saved=only_saved,
        only_applied=only_applied,
        status=status,
        include_signal_only=include_signal_only,
        db=db,
    )
