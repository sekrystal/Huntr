from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import Investigation, Signal
from core.schemas import InvestigationResponse


def upsert_investigation(
    session: Session,
    signal: Signal,
    status: str,
    confidence: float,
    note: str,
    next_check_at: datetime | None = None,
) -> Investigation:
    investigation = session.scalar(select(Investigation).where(Investigation.signal_id == signal.id))
    if not investigation:
        investigation = Investigation(
            signal_id=signal.id,
            company_guess=signal.company_guess,
            role_guess=signal.role_guess,
            confidence=confidence,
            status=status,
            attempts=0,
            next_check_at=next_check_at,
            resolution_notes=note,
        )
        session.add(investigation)
    else:
        if investigation.company_guess != signal.company_guess:
            investigation.company_guess = signal.company_guess
        if investigation.role_guess != signal.role_guess:
            investigation.role_guess = signal.role_guess
        if investigation.confidence != confidence:
            investigation.confidence = confidence
        if investigation.status != status:
            investigation.status = status
        if investigation.resolution_notes != note:
            investigation.resolution_notes = note
        if status == "resolved":
            investigation.next_check_at = None
        elif next_check_at is not None and (
            investigation.next_check_at is None or next_check_at < investigation.next_check_at
        ):
            investigation.next_check_at = next_check_at
    session.flush()
    return investigation


def mark_investigation_attempt(session: Session, signal_id: int, note: str, hours_until_retry: int = 6) -> None:
    investigation = session.scalar(select(Investigation).where(Investigation.signal_id == signal_id))
    if not investigation:
        return
    investigation.attempts += 1
    investigation.status = "rechecking"
    investigation.next_check_at = datetime.utcnow() + timedelta(hours=hours_until_retry)
    investigation.resolution_notes = note


def list_investigations(session: Session) -> list[InvestigationResponse]:
    investigations = session.scalars(select(Investigation).order_by(Investigation.updated_at.desc())).all()
    rows: list[InvestigationResponse] = []
    for investigation in investigations:
        signal = session.get(Signal, investigation.signal_id)
        rows.append(
            InvestigationResponse(
                id=investigation.id,
                signal_id=investigation.signal_id,
                company_guess=investigation.company_guess,
                role_guess=investigation.role_guess,
                confidence=investigation.confidence,
                status=investigation.status,
                attempts=investigation.attempts,
                next_check_at=investigation.next_check_at,
                resolution_notes=investigation.resolution_notes,
                source_url=signal.source_url if signal else None,
                raw_text=signal.raw_text if signal else None,
            )
        )
    return rows
