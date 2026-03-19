from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import Company, RecheckQueue


KNOWN_ALIASES = {
    "cursor": "Cursor",
    "granola": "Granola",
    "linear": "Linear",
    "mercor": "Mercor",
    "warp": "Warp",
    "vercel": "Vercel",
    "stealth ai infra startup": "Stealth AI Infra Startup",
}


def resolve_company_name(session: Session, company_guess: Optional[str], raw_text: str = "") -> Optional[str]:
    if company_guess:
        normalized = company_guess.strip()
        existing = session.scalar(select(Company).where(Company.name.ilike(normalized)))
        if existing:
            return existing.name
        return KNOWN_ALIASES.get(normalized.lower(), normalized)

    lowered = raw_text.lower()
    for alias, canonical in KNOWN_ALIASES.items():
        if alias in lowered:
            return canonical
    return None


def get_or_create_company(
    session: Session,
    name: str,
    domain: Optional[str] = None,
    careers_url: Optional[str] = None,
    ats_provider: Optional[str] = None,
) -> Company:
    company = session.scalar(select(Company).where(Company.name == name))
    if company:
        if domain and not company.domain:
            company.domain = domain
        if careers_url and not company.careers_url:
            company.careers_url = careers_url
        if ats_provider and not company.ats_provider:
            company.ats_provider = ats_provider
        return company

    company = Company(name=name, domain=domain, careers_url=careers_url, ats_provider=ats_provider)
    session.add(company)
    session.flush()
    return company


def queue_recheck(session: Session, entity_type: str, entity_id: int, note: str) -> RecheckQueue:
    existing = session.scalar(
        select(RecheckQueue).where(
            RecheckQueue.entity_type == entity_type,
            RecheckQueue.entity_id == entity_id,
            RecheckQueue.status.in_(["queued", "retrying"]),
        )
    )
    if existing:
        return existing

    item = RecheckQueue(
        entity_type=entity_type,
        entity_id=entity_id,
        next_check_at=datetime.utcnow() + timedelta(hours=6),
        retry_count=0,
        status="queued",
        notes=note,
    )
    session.add(item)
    session.flush()
    return item
