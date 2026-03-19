from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, Lead
from services.sync import list_leads


def test_signal_only_leads_are_excluded_by_default() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    session.add(
        Lead(
            lead_type="signal",
            company_name="Signal Co",
            primary_title="Chief of Staff",
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="low",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Signal lead",
            score_breakdown_json={},
            evidence_json={"url": "https://x.com/demo/status/1", "source_type": "x", "source_platform": "x_demo", "freshness_days": 0},
            hidden=False,
        )
    )
    session.add(
        Lead(
            lead_type="listing",
            company_name="Listing Co",
            primary_title="Operations Lead",
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Listing lead",
            score_breakdown_json={},
            evidence_json={"url": "https://jobs.example.com/1", "source_type": "ashby", "source_platform": "ashby", "freshness_days": 1, "listing_status": "active"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)
    assert len(items) == 1
    assert items[0].lead_type == "listing"

    items_with_signals = list_leads(session, include_signal_only=True)
    assert {item.lead_type for item in items_with_signals} == {"listing", "signal"}
