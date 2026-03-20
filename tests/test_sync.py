from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, CandidateProfile, Lead, Listing
from services.pipeline import run_critic_agent
from services.sync import list_leads


def _seed_profile(session) -> None:
    session.add(
        CandidateProfile(
            name="Tester",
            raw_resume_text="ops profile",
            core_titles_json=["operations lead", "deployment strategist"],
            preferred_locations_json=["Remote", "San Francisco", "New York"],
            minimum_fit_threshold=2.8,
        )
    )
    session.commit()


def test_signal_only_leads_are_excluded_by_default() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

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
                score_breakdown_json={"composite": 4.5},
                evidence_json={"url": "https://x.com/demo/status/1", "source_type": "x", "source_platform": "x_demo", "freshness_days": 0},
                hidden=False,
            )
        )
    session.add(
        Listing(
            company_name="Listing Co",
            title="Operations Lead",
            location="Remote",
            url="https://jobs.example.com/1",
            source_type="ashby",
            posted_at=datetime.utcnow(),
            first_published_at=datetime.utcnow(),
            last_seen_at=datetime.utcnow(),
            description_text="Own operating cadence and planning.",
            listing_status="active",
            freshness_hours=4.0,
            freshness_days=1,
            metadata_json={},
        )
    )
    session.flush()
    listing = session.query(Listing).filter(Listing.company_name == "Listing Co").one()
    session.add(
        Lead(
            lead_type="listing",
            company_name="Listing Co",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Listing lead",
            score_breakdown_json={"composite": 6.2},
            evidence_json={"url": "https://jobs.example.com/1", "source_type": "ashby", "source_platform": "ashby", "freshness_days": 1, "listing_status": "active"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)
    assert len(items) == 1
    assert items[0].lead_type == "listing"
    assert items[0].source_platform == "ashby"

    items_with_signals = list_leads(session, include_signal_only=True)
    assert {item.lead_type for item in items_with_signals} == {"listing", "signal"}


def test_lead_response_exposes_source_provenance_and_lineage() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="Agentic Co",
        title="Operations Lead",
        location="Remote, US",
        url="https://job-boards.greenhouse.io/agentic/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Own operating cadence and planning.",
        listing_status="active",
        freshness_hours=3.0,
        freshness_days=0,
        metadata_json={
            "discovery_source": "search_web",
            "surface_provenance": "discovered_new",
            "source_lineage": "greenhouse+search_web",
            "source_board_token": "agentic",
        },
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="Agentic Co",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Agent discovered listing",
            score_breakdown_json={"composite": 7.5},
            evidence_json={"url": listing.url, "source_type": "greenhouse", "source_platform": "greenhouse+search_web"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)

    assert len(items) == 1
    assert items[0].source_provenance == "discovered_new"
    assert items[0].source_lineage == "greenhouse+search_web"
    assert items[0].discovery_source == "search_web"


def test_default_leads_query_suppresses_stale_listing_even_if_lead_snapshot_looks_fresh() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="ArchiveCo",
        title="Chief of Staff",
        location="San Francisco, CA",
        url="https://boards.greenhouse.io/archive/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow() - timedelta(days=45),
        first_published_at=datetime.utcnow() - timedelta(days=45),
        last_seen_at=datetime.utcnow() - timedelta(days=45),
        description_text="This position has been filled.",
        listing_status="expired",
        freshness_hours=45 * 24,
        freshness_days=45,
        metadata_json={"page_text": "job no longer available"},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="ArchiveCo",
            primary_title="Chief of Staff",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Should not surface",
            score_breakdown_json={"composite": 7.0},
            evidence_json={"url": listing.url, "source_type": "greenhouse", "freshness_days": 1, "listing_status": "active"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)
    assert items == []


def test_combined_lead_with_stale_backing_listing_is_hidden_by_default() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="Mercor",
        title="Deployment Strategist",
        location="Remote, US",
        url="https://jobs.ashbyhq.com/Mercor/1",
        source_type="ashby",
        posted_at=datetime.utcnow() - timedelta(days=31),
        first_published_at=datetime.utcnow() - timedelta(days=31),
        last_seen_at=datetime.utcnow() - timedelta(days=31),
        description_text="Posting closed archived position.",
        listing_status="suspected_expired",
        freshness_hours=31 * 24,
        freshness_days=31,
        metadata_json={"page_text": "posting closed archived"},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="combined",
            company_name="Mercor",
            primary_title="Deployment Strategist",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Combined lead",
            score_breakdown_json={"composite": 7.0},
            evidence_json={"url": listing.url, "source_type": "ashby", "freshness_days": 1, "listing_status": "active"},
            hidden=False,
        )
    )
    session.commit()

    assert list_leads(session) == []


def test_critic_is_final_gate_and_marks_non_live_rows_hidden() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="BrokenCo",
        title="Ops Lead",
        location="Remote",
        url="https://jobs.example.com/broken",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Role text",
        listing_status="active",
        freshness_hours=2.0,
        freshness_days=1,
        metadata_json={"http_status": 404},
    )
    session.add(listing)
    session.flush()
    lead = Lead(
        lead_type="listing",
        company_name="BrokenCo",
        primary_title="Ops Lead",
        listing_id=listing.id,
        surfaced_at=datetime.utcnow(),
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        explanation="Broken should hide",
        score_breakdown_json={"composite": 6.0},
        evidence_json={"url": listing.url, "source_type": "greenhouse"},
        hidden=False,
    )
    session.add(lead)
    session.commit()

    run_critic_agent(session)
    session.commit()
    session.refresh(lead)

    assert lead.hidden is True
    assert lead.evidence_json["critic_status"] == "suppressed"
    assert "HTTP 404" in "; ".join(lead.evidence_json["critic_reasons"])


def test_ranker_cannot_force_suppressed_row_into_default_results() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="OldCo",
        title="Operations Lead",
        location="Remote",
        url="https://jobs.example.com/old",
        source_type="greenhouse",
        posted_at=datetime.utcnow() - timedelta(days=50),
        first_published_at=datetime.utcnow() - timedelta(days=50),
        last_seen_at=datetime.utcnow() - timedelta(days=50),
        description_text="Page not found.",
        listing_status="expired",
        freshness_hours=50 * 24,
        freshness_days=50,
        metadata_json={"page_text": "page not found"},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="OldCo",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Ranked strongly but should hide",
            score_breakdown_json={"composite": 9.0},
            evidence_json={"url": listing.url, "source_type": "greenhouse"},
            hidden=False,
        )
    )
    session.commit()

    items = list_leads(session)
    assert items == []


def test_default_leads_query_returns_timestamp_precision_and_recently_seen_rows_only() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    now = datetime.utcnow()
    listing = Listing(
        company_name="CurrentCo",
        title="Deployment Strategist",
        location="Remote",
        url="https://jobs.example.com/current",
        source_type="greenhouse",
        posted_at=now - timedelta(hours=6, minutes=15),
        first_published_at=now - timedelta(hours=6, minutes=15),
        discovered_at=now - timedelta(hours=5, minutes=30),
        last_seen_at=now - timedelta(minutes=10),
        description_text="Run customer deployments for an AI startup.",
        listing_status="active",
        freshness_hours=6.25,
        freshness_days=0,
        metadata_json={},
    )
    stale_listing = Listing(
        company_name="MissingCo",
        title="Operations Lead",
        location="Remote",
        url="https://jobs.example.com/missing",
        source_type="greenhouse",
        posted_at=now - timedelta(hours=5),
        first_published_at=now - timedelta(hours=5),
        discovered_at=now - timedelta(hours=5),
        last_seen_at=now - timedelta(hours=72),
        description_text="This row should be suppressed because it has not been seen recently.",
        listing_status="active",
        freshness_hours=5.0,
        freshness_days=0,
        metadata_json={},
    )
    session.add_all([listing, stale_listing])
    session.flush()
    session.add_all(
        [
            Lead(
                lead_type="listing",
                company_name="CurrentCo",
                primary_title="Deployment Strategist",
                listing_id=listing.id,
                surfaced_at=now,
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="core match",
                qualification_fit_label="strong fit",
                explanation="Visible",
                score_breakdown_json={"composite": 8.0},
                evidence_json={"url": listing.url, "source_type": "greenhouse"},
                hidden=False,
            ),
            Lead(
                lead_type="listing",
                company_name="MissingCo",
                primary_title="Operations Lead",
                listing_id=stale_listing.id,
                surfaced_at=now,
                rank_label="strong",
                confidence_label="high",
                freshness_label="fresh",
                title_fit_label="core match",
                qualification_fit_label="strong fit",
                explanation="Should not surface",
                score_breakdown_json={"composite": 8.0},
                evidence_json={"url": stale_listing.url, "source_type": "greenhouse"},
                hidden=False,
            ),
        ]
    )
    session.commit()

    items = list_leads(session)
    assert len(items) == 1
    item = items[0]
    assert item.company_name == "CurrentCo"
    assert item.freshness_hours is not None
    assert item.freshness_hours > 6
    assert item.posted_at is not None and item.posted_at.second != 0
    assert item.posted_at.tzinfo is not None
    assert item.first_published_at is not None and item.first_published_at.second != 0
    assert item.first_published_at.tzinfo is not None
    assert item.last_seen_at is not None
    assert item.last_seen_at.tzinfo is not None
    assert item.evidence_json["first_published_at"].endswith("Z")
    assert item.evidence_json["last_seen_at"].endswith("Z")


def test_default_leads_query_suppresses_out_of_region_listing() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    _seed_profile(session)

    listing = Listing(
        company_name="LondonCo",
        title="Operations Lead",
        location="London, UK",
        url="https://job-boards.greenhouse.io/londonco/jobs/1",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        first_published_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        description_text="Run strategic operations for an AI startup.",
        listing_status="active",
        freshness_hours=3.0,
        freshness_days=0,
        metadata_json={"location_scope": "uk", "location_reason": "matched region hint uk"},
    )
    session.add(listing)
    session.flush()
    session.add(
        Lead(
            lead_type="listing",
            company_name="LondonCo",
            primary_title="Operations Lead",
            listing_id=listing.id,
            surfaced_at=datetime.utcnow(),
            rank_label="strong",
            confidence_label="high",
            freshness_label="fresh",
            title_fit_label="core match",
            qualification_fit_label="strong fit",
            explanation="Should be filtered by location policy",
            score_breakdown_json={"composite": 8.0},
            evidence_json={"url": listing.url, "source_type": "greenhouse"},
            hidden=False,
        )
    )
    session.commit()

    assert list_leads(session) == []
