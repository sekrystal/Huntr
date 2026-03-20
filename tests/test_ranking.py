from __future__ import annotations

from core.schemas import CandidateProfilePayload
from services.ranking import classify_qualification_fit, classify_title_fit, score_lead


class DummyProfile:
    def __init__(self) -> None:
        payload = CandidateProfilePayload(
            core_titles_json=["chief of staff", "founding operations lead"],
            adjacent_titles_json=["business operations", "implementation lead"],
            excluded_titles_json=["intern"],
            excluded_keywords_json=["rocket propulsion"],
            min_seniority_band="mid",
            max_seniority_band="staff",
            stretch_role_families_json=["go_to_market"],
        )
        for key, value in payload.model_dump().items():
            setattr(self, key, value)


def test_weird_but_relevant_title_gets_adjacent_or_scope_match() -> None:
    profile = DummyProfile()
    label, score, _ = classify_title_fit(
        profile,
        title="Business Rhythm Architect",
        description_text="Own planning cadences, internal systems, and cross-functional operating rhythm.",
    )
    assert label in {"adjacent match", "unexpected but plausible"}
    assert score > 0.5


def test_specialized_role_is_underqualified() -> None:
    profile = DummyProfile()
    label, score, reasons = classify_qualification_fit(
        profile,
        title="Rocket Propulsion Engineer",
        description_text="Design propulsion systems and specialized hardware.",
    )
    assert label == "underqualified"
    assert score < 0
    assert reasons


def test_go_to_market_title_defaults_to_stretch_without_strong_title_signal() -> None:
    profile = DummyProfile()
    label, score, reasons = classify_qualification_fit(
        profile,
        title="Deployment Strategist",
        description_text="Help customers deploy product across a fast-growing startup.",
    )
    assert label == "stretch"
    assert score == 0.4
    assert reasons


def test_feedback_boosts_are_capped_and_do_not_exceed_total_positive_limit() -> None:
    profile = DummyProfile()
    breakdown = score_lead(
        profile=profile,
        lead_type="combined",
        title="Deployment Strategist",
        company_name="Mercor",
        company_domain="mercor.ai",
        location="Remote",
        description_text="Deployment work for an early-stage startup customer team.",
        freshness_label="fresh",
        listing_status="active",
        source_type="ashby",
        evidence_count=2,
        feedback_learning={
            "title_weights": {"deployment strategist": 1.2},
            "role_family_weights": {"go_to_market": 0.8},
            "domain_weights": {"mercor.ai": 0.5},
        },
    )
    total_positive_feedback = (
        breakdown["feedback_title_boost"]
        + breakdown["feedback_role_family_boost"]
        + breakdown["feedback_domain_boost"]
    )
    assert round(total_positive_feedback, 2) == 1.5


def test_strong_rank_threshold_now_requires_more_than_73() -> None:
    profile = DummyProfile()
    breakdown = score_lead(
        profile=profile,
        lead_type="listing",
        title="Strategic Programs Lead",
        company_name="Ramp",
        company_domain=None,
        location="Remote",
        description_text="Own operating cadence and planning for customers.",
        freshness_label="fresh",
        listing_status="active",
        source_type="greenhouse",
        evidence_count=1,
        feedback_learning={},
    )
    assert breakdown["composite"] == 5.7
    assert breakdown["rank_label"] == "medium"
