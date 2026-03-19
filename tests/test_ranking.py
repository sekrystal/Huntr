from __future__ import annotations

from core.schemas import CandidateProfilePayload
from services.ranking import classify_qualification_fit, classify_title_fit


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

