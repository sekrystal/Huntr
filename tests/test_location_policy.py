from __future__ import annotations

from core.schemas import CandidateProfilePayload
from services.location_policy import classify_location_scope, is_location_allowed_for_profile


class DummyProfile:
    def __init__(self) -> None:
        payload = CandidateProfilePayload(
            preferred_locations_json=["remote", "san francisco", "new york"],
        )
        for key, value in payload.model_dump().items():
            setattr(self, key, value)


def test_classify_location_scope_handles_non_us_regions() -> None:
    assert classify_location_scope("London, UK")["scope"] == "uk"
    assert classify_location_scope("Remote, Europe")["scope"] == "remote_global"
    assert classify_location_scope("Toronto, Canada")["scope"] == "canada"


def test_location_policy_blocks_non_us_and_flags_ambiguous() -> None:
    profile = DummyProfile()
    blocked = is_location_allowed_for_profile(profile, "London, UK")
    ambiguous = is_location_allowed_for_profile(profile, "Hybrid")
    generic_remote = is_location_allowed_for_profile(profile, "Remote")
    allowed = is_location_allowed_for_profile(profile, "Remote, US")

    assert blocked["allowed"] is False
    assert blocked["status"] == "blocked"
    assert ambiguous["allowed"] is False
    assert ambiguous["status"] == "uncertain"
    assert generic_remote["allowed"] is True
    assert allowed["allowed"] is True
    assert allowed["scope"] == "remote_us"


def test_location_policy_blocks_ireland_bangalore_and_australia() -> None:
    profile = DummyProfile()
    assert is_location_allowed_for_profile(profile, "Dublin, Ireland")["allowed"] is False
    assert is_location_allowed_for_profile(profile, "Bangalore, India")["allowed"] is False
    assert is_location_allowed_for_profile(profile, "Sydney, Australia")["allowed"] is False
