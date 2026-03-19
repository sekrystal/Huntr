from __future__ import annotations

import re
from typing import Optional

from core.models import CandidateProfile


ROLE_FAMILY_KEYWORDS = {
    "operations": ["ops", "operations", "chief of staff", "bizops", "program", "strategic"],
    "go_to_market": ["deployment", "implementation", "customer", "solutions", "growth"],
    "product": ["product", "pm", "technical product", "program"],
    "engineering": ["engineer", "infrastructure", "platform", "ai infra"],
}
SENIORITY_ORDER = {"entry": 0, "junior": 1, "mid": 2, "senior": 3, "staff": 4, "executive": 5}
TITLE_HARD_FILTERS = ["intern", "new grad", "ceo", "founder", "principal scientist", "rocket propulsion engineer"]
QUALIFICATION_SPECIALIZATIONS = ["rocket propulsion", "specialized hardware", "principal scientist", "bar admission"]


def infer_role_family(title: str, description_text: str = "") -> str:
    lowered = f"{title} {description_text}".lower()
    for family, keywords in ROLE_FAMILY_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return family
    return "generalist"


def infer_seniority_band(title: str, description_text: str = "") -> str:
    lowered = f"{title} {description_text}".lower()
    years = [int(item) for item in re.findall(r"(\d+)\+?\s+years", lowered)]
    if "chief of staff" in lowered:
        return "senior"
    if "founding" in lowered or "architect" in lowered:
        return "mid"
    if re.search(r"\bintern\b|\bnew grad\b|\bcampus\b|\bentry level\b", lowered):
        return "entry"
    if years and max(years) <= 2:
        return "junior"
    if years and max(years) <= 5:
        return "mid"
    if years and max(years) <= 8:
        return "senior"
    if years and max(years) > 8:
        return "staff"
    if any(keyword in lowered for keyword in ["director", "head of", "lead"]):
        return "senior"
    if any(keyword in lowered for keyword in ["vp", "chief", "founder", "ceo"]):
        return "executive"
    return "mid"


def classify_title_fit(profile: CandidateProfile, title: str, description_text: str = "") -> tuple[str, float, list[str]]:
    title_lower = title.lower()
    description_lower = description_text.lower()
    matched_fields: list[str] = []

    if any(excluded.lower() in title_lower for excluded in profile.excluded_titles_json or []):
        return "excluded", -10.0, ["excluded title"]

    core_titles = [item.lower() for item in (profile.core_titles_json or profile.preferred_titles_json or [])]
    adjacent_titles = [item.lower() for item in (profile.adjacent_titles_json or [])]

    if any(core in title_lower for core in core_titles):
        matched_fields.append("core title")
        return "core match", 2.4, matched_fields
    if any(adjacent in title_lower for adjacent in adjacent_titles):
        matched_fields.append("adjacent title")
        return "adjacent match", 1.5, matched_fields

    scope_keywords = set()
    for item in core_titles + adjacent_titles:
        scope_keywords.update(item.split())
    scope_keywords.update(["systems", "operating", "operations", "cadence", "planning", "deployment", "customer"])
    overlap = [word for word in scope_keywords if len(word) > 3 and word in description_lower]
    if len(overlap) >= 2:
        matched_fields.append("scope match")
        return "unexpected but plausible", 1.1, matched_fields

    return "weak title match", 0.2, matched_fields


def classify_qualification_fit(profile: CandidateProfile, title: str, description_text: str = "") -> tuple[str, float, list[str]]:
    title_lower = title.lower()
    description_lower = description_text.lower()
    reasons: list[str] = []
    seniority_band = infer_seniority_band(title, description_text)
    role_family = infer_role_family(title, description_text)
    min_band = SENIORITY_ORDER.get(profile.min_seniority_band, 2)
    max_band = SENIORITY_ORDER.get(profile.max_seniority_band, 3)
    lead_band = SENIORITY_ORDER.get(seniority_band, 2)

    if any(pattern in f"{title_lower} {description_lower}" for pattern in QUALIFICATION_SPECIALIZATIONS):
        return "underqualified", -3.0, ["specialized requirement mismatch"]
    if any(pattern in title_lower for pattern in TITLE_HARD_FILTERS):
        if any(pattern in title_lower for pattern in ["intern", "new grad"]):
            return "overqualified", -2.5, ["role is clearly junior"]
        return "underqualified", -3.0, ["title implies unrealistic qualification gap"]
    if lead_band < min_band:
        return "overqualified", -1.8, ["seniority below candidate floor"]
    if lead_band > max_band:
        if role_family in (profile.stretch_role_families_json or []):
            return "stretch", 0.4, ["above normal seniority but in stretch family"]
        return "underqualified", -2.0, ["seniority above candidate band"]
    if "phd required" in description_lower or "board certification" in description_lower:
        return "underqualified", -2.4, ["credential requirement mismatch"]
    return "strong fit", 1.0, reasons


def score_lead(
    profile: CandidateProfile,
    lead_type: str,
    title: str,
    company_name: str,
    company_domain: Optional[str],
    location: Optional[str],
    description_text: str,
    freshness_label: str,
    listing_status: Optional[str],
    source_type: str,
    evidence_count: int,
    feedback_learning: Optional[dict] = None,
) -> dict:
    feedback_learning = feedback_learning or {}
    role_family = infer_role_family(title, description_text)
    title_fit_label, title_fit_score, matched_title_fields = classify_title_fit(profile, title, description_text)
    qualification_fit_label, qualification_score, qualification_reasons = classify_qualification_fit(profile, title, description_text)

    freshness_score = {"fresh": 1.6, "recent": 1.0, "stale": -1.2, "unknown": -0.5}[freshness_label]
    source_quality = {"greenhouse": 1.2, "ashby": 1.2, "x": 0.5, "x_signal": 0.5}.get(source_type, 0.6)
    evidence_quality = min(0.4 * max(evidence_count, 1), 1.2)
    novelty = 0.5 if lead_type in {"signal", "combined"} else 0.2
    location_fit = 1.0 if location and any(item.lower() in location.lower() for item in (profile.preferred_locations_json or [])) else 0.0
    domain_fit = 0.9 if company_domain and any(item.lower() in company_domain.lower() for item in (profile.preferred_domains_json or [])) else 0.0
    stage_fit = 0.5 if any(stage in description_text.lower() for stage in (profile.stage_preferences_json or [])) else 0.0
    role_family_fit = 0.8 if role_family in {"operations", "go_to_market"} else 0.3
    negative_signals = 0.0

    if listing_status in {"expired", "suspected_expired"}:
        negative_signals -= 3.0
    if lead_type in {"listing", "combined"} and freshness_label == "unknown":
        negative_signals -= 1.2
    if lead_type == "signal":
        negative_signals -= 0.2
    if company_name.lower() in [item.lower() for item in (profile.excluded_companies_json or [])]:
        negative_signals -= 4.0
    if any(keyword.lower() in f"{title.lower()} {description_text.lower()}" for keyword in (profile.excluded_keywords_json or [])):
        negative_signals -= 3.5

    title_weights = feedback_learning.get("title_weights", {})
    role_family_weights = feedback_learning.get("role_family_weights", {})
    domain_weights = feedback_learning.get("domain_weights", {})
    source_penalties = feedback_learning.get("source_penalties", {})
    title_feedback = title_weights.get(title.lower(), 0.0)
    role_family_feedback = role_family_weights.get(role_family, 0.0)
    domain_feedback = domain_weights.get((company_domain or "").lower(), 0.0)
    source_feedback = -source_penalties.get(source_type, 0.0)

    composite = round(
        freshness_score
        + title_fit_score
        + role_family_fit
        + domain_fit
        + location_fit
        + stage_fit
        + source_quality
        + evidence_quality
        + novelty
        + qualification_score
        + title_feedback
        + role_family_feedback
        + domain_feedback
        + source_feedback
        + negative_signals,
        2,
    )

    rank_label = "strong" if composite >= 5.5 else "medium" if composite >= max(profile.minimum_fit_threshold, 3.0) else "weak"
    confidence_components = [
        source_quality,
        evidence_quality,
        0.8 if lead_type == "combined" else 0.4,
        0.7 if listing_status == "active" else 0.0,
        -0.4 if freshness_label == "unknown" else 0.0,
    ]
    confidence_total = sum(confidence_components)
    confidence_label = "high" if confidence_total >= 2.6 else "medium" if confidence_total >= 1.4 else "low"

    return {
        "composite": composite,
        "freshness": round(freshness_score, 2),
        "title_fit": round(title_fit_score, 2),
        "role_family_fit": round(role_family_fit, 2),
        "domain_fit": round(domain_fit, 2),
        "location_fit": round(location_fit, 2),
        "stage_company_fit": round(stage_fit, 2),
        "source_quality": round(source_quality, 2),
        "evidence_quality": round(evidence_quality, 2),
        "novelty": round(novelty, 2),
        "negative_signals": round(negative_signals, 2),
        "feedback_title_boost": round(title_feedback, 2),
        "feedback_role_family_boost": round(role_family_feedback, 2),
        "feedback_domain_boost": round(domain_feedback, 2),
        "feedback_source_penalty": round(source_feedback, 2),
        "rank_label": rank_label,
        "confidence_label": confidence_label,
        "freshness_label": freshness_label,
        "title_fit_label": title_fit_label,
        "qualification_fit_label": qualification_fit_label,
        "matched_profile_fields": matched_title_fields + qualification_reasons,
        "role_family": role_family,
    }
