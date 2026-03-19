from __future__ import annotations

from typing import Optional


def build_explanation(
    lead_type: str,
    matched_profile_fields: list[str],
    feedback_notes: list[str],
    freshness_label: str,
    confidence_label: str,
    uncertainty: Optional[str] = None,
) -> str:
    matched = ", ".join(matched_profile_fields[:3]) if matched_profile_fields else "limited direct profile matches"
    feedback = ", ".join(feedback_notes[:2]) if feedback_notes else "no strong feedback adjustments yet"
    uncertainty_text = f" Uncertainty: {uncertainty}." if uncertainty else ""
    return (
        f"Surfaced as a {lead_type} lead. Matched profile fields: {matched}. "
        f"Feedback influence: {feedback}. Freshness is {freshness_label} and confidence is {confidence_label}."
        f"{uncertainty_text}"
    )
