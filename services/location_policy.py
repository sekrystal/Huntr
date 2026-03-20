from __future__ import annotations

import re
from typing import Optional

from core.config import Settings, get_settings


US_STATES = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
    "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
    "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
}
US_CITY_HINTS = {"san francisco", "new york", "nyc", "bay area", "los angeles", "austin", "seattle", "boston", "chicago"}
NON_US_HINTS = {
    "emea": "emea",
    "europe": "emea",
    "ireland": "uk",
    "dublin": "uk",
    "united kingdom": "uk",
    " uk ": "uk",
    "london": "uk",
    "uki": "uki",
    "apac": "apac",
    "asia pacific": "apac",
    "singapore": "sea",
    "bangalore": "apac",
    "india": "apac",
    "southeast asia": "sea",
    "sea ": "sea",
    "latam": "latam",
    "latin america": "latam",
    "cdmx": "cdmx",
    "mexico city": "cdmx",
    "canada": "canada",
    "toronto": "canada",
    "vancouver": "canada",
    "australia": "apac",
    "sydney": "apac",
    "melbourne": "apac",
}


def _normalize(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def classify_location_scope(location: Optional[str]) -> dict:
    normalized = _normalize(location)
    if not normalized:
        return {"scope": "ambiguous", "normalized_location": normalized, "reason": "missing location"}

    remote = "remote" in normalized or "anywhere" in normalized or "distributed" in normalized
    if remote and re.search(r"\b(us only|united states|usa|u\.s\.|u\.s\.a\.|us)\b", normalized):
        return {"scope": "remote_us", "normalized_location": normalized, "reason": "remote restricted to us"}

    for hint, scope in NON_US_HINTS.items():
        if hint in f" {normalized} ":
            if remote:
                return {"scope": "remote_global", "normalized_location": normalized, "reason": f"remote and non-us region hint {scope}"}
            return {"scope": scope, "normalized_location": normalized, "reason": f"matched region hint {scope}"}

    if re.search(r"\b(remote|anywhere|distributed)\b", normalized):
        return {"scope": "remote_global", "normalized_location": normalized, "reason": "remote without explicit us restriction"}

    if any(city in normalized for city in US_CITY_HINTS):
        return {"scope": "us", "normalized_location": normalized, "reason": "matched us city hint"}

    if re.search(r"\b(united states|usa|u\.s\.|u\.s\.a\.)\b", normalized):
        return {"scope": "us", "normalized_location": normalized, "reason": "matched explicit us country hint"}

    state_match = re.search(r",\s*([a-z]{2})\b", normalized)
    if state_match and state_match.group(1) in US_STATES:
        return {"scope": "us", "normalized_location": normalized, "reason": "matched us state abbreviation"}

    if any(token in normalized for token in ["europe", "germany", "france", "spain", "netherlands", "poland", "india", "japan", "australia", "brazil", "mexico", "ireland", "london", "bangalore", "singapore"]):
        return {"scope": "non_us", "normalized_location": normalized, "reason": "matched non-us geography hint"}

    return {"scope": "ambiguous", "normalized_location": normalized, "reason": "location could not be classified confidently"}


def is_location_allowed_for_profile(profile, location: Optional[str], settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    classification = classify_location_scope(location)
    scope = classification["scope"]
    preferred_locations = [item.lower() for item in (getattr(profile, "preferred_locations_json", None) or [])]

    if scope in settings.allowed_location_scope_list:
        return {"allowed": True, "status": "allowed", "scope": scope, "reason": classification["reason"]}

    if scope == "remote_global":
        if classification["reason"] == "remote without explicit us restriction" and "remote" in preferred_locations:
            return {"allowed": True, "status": "allowed", "scope": scope, "reason": "generic remote role accepted because remote is in profile preferences"}
        if settings.allow_remote_global:
            return {"allowed": True, "status": "allowed", "scope": scope, "reason": "remote global explicitly allowed"}
        return {"allowed": False, "status": "blocked", "scope": scope, "reason": "remote global roles are outside allowed region policy"}

    if scope == "ambiguous":
        if settings.allow_ambiguous_locations:
            return {"allowed": True, "status": "allowed", "scope": scope, "reason": "ambiguous locations explicitly allowed"}
        if any(item in {"remote", "united states", "usa", "us"} for item in preferred_locations):
            return {"allowed": False, "status": "uncertain", "scope": scope, "reason": "location is ambiguous and needs explicit review"}
        return {"allowed": False, "status": "uncertain", "scope": scope, "reason": "location is ambiguous"}

    if scope in {"uk", "uki", "emea", "apac", "sea", "cdmx", "latam", "canada", "non_us"}:
        if any(scope in item for item in preferred_locations):
            return {"allowed": True, "status": "allowed", "scope": scope, "reason": f"profile explicitly includes {scope}"}
        return {"allowed": False, "status": "blocked", "scope": scope, "reason": f"{scope} is outside allowed region policy"}

    return {"allowed": False, "status": "uncertain", "scope": scope, "reason": "location policy could not classify this role as eligible"}
