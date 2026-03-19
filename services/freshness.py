from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from core.schemas import ListingRecord


EXPIRED_PATTERNS = [
    "job no longer available",
    "position has been filled",
    "position filled",
    "page not found",
    "no longer accepting applications",
    "archived",
    "posting closed",
]


def has_expired_pattern(*parts: str | None) -> bool:
    haystack = " ".join(part or "" for part in parts).lower()
    return any(pattern in haystack for pattern in EXPIRED_PATTERNS)


def compute_freshness_hours(posted_at: Optional[datetime]) -> Optional[float]:
    if not posted_at:
        return None
    current = datetime.now(timezone.utc)
    posted = posted_at if posted_at.tzinfo else posted_at.replace(tzinfo=timezone.utc)
    return round(max((current - posted).total_seconds() / 3600, 0.0), 2)


def compute_freshness_days(posted_at: Optional[datetime]) -> Optional[int]:
    freshness_hours = compute_freshness_hours(posted_at)
    if freshness_hours is None:
        return None
    return int(freshness_hours // 24)


def classify_freshness_label(freshness_days: Optional[int], freshness_hours: Optional[float] = None) -> str:
    if freshness_days is None and freshness_hours is None:
        return "unknown"
    if freshness_hours is None:
        freshness_hours = freshness_days * 24
    if freshness_hours <= 72:
        return "fresh"
    if freshness_hours <= 14 * 24:
        return "recent"
    return "stale"


def validate_listing(record: ListingRecord) -> ListingRecord:
    text = f"{record.description_text or ''} {(record.metadata_json or {}).get('page_text', '')}".lower()
    freshness_hours = compute_freshness_hours(record.posted_at)
    freshness_days = compute_freshness_days(record.posted_at)
    listing_status = "active" if freshness_hours is not None else "unknown"
    expiration_confidence = 0.05

    if has_expired_pattern(text):
        listing_status = "expired"
        expiration_confidence = 0.98

    if listing_status != "expired" and freshness_hours is not None and freshness_hours > 30 * 24:
        listing_status = "suspected_expired"
        expiration_confidence = 0.7

    if record.metadata_json.get("http_status") in {404, 410}:
        listing_status = "expired"
        expiration_confidence = 0.99

    record.freshness_hours = freshness_hours
    record.freshness_days = freshness_days
    record.listing_status = listing_status
    record.expiration_confidence = max(record.expiration_confidence, expiration_confidence)
    return record
