from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from core.schemas import ListingRecord


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_greenhouse_job(job: dict) -> ListingRecord:
    content = job.get("content", "") or ""
    location = (job.get("location") or {}).get("name")
    first_published_at = _parse_datetime(job.get("first_published"))
    created_at = _parse_datetime(job.get("created_at"))
    updated_at = _parse_datetime(job.get("updated_at"))
    return ListingRecord(
        company_name=job.get("company_name") or "Unknown Company",
        company_domain=job.get("company_domain"),
        careers_url=job.get("absolute_url"),
        title=(job.get("title") or "").strip(),
        location=location,
        url=job.get("absolute_url") or job.get("url"),
        source_type="greenhouse",
        posted_at=first_published_at or created_at or updated_at,
        first_published_at=first_published_at,
        last_seen_at=datetime.now(timezone.utc),
        description_text=content,
        metadata_json={
            "provider": "greenhouse",
            "page_text": job.get("page_text", ""),
            "source_queries": job.get("source_queries", []),
            "discovery_source": job.get("discovery_source"),
            "company_domain": job.get("company_domain"),
            "source_board_token": job.get("source_board_token"),
            "internal_job_id": job.get("internal_job_id"),
            "live_quality": job.get("live_quality", "unknown"),
            "source_updated_at": job.get("updated_at"),
            "source_created_at": job.get("created_at"),
        },
    )


def normalize_ashby_job(job: dict, org_name: Optional[str] = None) -> ListingRecord:
    description = job.get("descriptionPlain") or job.get("descriptionHtml") or ""
    location = None
    if job.get("location"):
        location = job["location"].get("location") or job["location"].get("name")
    published_at = _parse_datetime(job.get("publishedDate"))
    updated_at = _parse_datetime(job.get("updatedAt"))

    return ListingRecord(
        company_name=job.get("companyName") or org_name or "Unknown Company",
        company_domain=job.get("companyDomain"),
        careers_url=job.get("jobUrl") or job.get("applyUrl"),
        title=(job.get("title") or "").strip(),
        location=location,
        url=job.get("jobUrl") or job.get("applyUrl"),
        source_type="ashby",
        posted_at=published_at or updated_at,
        first_published_at=published_at,
        last_seen_at=datetime.now(timezone.utc),
        description_text=description,
        metadata_json={
            "provider": "ashby",
            "page_text": job.get("page_text", ""),
            "source_queries": job.get("source_queries", []),
            "discovery_source": job.get("discovery_source"),
            "company_domain": job.get("companyDomain"),
            "source_org_key": job.get("source_org_key"),
            "source_updated_at": job.get("updatedAt"),
        },
    )
