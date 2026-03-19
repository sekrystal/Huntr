from __future__ import annotations

from datetime import datetime
from typing import Optional

from core.schemas import ListingRecord


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_greenhouse_job(job: dict) -> ListingRecord:
    content = job.get("content", "") or ""
    location = (job.get("location") or {}).get("name")
    return ListingRecord(
        company_name=job.get("company_name") or "Unknown Company",
        company_domain=job.get("company_domain"),
        careers_url=job.get("absolute_url"),
        title=(job.get("title") or "").strip(),
        location=location,
        url=job.get("absolute_url") or job.get("url"),
        source_type="greenhouse",
        posted_at=_parse_datetime(job.get("first_published") or job.get("updated_at") or job.get("created_at")),
        description_text=content,
        metadata_json={
            "provider": "greenhouse",
            "page_text": job.get("page_text", ""),
            "source_queries": job.get("source_queries", []),
            "company_domain": job.get("company_domain"),
            "source_board_token": job.get("source_board_token"),
            "internal_job_id": job.get("internal_job_id"),
            "live_quality": job.get("live_quality", "unknown"),
        },
    )


def normalize_ashby_job(job: dict, org_name: Optional[str] = None) -> ListingRecord:
    description = job.get("descriptionPlain") or job.get("descriptionHtml") or ""
    location = None
    if job.get("location"):
        location = job["location"].get("location") or job["location"].get("name")

    return ListingRecord(
        company_name=job.get("companyName") or org_name or "Unknown Company",
        company_domain=job.get("companyDomain"),
        careers_url=job.get("jobUrl") or job.get("applyUrl"),
        title=(job.get("title") or "").strip(),
        location=location,
        url=job.get("jobUrl") or job.get("applyUrl"),
        source_type="ashby",
        posted_at=_parse_datetime(job.get("publishedDate") or job.get("updatedAt")),
        description_text=description,
        metadata_json={
            "provider": "ashby",
            "page_text": job.get("page_text", ""),
            "source_queries": job.get("source_queries", []),
            "company_domain": job.get("companyDomain"),
        },
    )
