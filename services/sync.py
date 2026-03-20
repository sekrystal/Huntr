from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import partial
from collections import defaultdict
from collections import Counter
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from connectors.ashby import AshbyConnector
from connectors.greenhouse import GreenhouseConnector
from connectors.search_web import (
    SearchDiscoveryConnector,
    build_search_queries,
    extract_discovered_ashby_orgs,
    extract_discovered_greenhouse_tokens,
)
from connectors.x_search import XSearchConnector
from core.config import get_settings
from core.logging import get_logger
from core.models import Application, Investigation, Lead, Listing, RecheckQueue, Signal, SourceQuery, WatchlistItem
from core.schemas import ListingRecord, LeadResponse, SignalRecord, StatsResponse, SyncResult
from services.activity import append_lead_agent_trace
from services.ai_judges import judge_critic_with_ai, judge_fit_with_ai
from services.connectors_health import run_connector_fetch
from services.explain import build_explanation
from services.extract_signal import extract_many
from services.freshness import classify_freshness_label, has_expired_pattern, validate_listing
from services.investigations import mark_investigation_attempt, upsert_investigation
from services.learning import generate_follow_up_tasks, increment_query_stat, next_action_for_application
from services.normalize import normalize_ashby_job, normalize_greenhouse_job
from services.profile import get_candidate_profile
from services.query_learning import ensure_source_queries
from services.ranking import infer_role_family, score_lead
from services.resolve_company import get_or_create_company, queue_recheck, resolve_company_name


logger = get_logger(__name__)


def _source_learning(profile) -> dict:
    return (profile.extracted_summary_json or {}).get("learning", {})


def _ensure_utc_datetime(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _isoformat_utc(value):
    normalized = _ensure_utc_datetime(value)
    if not normalized:
        return None
    return normalized.isoformat().replace("+00:00", "Z")


def _verify_listing_record(record: ListingRecord) -> bool:
    if not record:
        return False
    if not record.url:
        return False
    external_id = (record.metadata_json or {}).get("internal_job_id")
    if not external_id:
        return False
    lowered_url = record.url.lower()
    if "greenhouse.io" not in lowered_url and "ashbyhq.com" not in lowered_url:
        return False
    return True


def _verify_signal_record(record: SignalRecord) -> bool:
    if not record:
        return False
    if not record.source_url:
        return False
    if not record.raw_text:
        return False
    return True


def _upsert_signal(session: Session, record: SignalRecord) -> Signal:
    existing = session.scalar(select(Signal).where(Signal.source_url == record.source_url))
    payload = record.model_dump(exclude={"metadata_json"})
    payload["signal_status"] = payload.get("signal_status") or "new"
    if existing:
        for key, value in payload.items():
            setattr(existing, key, value)
        return existing
    signal = Signal(**payload)
    session.add(signal)
    session.flush()
    return signal


def _upsert_listing(session: Session, record: ListingRecord, company_id: Optional[int]) -> tuple[Listing, bool]:
    existing = session.scalar(select(Listing).where(Listing.url == record.url))
    payload = record.model_dump()
    payload["company_id"] = company_id
    metadata = dict(payload.get("metadata_json") or {})
    if payload.get("company_domain"):
        metadata["company_domain"] = payload["company_domain"]
    if payload.get("careers_url"):
        metadata["careers_url"] = payload["careers_url"]
    payload["metadata_json"] = metadata
    payload.pop("company_domain", None)
    payload.pop("careers_url", None)
    if existing:
        material_changed = False
        for key, value in payload.items():
            if value is not None and getattr(existing, key) != value:
                setattr(existing, key, value)
                material_changed = True
        if existing.last_seen_at != payload.get("last_seen_at"):
            existing.last_seen_at = payload.get("last_seen_at") or datetime.now(timezone.utc)
            material_changed = True
        if material_changed:
            existing.updated_at = datetime.utcnow()
        return existing, False
    listing = Listing(**payload)
    session.add(listing)
    session.flush()
    return listing, True


def _query_stats_increment(session: Session, query_texts: list[str], delta: int = 1) -> None:
    for query_text in query_texts:
        item = session.scalar(
            select(SourceQuery).where(SourceQuery.query_text == query_text, SourceQuery.source_type == "x")
        )
        if not item:
            continue
        stats = dict(item.performance_stats_json or {})
        stats["leads_generated"] = stats.get("leads_generated", 0) + delta
        item.performance_stats_json = stats
        increment_query_stat(session, source_type="x", query_text=query_text, field_name="leads_generated", delta=delta)


def _matching_listing_for_signal(listings: list[Listing], signal: Signal) -> Optional[Listing]:
    for listing in listings:
        if listing.company_name.lower() != (signal.company_guess or "").lower():
            continue
        if listing.listing_status != "active":
            continue
        signal_role = (signal.role_guess or "").lower()
        if signal_role and signal_role in listing.title.lower():
            return listing
        if infer_role_family(listing.title, listing.description_text or "") == infer_role_family(signal.role_guess or "", signal.raw_text):
            return listing
    return None


def _authoritative_listing_context(session: Session, lead: Lead) -> dict:
    evidence = dict(lead.evidence_json or {})
    listing = session.get(Listing, lead.listing_id) if lead.listing_id else None
    page_text = ""
    http_status = None
    if listing:
        metadata = dict(listing.metadata_json or {})
        page_text = metadata.get("page_text", "")
        http_status = metadata.get("http_status")
    else:
        page_text = evidence.get("page_text", "")
        http_status = evidence.get("http_status")
    return {
        "listing": listing,
        "url": (listing.url if listing else evidence.get("url")),
        "posted_at": _ensure_utc_datetime(listing.posted_at) if listing else _ensure_utc_datetime(evidence.get("posted_at")),
        "first_published_at": _ensure_utc_datetime(listing.first_published_at) if listing else _ensure_utc_datetime(evidence.get("first_published_at")),
        "discovered_at": _ensure_utc_datetime(listing.discovered_at) if listing else _ensure_utc_datetime(evidence.get("discovered_at")),
        "last_seen_at": _ensure_utc_datetime(listing.last_seen_at) if listing else _ensure_utc_datetime(evidence.get("last_seen_at")),
        "updated_at": _ensure_utc_datetime(listing.updated_at) if listing else _ensure_utc_datetime(evidence.get("updated_at")),
        "freshness_hours": listing.freshness_hours if listing else evidence.get("freshness_hours"),
        "freshness_days": listing.freshness_days if listing else evidence.get("freshness_days"),
        "listing_status": listing.listing_status if listing else evidence.get("listing_status"),
        "expiration_confidence": listing.expiration_confidence if listing else evidence.get("expiration_confidence", 0.0),
        "description_text": listing.description_text if listing else "",
        "page_text": page_text or "",
        "http_status": http_status,
    }


def _duplicate_winner_context(session: Session, leads: list[Lead]) -> dict[int, str]:
    freshness_order = {"fresh": 0, "recent": 1, "stale": 2, "unknown": 3}
    lead_type_order = {"combined": 0, "listing": 1, "signal": 2}
    duplicate_groups: dict[tuple, list[Lead]] = defaultdict(list)

    for lead in leads:
        context = _authoritative_listing_context(session, lead)
        dedupe_key = (
            lead.listing_id or None,
            (context["url"] or "").lower(),
            lead.company_name.lower(),
            lead.primary_title.lower(),
        )
        duplicate_groups[dedupe_key].append(lead)

    losers: dict[int, str] = {}
    for grouped in duplicate_groups.values():
        if len(grouped) <= 1:
            continue
        ordered = sorted(
            grouped,
            key=lambda lead: (
                lead_type_order.get(lead.lead_type, 9),
                freshness_order.get(lead.freshness_label, 9),
                -int(lead.updated_at.timestamp()) if lead.updated_at else 0,
                lead.id,
            ),
        )
        winner = ordered[0]
        for loser in ordered[1:]:
            losers[loser.id] = f"Duplicate of {winner.company_name} / {winner.primary_title}"
    return losers


def evaluate_critic_decision(
    session: Session,
    lead: Lead,
    profile,
    freshness_window_days: Optional[int] = 14,
    duplicate_losers: Optional[dict[int, str]] = None,
) -> dict:
    duplicate_losers = duplicate_losers or {}
    context = _authoritative_listing_context(session, lead)
    url = context["url"]
    listing_status = context["listing_status"]
    freshness_days = context["freshness_days"]
    freshness_hours = context["freshness_hours"]
    expiration_confidence = context["expiration_confidence"] or 0.0
    page_text = context["page_text"]
    description_text = context["description_text"]
    http_status = context["http_status"]
    last_seen_at = context["last_seen_at"]
    reasons: list[str] = []
    status = "visible"
    suppression_category = "none"
    ai_critic = None

    if lead.id in duplicate_losers:
        reasons.append(duplicate_losers[lead.id])
        status = "suppressed"
        suppression_category = "duplicate"

    if lead.lead_type in {"listing", "combined"}:
        if not context["listing"]:
            reasons.append("Missing backing listing record")
            status = "suppressed"
            suppression_category = "non_live"
        if not url or not str(url).startswith("http"):
            reasons.append("Missing or invalid job URL")
            status = "suppressed"
            suppression_category = "broken"
        if http_status in {404, 410}:
            reasons.append(f"Job page returned HTTP {http_status}")
            status = "suppressed"
            suppression_category = "broken"
        if has_expired_pattern(description_text, page_text):
            reasons.append("Expired text pattern detected in job content")
            status = "suppressed"
            suppression_category = "expired"
        if last_seen_at is None:
            reasons.append("Listing has no last-seen timestamp from connector fetches")
            status = "uncertain"
            suppression_category = "uncertain"
        else:
            age_since_seen_hours = max((datetime.now(timezone.utc) - last_seen_at).total_seconds() / 3600, 0.0)
            if age_since_seen_hours > 48:
                reasons.append(f"Listing has not been seen in {round(age_since_seen_hours, 1)} hours")
                status = "suppressed"
                suppression_category = "non_live"
        if listing_status in {"expired", "suspected_expired"}:
            reasons.append(f"Listing status is {listing_status}")
            status = "suppressed"
            suppression_category = "expired"
        elif listing_status != "active" and status == "visible":
            if freshness_hours is not None and freshness_hours <= 72 and expiration_confidence < 0.2:
                reasons.append("Listing is recent but liveness is still uncertain")
                status = "uncertain"
                suppression_category = "uncertain"
            else:
                reasons.append(f"Listing status is {listing_status or 'unknown'}")
                status = "uncertain"
                suppression_category = "uncertain"
        if freshness_hours is None and status == "visible":
            reasons.append("No reliable posted date found")
            status = "uncertain"
            suppression_category = "uncertain"
        elif freshness_hours is not None and freshness_window_days is not None and freshness_hours > freshness_window_days * 24:
            reasons.append(f"Freshness exceeded the default {freshness_window_days}-day window")
            status = "suppressed"
            suppression_category = "stale"
        if lead.confidence_label == "low" and status == "visible":
            reasons.append("Confidence is too low for default surfaced listings")
            status = "uncertain"
            suppression_category = "uncertain"
        ai_critic = judge_critic_with_ai(
            title=lead.primary_title,
            company_name=lead.company_name,
            description_text=description_text,
            listing_status=listing_status,
            freshness_days=freshness_days,
            page_text=page_text,
            url=url,
        )
        if ai_critic and status == "visible" and ai_critic.get("quality_assessment") in {"uncertain", "stale", "suppress"}:
            reasons.append(f"AI critic flagged: {'; '.join(ai_critic.get('reasons', []))}")
            status = "uncertain"
            suppression_category = "uncertain"

    if lead.qualification_fit_label in {"underqualified", "overqualified"} and status == "visible":
        reasons.append(f"Qualification fit is {lead.qualification_fit_label}")
        status = "hidden"
        suppression_category = "qualification"

    if (lead.score_breakdown_json or {}).get("composite", 0.0) < profile.minimum_fit_threshold and status == "visible":
        reasons.append("Composite fit is below the candidate threshold")
        status = "hidden"
        suppression_category = "low_fit"

    if lead.company_name.lower() in [item.lower() for item in (profile.excluded_companies_json or [])] and status == "visible":
        reasons.append("Company is muted in the candidate profile")
        status = "hidden"
        suppression_category = "user_suppressed"

    if lead.lead_type == "signal":
        signal = session.get(Signal, lead.signal_id) if lead.signal_id else None
        if signal and signal.signal_status in {"needs_recheck", "resolved_no_listing"} and status == "visible":
            status = "investigation"
            reasons = reasons or ["Weak signal is under investigation without a confirmed active listing"]
            suppression_category = "investigation"
        elif status == "visible":
            status = "uncertain"
            reasons = reasons or ["Signal-only lead requires explicit opt-in"]
            suppression_category = "uncertain"

    if status == "visible":
        reasons = ["Passed freshness, liveness, duplicate, and qualification gates"]

    return {
        "status": status,
        "visible": status == "visible",
        "reasons": reasons,
        "suppression_category": suppression_category,
        "authoritative_url": url,
        "listing_status": listing_status,
        "freshness_hours": freshness_hours,
        "freshness_days": freshness_days,
        "posted_at": context["posted_at"],
        "first_published_at": context["first_published_at"],
        "discovered_at": context["discovered_at"],
        "last_seen_at": context["last_seen_at"],
        "updated_at": context["updated_at"],
        "liveness_evidence": {
            "listing_status": listing_status,
            "freshness_hours": freshness_hours,
            "freshness_days": freshness_days,
            "expiration_confidence": round(expiration_confidence, 2),
            "http_status": http_status,
            "expired_pattern_detected": has_expired_pattern(description_text, page_text),
            "first_published_at": context["first_published_at"],
            "discovered_at": context["discovered_at"],
            "last_seen_at": context["last_seen_at"],
            "updated_at": context["updated_at"],
        },
        "ai_critic_assessment": ai_critic,
    }


def apply_critic_decision_to_lead(
    session: Session,
    lead: Lead,
    profile,
    freshness_window_days: Optional[int] = 14,
    duplicate_losers: Optional[dict[int, str]] = None,
) -> dict:
    decision = evaluate_critic_decision(
        session=session,
        lead=lead,
        profile=profile,
        freshness_window_days=freshness_window_days,
        duplicate_losers=duplicate_losers,
    )
    evidence = dict(lead.evidence_json or {})
    evidence["critic_status"] = decision["status"]
    evidence["critic_reasons"] = decision["reasons"]
    evidence["suppression_reason"] = "; ".join(decision["reasons"]) if decision["status"] != "visible" else None
    evidence["suppression_category"] = decision["suppression_category"]
    evidence["liveness_evidence"] = decision["liveness_evidence"]
    evidence["ai_critic_assessment"] = decision.get("ai_critic_assessment")
    evidence["listing_status"] = decision["listing_status"]
    evidence["freshness_hours"] = decision["freshness_hours"]
    evidence["freshness_days"] = decision["freshness_days"]
    evidence["url"] = decision["authoritative_url"]
    evidence["posted_at"] = _isoformat_utc(decision["posted_at"])
    evidence["first_published_at"] = _isoformat_utc(decision["first_published_at"])
    evidence["discovered_at"] = _isoformat_utc(decision["discovered_at"])
    evidence["last_seen_at"] = _isoformat_utc(decision["last_seen_at"])
    evidence["updated_at"] = _isoformat_utc(decision["updated_at"])
    liveness = dict(evidence.get("liveness_evidence") or {})
    for key in ("first_published_at", "discovered_at", "last_seen_at", "updated_at"):
        liveness[key] = _isoformat_utc(liveness.get(key))
    evidence["liveness_evidence"] = liveness
    lead.evidence_json = evidence
    lead.hidden = not decision["visible"]
    return decision


def _upsert_lead(
    session: Session,
    lead_type: str,
    company_name: str,
    company_id: Optional[int],
    title: str,
    listing: Optional[Listing],
    signal: Optional[Signal],
    profile,
    listing_url: Optional[str],
    source_type: str,
    company_domain: Optional[str],
    location: Optional[str],
    description_text: str,
    listing_status: Optional[str],
    freshness_label: str,
    evidence_json: dict,
) -> tuple[Lead, bool]:
    existing = session.scalar(
        select(Lead).where(
            Lead.lead_type == lead_type,
            Lead.company_name == company_name,
            Lead.primary_title == title,
            Lead.listing_id == (listing.id if listing else None),
            Lead.signal_id == (signal.id if signal else None),
        )
    )
    feedback_learning = _source_learning(profile)
    breakdown = score_lead(
        profile=profile,
        lead_type=lead_type,
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        location=location,
        description_text=description_text,
        freshness_label=freshness_label,
        listing_status=listing_status,
        source_type=source_type,
        evidence_count=len(evidence_json.get("snippets", [])),
        feedback_learning=feedback_learning,
    )
    candidate_context = profile.raw_resume_text or (profile.extracted_summary_json or {}).get("summary", "")
    ai_fit = judge_fit_with_ai(
        profile_text=candidate_context,
        title=title,
        company_name=company_name,
        location=location,
        description_text=description_text,
    )
    displayed_fit_label = breakdown["qualification_fit_label"]
    if ai_fit:
        displayed_fit_label = {
            "strong_fit": "strong fit",
            "adjacent": "adjacent",
            "stretch": "stretch",
            "underqualified": "underqualified",
            "overqualified": "overqualified",
            "unclear": "unclear",
        }.get(ai_fit.get("classification"), displayed_fit_label)
        if displayed_fit_label != breakdown["qualification_fit_label"]:
            logger.info(
                "[AI_FIT_LABEL_CHANGE] %s",
                {
                    "title": title,
                    "company": company_name,
                    "deterministic_label": breakdown["qualification_fit_label"],
                    "ai_label": displayed_fit_label,
                    "source_type": source_type,
                },
            )
        ai_matched_fields = ai_fit.get("matched_profile_fields", [])
        if ai_matched_fields:
            breakdown["matched_profile_fields"] = list(
                dict.fromkeys(breakdown.get("matched_profile_fields", []) + ai_matched_fields)
            )

    feedback_notes = (profile.extracted_summary_json or {}).get("learning", {}).get("feedback_notes", [])[-3:]
    uncertainty = None
    if lead_type == "signal":
        uncertainty = "Signal exists without a confirmed active listing yet"
    elif listing_status != "active":
        uncertainty = f"Listing status is {listing_status}"

    explanation = build_explanation(
        lead_type=lead_type,
        matched_profile_fields=breakdown.get("matched_profile_fields", []),
        feedback_notes=feedback_notes,
        freshness_label=breakdown["freshness_label"],
        confidence_label=breakdown["confidence_label"],
        candidate_context=candidate_context[:1000] if candidate_context else None,
        fit_assessment=ai_fit,
        uncertainty=uncertainty,
    )

    score_breakdown = {key: value for key, value in breakdown.items() if key not in {"matched_profile_fields"}}
    evidence_json = dict(evidence_json)
    discovery_source = evidence_json.get("discovery_source") or ((listing.metadata_json or {}).get("discovery_source") if listing else None)
    source_platform = "x_demo" if source_type == "x" else source_type
    if discovery_source:
        source_platform = f"{source_type}+{discovery_source}"
    evidence_json.update(
        {
            "matched_profile_fields": breakdown.get("matched_profile_fields", []),
            "feedback_notes": feedback_notes,
            "freshness_status": freshness_label,
            "freshness_hours": listing.freshness_hours if listing else 0.0,
            "freshness_days": listing.freshness_days if listing else 0,
            "confidence_status": breakdown["confidence_label"],
            "listing_status": listing_status,
            "source_type": source_type,
            "source_platform": source_platform,
            "discovery_source": discovery_source,
            "company_domain": company_domain,
            "url": listing_url,
            "first_published_at": listing.first_published_at.isoformat() if listing and listing.first_published_at else None,
            "discovered_at": listing.discovered_at.isoformat() if listing and listing.discovered_at else None,
            "last_seen_at": listing.last_seen_at.isoformat() if listing and listing.last_seen_at else None,
            "ai_fit_assessment": ai_fit,
        }
    )
    if lead_type == "combined" and signal and listing:
        evidence_json["resolution_story"] = [
            f"Weak hiring signal found from {signal.source_type}",
            f"Company guess resolved to {company_name}",
            f"Fresh active listing found via {listing.source_type}",
            "Signal and listing merged into one surfaced lead",
        ]
    elif lead_type == "signal":
        evidence_json["resolution_story"] = [
            "Weak hiring signal found",
            "No active listing confirmed yet",
        ]

    payload = {
        "lead_type": lead_type,
        "company_name": company_name,
        "company_id": company_id,
        "primary_title": title,
        "listing_id": listing.id if listing else None,
        "signal_id": signal.id if signal else None,
        "rank_label": breakdown["rank_label"],
        "confidence_label": breakdown["confidence_label"],
        "freshness_label": breakdown["freshness_label"],
        "title_fit_label": breakdown["title_fit_label"],
        "qualification_fit_label": displayed_fit_label,
        "explanation": explanation,
        "score_breakdown_json": score_breakdown,
        "evidence_json": evidence_json,
        "last_agent_action": "Resolver: surfaced lead",
        "hidden": False,
    }

    if existing:
        material_changed = False
        for key, value in payload.items():
            if getattr(existing, key) != value:
                setattr(existing, key, value)
                material_changed = True
        apply_critic_decision_to_lead(session, existing, profile)
        if material_changed:
            existing.updated_at = datetime.utcnow()
            append_lead_agent_trace(existing, "Resolver", "surfaced lead", f"Resolver refreshed {company_name} / {title}", change_state="updated")
        return existing, False

    lead = Lead(**payload)
    session.add(lead)
    session.flush()
    apply_critic_decision_to_lead(session, lead, profile)
    append_lead_agent_trace(lead, "Resolver", "surfaced lead", f"Resolver surfaced {company_name} / {title}", change_state="new")
    return lead, True


def sync_all(
    session: Session,
    include_rechecks: bool = True,
    enabled_connectors: set[str] | None = None,
    strict_live_connectors: set[str] | None = None,
) -> SyncResult:
    settings = get_settings()
    profile = get_candidate_profile(session)
    queries = ensure_source_queries(session)
    enabled_connectors = enabled_connectors or {"greenhouse", "ashby", "x_search"}
    strict_live_connectors = strict_live_connectors or set()

    greenhouse_connector = GreenhouseConnector()
    ashby_connector = AshbyConnector()
    search_connector = SearchDiscoveryConnector()
    x_connector = XSearchConnector()

    greenhouse_jobs: list[dict] = []
    ashby_jobs: list[dict] = []
    search_results = []
    x_raw_signals: list[dict] = []
    greenhouse_live = False
    ashby_live = False
    search_live = False
    x_live = False
    discovered_greenhouse_queries: dict[str, list[str]] = {}
    discovered_ashby_queries: dict[str, list[str]] = {}
    discovery_metrics: dict[str, dict[str, int]] = {}

    watchlist_values = [
        item.value
        for item in session.scalars(
            select(WatchlistItem).where(WatchlistItem.status.in_(["active", "proposed"])).order_by(WatchlistItem.updated_at.desc())
        ).all()
    ]
    search_queries = build_search_queries(
        core_titles=profile.core_titles_json or profile.preferred_titles_json or [],
        adjacent_titles=profile.adjacent_titles_json or [],
        preferred_domains=profile.preferred_domains_json or [],
        watchlist_items=watchlist_values,
    )

    if settings.search_discovery_enabled and search_queries:
        search_results, search_live, _ = run_connector_fetch(
            session,
            "search_web",
            partial(search_connector.fetch, search_queries, "search_web" in strict_live_connectors),
            date_fields=[],
        )
        discovered_greenhouse_queries = extract_discovered_greenhouse_tokens(search_results)
        discovered_ashby_queries = extract_discovered_ashby_orgs(search_results)
    search_verified_count = sum(
        1
        for result in search_results
        if result.url and ("greenhouse.io" in result.url.lower() or "ashbyhq.com" in result.url.lower())
    )
    discovery_metrics["search_web"] = {
        "raw": len(search_results),
        "normalized": len(search_results),
        "verified": search_verified_count,
    }

    if "greenhouse" in enabled_connectors:
        greenhouse_tokens = list(dict.fromkeys(settings.greenhouse_tokens + list(discovered_greenhouse_queries)))
        greenhouse_jobs, greenhouse_live, _ = run_connector_fetch(
            session,
            "greenhouse",
            partial(
                greenhouse_connector.fetch,
                "greenhouse" in strict_live_connectors,
                greenhouse_tokens,
                discovered_greenhouse_queries,
            ),
            date_fields=["first_published", "updated_at"],
        )
    if "ashby" in enabled_connectors:
        ashby_orgs = list(dict.fromkeys(settings.ashby_orgs + list(discovered_ashby_queries)))
        ashby_jobs, ashby_live, _ = run_connector_fetch(
            session,
            "ashby",
            partial(
                ashby_connector.fetch,
                "ashby" in strict_live_connectors,
                ashby_orgs,
                discovered_ashby_queries,
            ),
            date_fields=["publishedDate"],
        )
    if "x_search" in enabled_connectors:
        x_raw_signals, x_live, _ = run_connector_fetch(
            session,
            "x_search",
            partial(x_connector.fetch, queries, "x_search" in strict_live_connectors),
            date_fields=["published_at"],
        )

    greenhouse_normalized = [normalize_greenhouse_job(job) for job in greenhouse_jobs]
    greenhouse_verified = [validate_listing(record) for record in greenhouse_normalized if _verify_listing_record(record)]
    discovery_metrics["greenhouse"] = {
        "raw": len(greenhouse_jobs),
        "normalized": len(greenhouse_normalized),
        "verified": len(greenhouse_verified),
    }
    logger.info(
        "[VERIFICATION] connector=greenhouse before=%s after=%s",
        len(greenhouse_normalized),
        len(greenhouse_verified),
    )

    ashby_normalized = [normalize_ashby_job(job, job.get("companyName")) for job in ashby_jobs]
    ashby_verified = [validate_listing(record) for record in ashby_normalized if _verify_listing_record(record)]
    discovery_metrics["ashby"] = {
        "raw": len(ashby_jobs),
        "normalized": len(ashby_normalized),
        "verified": len(ashby_verified),
    }
    logger.info(
        "[VERIFICATION] connector=ashby before=%s after=%s",
        len(ashby_normalized),
        len(ashby_verified),
    )

    signals_ingested = 0
    listings_ingested = 0
    leads_created = 0
    leads_updated = 0
    rechecks_queued = 0
    investigations_opened = 0

    extracted_signals = extract_many(x_raw_signals)
    verified_signals = [raw for raw in extracted_signals if _verify_signal_record(raw)]
    discovery_metrics["x_search"] = {
        "raw": len(x_raw_signals),
        "normalized": len(extracted_signals),
        "verified": len(verified_signals),
    }
    logger.info(
        "[VERIFICATION] connector=x_search before=%s after=%s",
        len(extracted_signals),
        len(verified_signals),
    )
    logger.info("[DISCOVERY_METRICS] %s", discovery_metrics)

    signal_objects: list[Signal] = []
    for raw in verified_signals:
        if raw.published_at and isinstance(raw.published_at, str):
            raw.published_at = datetime.fromisoformat(raw.published_at.replace("Z", "+00:00"))
        signal = _upsert_signal(session, raw)
        signals_ingested += 1
        signal_objects.append(signal)

    listing_records = list(greenhouse_verified)
    listing_records.extend(ashby_verified)

    listing_objects: list[Listing] = []
    for record in listing_records:
        resolved_company = resolve_company_name(session, record.company_name, record.description_text or "") or record.company_name
        company = get_or_create_company(
            session,
            name=resolved_company,
            domain=record.company_domain,
            careers_url=record.careers_url,
            ats_provider=record.source_type,
        )
        record.company_name = company.name
        listing, _ = _upsert_listing(session, record, company.id)
        listing_objects.append(listing)
        listings_ingested += 1

    used_listing_ids: set[int] = set()
    for signal in signal_objects:
        resolved_company = resolve_company_name(session, signal.company_guess, signal.raw_text)
        if resolved_company:
            signal.company_guess = resolved_company
        matching_listing = _matching_listing_for_signal(listing_objects, signal) if resolved_company else None

        if matching_listing:
            used_listing_ids.add(matching_listing.id)
            lead, created = _upsert_lead(
                session=session,
                lead_type="combined",
                company_name=matching_listing.company_name,
                company_id=matching_listing.company_id,
                title=matching_listing.title,
                listing=matching_listing,
                signal=signal,
                profile=profile,
                listing_url=matching_listing.url,
                source_type=matching_listing.source_type,
                company_domain=(matching_listing.metadata_json or {}).get("company_domain"),
                location=matching_listing.location,
                description_text=f"{matching_listing.description_text or ''}\n{signal.raw_text}",
                listing_status=matching_listing.listing_status,
                freshness_label=classify_freshness_label(matching_listing.freshness_days, matching_listing.freshness_hours),
                evidence_json={
                    "snippets": [signal.raw_text[:220], (matching_listing.description_text or "")[:220]],
                    "source_queries": [
                        item.get("query_text")
                        for item in x_raw_signals
                        if item["url"] == signal.source_url and item.get("query_text")
                    ],
                },
            )
            leads_created += 1 if created else 0
            leads_updated += 0 if created else 1
            query_text = next((item.get("query_text") for item in x_raw_signals if item["url"] == signal.source_url), None)
            if query_text:
                _query_stats_increment(session, [query_text])
            signal.signal_status = "matched_to_listing"
            upsert_investigation(
                session,
                signal=signal,
                status="resolved",
                confidence=signal.hiring_confidence,
                note=f"Resolved to active listing at {matching_listing.company_name}.",
            )
            continue

        if resolved_company:
            company = get_or_create_company(session, name=resolved_company, ats_provider="x")
            query_text = next((item.get("query_text") for item in x_raw_signals if item["url"] == signal.source_url), None)
            lead, created = _upsert_lead(
                session=session,
                lead_type="signal",
                company_name=company.name,
                company_id=company.id,
                title=(signal.role_guess or "Hiring signal").title(),
                listing=None,
                signal=signal,
                profile=profile,
                listing_url=signal.source_url,
                source_type="x",
                company_domain=company.domain,
                location=signal.location_guess,
                description_text=signal.raw_text,
                listing_status=None,
                freshness_label=classify_freshness_label(0),
                evidence_json={
                    "snippets": [signal.raw_text[:220]],
                    "source_queries": [query_text] if query_text else [],
                },
            )
            leads_created += 1 if created else 0
            leads_updated += 0 if created else 1
            if query_text:
                _query_stats_increment(session, [query_text])
            signal.signal_status = "resolved_no_listing"
            existing_investigation = session.scalar(select(Investigation).where(Investigation.signal_id == signal.id))
            if existing_investigation or investigations_opened < settings.max_investigations_opened_per_cycle:
                upsert_investigation(
                    session,
                    signal=signal,
                    status="open",
                    confidence=signal.hiring_confidence,
                    note="Promising weak signal without a confirmed active listing yet.",
                    next_check_at=datetime.utcnow() + timedelta(hours=6),
                )
                if not existing_investigation:
                    investigations_opened += 1
        else:
            signal.signal_status = "needs_recheck"
            queue_recheck(session, "signal", signal.id, "Unresolved weak signal without confident company resolution")
            rechecks_queued += 1
            existing_investigation = session.scalar(select(Investigation).where(Investigation.signal_id == signal.id))
            if existing_investigation or investigations_opened < settings.max_investigations_opened_per_cycle:
                upsert_investigation(
                    session,
                    signal=signal,
                    status="open",
                    confidence=signal.hiring_confidence,
                    note="Could not confidently resolve the company yet. Recheck queued.",
                    next_check_at=datetime.utcnow() + timedelta(hours=6),
                )
                if not existing_investigation:
                    investigations_opened += 1

    for listing in listing_objects:
        if listing.id in used_listing_ids:
            continue
        company = get_or_create_company(session, name=listing.company_name, ats_provider=listing.source_type)
        query_texts = listing.metadata_json.get("source_queries", []) if listing.metadata_json else []
        lead, created = _upsert_lead(
            session=session,
            lead_type="listing",
            company_name=listing.company_name,
            company_id=company.id,
            title=listing.title,
            listing=listing,
            signal=None,
            profile=profile,
            listing_url=listing.url,
            source_type=listing.source_type,
            company_domain=company.domain,
            location=listing.location,
            description_text=listing.description_text or "",
            listing_status=listing.listing_status,
            freshness_label=classify_freshness_label(listing.freshness_days, listing.freshness_hours),
            evidence_json={
                "snippets": [(listing.description_text or "")[:240]],
                "source_queries": query_texts,
                "discovery_source": (listing.metadata_json or {}).get("discovery_source"),
            },
        )
        leads_created += 1 if created else 0
        leads_updated += 0 if created else 1
        if query_texts:
            _query_stats_increment(session, query_texts)

    if include_rechecks:
        due_items = session.scalars(
            select(RecheckQueue).where(
                RecheckQueue.status.in_(["queued", "retrying"]),
                RecheckQueue.next_check_at <= datetime.utcnow(),
            )
        ).all()
        for item in due_items:
            item.status = "retrying" if item.retry_count < 2 else "exhausted"
            item.retry_count += 1
            if item.entity_type == "signal":
                mark_investigation_attempt(
                    session,
                    signal_id=item.entity_id,
                    note=f"Automatic resolver recheck attempt {item.retry_count} ran.",
                )

    generate_follow_up_tasks(session)
    session.flush()
    surfaced_count = session.scalar(select(func.count(Lead.id)).where(Lead.hidden.is_(False))) or 0
    discovery_summary = None
    if (
        discovery_metrics.get("greenhouse", {}).get("verified", 0) == 0
        and discovery_metrics.get("ashby", {}).get("verified", 0) == 0
        and discovery_metrics.get("search_web", {}).get("raw", 0) > 0
        and surfaced_count == 0
    ):
        discovery_summary = "Jobs were discovered but all were filtered out before surfacing."
        logger.error("[DISCOVERY_FAILURE] No high-signal jobs. Only weak signals found and filtered out.")
    elif all(item.get("raw", 0) == 0 for item in discovery_metrics.values()):
        discovery_summary = "No jobs found from any connector."
        logger.error("[DISCOVERY_FAILURE] No jobs found from any source.")
    elif surfaced_count > 0:
        discovery_summary = "Jobs found and surfaced normally."
    return SyncResult(
        signals_ingested=signals_ingested,
        listings_ingested=listings_ingested,
        leads_created=leads_created,
        leads_updated=leads_updated,
        rechecks_queued=rechecks_queued,
        live_mode_used=any([greenhouse_live, ashby_live, search_live, x_live]),
        discovery_metrics=discovery_metrics,
        surfaced_count=surfaced_count,
        discovery_summary=discovery_summary,
    )


def list_leads(
    session: Session,
    freshness_window_days: Optional[int] = 14,
    include_hidden: bool = False,
    include_unqualified: bool = False,
    lead_type: Optional[str] = None,
    only_saved: bool = False,
    only_applied: bool = False,
    status: Optional[str] = None,
    include_signal_only: bool = False,
) -> list[LeadResponse]:
    records = session.scalars(select(Lead).order_by(Lead.surfaced_at.desc(), Lead.rank_label.asc())).all()
    profile = get_candidate_profile(session)
    duplicate_losers = _duplicate_winner_context(session, records)
    items: list[LeadResponse] = []
    omitted_by_status: Counter[str] = Counter()
    omitted_by_category: Counter[str] = Counter()
    total_considered = 0
    for lead in records:
        total_considered += 1
        evidence = lead.evidence_json or {}
        decision = evaluate_critic_decision(
            session=session,
            lead=lead,
            profile=profile,
            freshness_window_days=freshness_window_days,
            duplicate_losers=duplicate_losers,
        )
        authoritative = _authoritative_listing_context(session, lead)
        freshness_days = decision["freshness_days"]
        freshness_hours = decision["freshness_hours"]
        listing_status = decision["listing_status"]
        source_type = evidence.get("source_type", lead.lead_type)
        application = session.scalar(select(Application).where(Application.lead_id == lead.id))
        saved = application is not None and application.date_saved is not None
        applied = application is not None and application.date_applied is not None
        current_status = application.current_status if application else None
        next_action, follow_up_due = next_action_for_application(session, application.id) if application else (None, False)

        if only_saved and not saved:
            continue
        if only_applied and not applied:
            continue
        if status and current_status != status:
            continue

        if lead_type and lead.lead_type != lead_type:
            omitted_by_status["lead_type_filtered"] += 1
            continue
        if lead.lead_type == "signal" and lead_type != "signal" and not include_signal_only:
            omitted_by_status["signal_only_filtered"] += 1
            continue
        if not include_hidden and not decision["visible"]:
            if lead.lead_type == "signal" and include_signal_only and decision["status"] in {"uncertain", "investigation"}:
                pass
            elif include_unqualified and decision["suppression_category"] == "qualification":
                pass
            else:
                omitted_by_status[decision["status"]] += 1
                omitted_by_category[decision["suppression_category"]] += 1
                continue
        if freshness_window_days is not None and freshness_hours is not None and freshness_hours > freshness_window_days * 24:
            omitted_by_status["freshness_window_filtered"] += 1
            omitted_by_category["stale"] += 1
            continue

        response_evidence = dict(evidence)
        response_evidence["critic_status"] = decision["status"]
        response_evidence["critic_reasons"] = decision["reasons"]
        response_evidence["suppression_reason"] = "; ".join(decision["reasons"]) if decision["status"] != "visible" else None
        response_evidence["suppression_category"] = decision["suppression_category"]
        response_evidence["liveness_evidence"] = decision["liveness_evidence"]
        response_evidence["listing_status"] = listing_status
        response_evidence["freshness_hours"] = freshness_hours
        response_evidence["freshness_days"] = freshness_days
        response_evidence["url"] = decision["authoritative_url"]
        response_evidence["posted_at"] = _isoformat_utc(decision["posted_at"])
        response_evidence["first_published_at"] = _isoformat_utc(decision["first_published_at"])
        response_evidence["discovered_at"] = _isoformat_utc(decision["discovered_at"])
        response_evidence["last_seen_at"] = _isoformat_utc(decision["last_seen_at"])
        response_evidence["updated_at"] = _isoformat_utc(decision["updated_at"])

        items.append(
            LeadResponse(
                id=lead.id,
                lead_type=lead.lead_type,
                company_name=lead.company_name,
                primary_title=lead.primary_title,
                url=decision["authoritative_url"],
                source_type=source_type,
                listing_status=listing_status,
                first_published_at=_ensure_utc_datetime(decision["first_published_at"]),
                discovered_at=_ensure_utc_datetime(decision["discovered_at"]),
                last_seen_at=_ensure_utc_datetime(decision["last_seen_at"]),
                updated_at=_ensure_utc_datetime(decision["updated_at"] or lead.updated_at),
                freshness_hours=freshness_hours,
                freshness_days=freshness_days,
                posted_at=_ensure_utc_datetime(decision["posted_at"]),
                surfaced_at=_ensure_utc_datetime(lead.surfaced_at),
                rank_label=lead.rank_label,
                confidence_label=lead.confidence_label,
                freshness_label=lead.freshness_label,
                title_fit_label=lead.title_fit_label,
                qualification_fit_label=lead.qualification_fit_label,
                source_platform=evidence.get("source_platform", source_type),
                saved=saved,
                applied=applied,
                current_status=current_status,
                date_saved=_ensure_utc_datetime(application.date_saved) if application else None,
                date_applied=_ensure_utc_datetime(application.date_applied) if application else None,
                application_notes=application.notes if application else None,
                application_updated_at=_ensure_utc_datetime(application.updated_at) if application else None,
                next_action=next_action,
                follow_up_due=follow_up_due,
                explanation=lead.explanation,
                last_agent_action=lead.last_agent_action,
                hidden=not decision["visible"],
                score_breakdown_json=lead.score_breakdown_json or {},
                evidence_json=response_evidence,
            )
        )
    logger.info(
        "[READTIME_CRITIC_DROPS] %s",
        {
            "total_considered": total_considered,
            "total_returned": len(items),
            "omitted_by_status": dict(omitted_by_status),
            "omitted_by_category": dict(omitted_by_category),
        },
    )
    rank_order = {"strong": 0, "medium": 1, "weak": 2}
    freshness_order = {"fresh": 0, "recent": 1, "stale": 2, "unknown": 3}
    lead_type_order = {"combined": 0, "listing": 1, "signal": 2}

    def recency_value(item: LeadResponse) -> float:
        reference = item.posted_at or item.surfaced_at
        return -reference.timestamp() if reference else 0.0

    return sorted(
        items,
        key=lambda item: (
            rank_order.get(item.rank_label, 3),
            lead_type_order.get(item.lead_type, 3),
            freshness_order.get(item.freshness_label, 4),
            recency_value(item),
            item.company_name.lower(),
        ),
    )


def get_stats(session: Session) -> StatsResponse:
    return StatsResponse(
        total_leads=session.scalar(select(func.count(Lead.id))) or 0,
        visible_leads=session.scalar(select(func.count(Lead.id)).where(Lead.hidden.is_(False))) or 0,
        active_listings=session.scalar(select(func.count(Listing.id)).where(Listing.listing_status == "active")) or 0,
        fresh_listings=session.scalar(select(func.count(Listing.id)).where(Listing.freshness_days <= 7, Listing.listing_status == "active")) or 0,
        combined_leads=session.scalar(select(func.count(Lead.id)).where(Lead.lead_type == "combined")) or 0,
        signal_only_leads=session.scalar(select(func.count(Lead.id)).where(Lead.lead_type == "signal")) or 0,
        saved_leads=session.scalar(select(func.count(Application.id)).where(Application.date_saved.is_not(None))) or 0,
        applied_leads=session.scalar(select(func.count(Application.id)).where(Application.date_applied.is_not(None))) or 0,
        pending_rechecks=session.scalar(select(func.count(RecheckQueue.id)).where(RecheckQueue.status.in_(["queued", "retrying"]))) or 0,
    )
