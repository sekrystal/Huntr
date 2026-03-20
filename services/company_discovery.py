from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import re
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from connectors.search_web import SearchDiscoveryResult
from core.config import Settings, get_settings
from core.models import Application, CompanyDiscovery, Lead
from core.schemas import CompanyDiscoveryRowResponse, DiscoveryStatusResponse


def classify_surface_provenance(
    board_type: str,
    board_locator: str,
    *,
    is_new: bool,
    settings: Settings | None = None,
) -> str:
    settings = settings or get_settings()
    normalized = (board_locator or "").lower()
    if board_type == "greenhouse" and normalized in {token.lower() for token in settings.greenhouse_tokens}:
        return "preseeded"
    if board_type == "ashby" and normalized in {org.lower() for org in settings.ashby_orgs}:
        return "preseeded"
    return "discovered_new" if is_new else "discovered_existing"


def source_lineage_for_surface(board_type: str, provenance: str, discovery_source: Optional[str]) -> str:
    if provenance == "preseeded" or not discovery_source:
        return board_type
    return f"{board_type}+{discovery_source}"


def normalize_company_key(company_name: str, company_domain: Optional[str] = None) -> str:
    if company_domain:
        return re.sub(r"[^a-z0-9]+", "-", company_domain.lower()).strip("-")
    return re.sub(r"[^a-z0-9]+", "-", (company_name or "unknown-company").lower()).strip("-")


@dataclass
class CompanyDiscoveryCandidate:
    company_name: str
    company_domain: Optional[str]
    normalized_company_key: str
    discovery_source: str
    discovery_query: str
    board_type: str
    board_locator: str
    result_url: str
    result_title: str
    triage_score: float = 0.0
    triage_reasons: list[str] = field(default_factory=list)
    is_new: bool = False

    @property
    def discovery_key(self) -> str:
        return f"{self.board_type}:{self.board_locator.lower()}"


def _company_name_from_locator(locator: str) -> str:
    return locator.replace("-", " ").replace("_", " ").title()


def _normalize_result_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        uddg = parse_qs(parsed.query).get("uddg", [])
        if uddg:
            return unquote(uddg[0])
    return url


def inspect_search_result_candidate(result: SearchDiscoveryResult) -> dict[str, Optional[str]]:
    normalized_url = _normalize_result_url(result.url)
    parsed = urlparse(normalized_url)
    host = parsed.netloc.lower()
    path_parts = [part for part in parsed.path.split("/") if part]
    board_type = None
    board_locator = None
    reason = None

    if not parsed.scheme or not host:
        reason = "missing_host"
    elif parsed.scheme not in {"http", "https"}:
        reason = "non_http_url"
    elif ("job-boards.greenhouse.io" in host or "boards.greenhouse.io" in host) and len(path_parts) >= 1:
        board_type = "greenhouse"
        board_locator = path_parts[0]
    elif "jobs.ashbyhq.com" in host and path_parts:
        board_type = "ashby"
        board_locator = path_parts[0]
    elif host.startswith("careers.") or any(token in parsed.path.lower() for token in ["/careers", "/jobs", "/join-us", "/work-with-us", "/open-roles", "/join", "/company/careers"]):
        board_type = "careers_page"
        board_locator = host
    else:
        reason = "unsupported_surface"

    if board_type and not board_locator:
        reason = "missing_board_locator"

    return {
        "normalized_url": normalized_url,
        "host": host,
        "path": parsed.path,
        "board_type": board_type,
        "board_locator": board_locator,
        "reason": reason,
    }


def candidate_from_search_result(result: SearchDiscoveryResult) -> CompanyDiscoveryCandidate | None:
    inspection = inspect_search_result_candidate(result)
    board_type = inspection["board_type"]
    board_locator = inspection["board_locator"]
    if not board_type or not board_locator:
        return None
    company_name = _company_name_from_locator(board_locator.split(".")[0] if board_type == "careers_page" else board_locator)
    company_domain = inspection["host"] if board_type == "careers_page" else None
    return CompanyDiscoveryCandidate(
        company_name=company_name,
        company_domain=company_domain,
        normalized_company_key=normalize_company_key(company_name, company_domain),
        discovery_source=result.source_surface,
        discovery_query=result.query_text,
        board_type=board_type,
        board_locator=board_locator,
        result_url=inspection["normalized_url"] or result.url,
        result_title=result.title,
    )


def build_query_inputs(session: Session, profile) -> dict[str, list[str]]:
    learning = (profile.extracted_summary_json or {}).get("learning", {})
    boosted_titles = [title for title, _ in sorted((learning.get("title_weights") or {}).items(), key=lambda item: item[1], reverse=True)[:3]]
    role_families = [family for family, _ in sorted((learning.get("role_family_weights") or {}).items(), key=lambda item: item[1], reverse=True)[:3]]
    recent_titles = [
        row[0]
        for row in session.execute(
            select(Lead.primary_title)
            .join(Application, Application.lead_id == Lead.id)
            .where(Application.current_status.in_(["saved", "applied"]))
            .order_by(Application.updated_at.desc())
            .limit(4)
        ).all()
    ]
    return {
        "boosted_titles": boosted_titles,
        "role_families": role_families,
        "recent_titles": recent_titles,
    }


def triage_candidate(
    session: Session,
    candidate: CompanyDiscoveryCandidate,
    profile,
    configured_boards: set[str],
    settings: Settings | None = None,
) -> tuple[float, list[str], CompanyDiscovery | None]:
    settings = settings or get_settings()
    existing = session.scalar(select(CompanyDiscovery).where(CompanyDiscovery.discovery_key == candidate.discovery_key))
    score = 0.0
    reasons: list[str] = []
    query_lower = candidate.discovery_query.lower()
    title_lower = candidate.result_title.lower()

    if candidate.discovery_key not in configured_boards:
        score += 1.8
        reasons.append("new board outside configured seed set")
    else:
        score += 0.6
        reasons.append("known configured board")

    core_titles = [item.lower() for item in (profile.core_titles_json or profile.preferred_titles_json or [])]
    adjacent_titles = [item.lower() for item in (profile.adjacent_titles_json or [])]
    if any(title in query_lower or title in title_lower for title in core_titles):
        score += 1.5
        reasons.append("matched core title")
    elif any(title in query_lower or title in title_lower for title in adjacent_titles):
        score += 0.9
        reasons.append("matched adjacent title")

    if any(domain.lower() in query_lower or domain.lower() in title_lower for domain in (profile.preferred_domains_json or [])):
        score += 0.7
        reasons.append("matched preferred domain theme")

    if any(term in query_lower for term in ["careers", "greenhouse", "ashby", "startup"]):
        score += 0.4
        reasons.append("query indicates job-surface intent")

    if existing:
        candidate.is_new = False
        score += min(existing.utility_score, 3.0)
        if existing.last_expansion_result_count == 0 and existing.last_expanded_at:
            cooldown_cutoff = datetime.utcnow() - timedelta(minutes=settings.discovery_company_cooldown_minutes)
            if existing.last_expanded_at >= cooldown_cutoff:
                score -= 2.5
                reasons.append("recent empty expansion cooldown")
        if existing.blocked_reason:
            score -= 1.5
            reasons.append(f"existing blocked reason: {existing.blocked_reason}")
    else:
        candidate.is_new = True
        score += 1.0
        reasons.append("newly discovered company")

    return round(score, 2), reasons, existing


def upsert_discovered_company(
    session: Session,
    candidate: CompanyDiscoveryCandidate,
    triage_score: float,
    triage_reasons: list[str],
) -> tuple[CompanyDiscovery, bool]:
    row = session.scalar(select(CompanyDiscovery).where(CompanyDiscovery.discovery_key == candidate.discovery_key))
    now = datetime.utcnow()
    metadata = {
        "result_url": candidate.result_url,
        "result_title": candidate.result_title,
        "triage_score": triage_score,
        "triage_reasons": triage_reasons,
    }
    if row:
        row.company_name = candidate.company_name
        row.company_domain = candidate.company_domain or row.company_domain
        row.normalized_company_key = candidate.normalized_company_key
        row.discovery_source = candidate.discovery_source
        row.discovery_query = candidate.discovery_query
        row.last_discovered_at = now
        row.metadata_json = {**(row.metadata_json or {}), **metadata}
        session.flush()
        return row, False

    row = CompanyDiscovery(
        discovery_key=candidate.discovery_key,
        company_name=candidate.company_name,
        company_domain=candidate.company_domain,
        normalized_company_key=candidate.normalized_company_key,
        discovery_source=candidate.discovery_source,
        discovery_query=candidate.discovery_query,
        first_discovered_at=now,
        last_discovered_at=now,
        board_type=candidate.board_type,
        board_locator=candidate.board_locator,
        expansion_status="discovered",
        metadata_json=metadata,
    )
    session.add(row)
    session.flush()
    return row, True


def select_candidates_for_expansion(
    rows: list[tuple[CompanyDiscoveryCandidate, CompanyDiscovery, float, list[str]]],
    settings: Settings | None = None,
) -> list[tuple[CompanyDiscoveryCandidate, CompanyDiscovery, float, list[str]]]:
    settings = settings or get_settings()
    ranked = sorted(
        rows,
        key=lambda item: (
            item[1].visible_yield_count > 0,
            item[2],
            item[1].utility_score,
            item[0].is_new,
        ),
        reverse=True,
    )
    selected: list[tuple[CompanyDiscoveryCandidate, CompanyDiscovery, float, list[str]]] = []
    new_count = 0
    for item in ranked:
        candidate, row, _, _ = item
        if len(selected) >= settings.discovery_max_expansions_per_cycle:
            break
        if candidate.is_new and new_count >= settings.discovery_max_new_companies_per_cycle:
            continue
        cooldown_cutoff = datetime.utcnow() - timedelta(minutes=settings.discovery_company_cooldown_minutes)
        if row.last_expanded_at and row.last_expansion_result_count == 0 and row.last_expanded_at >= cooldown_cutoff:
            continue
        selected.append(item)
        if candidate.is_new:
            new_count += 1
    return selected


def record_expansion_attempt(
    row: CompanyDiscovery,
    result_count: int,
    visible_yield: int = 0,
    suppressed_yield: int = 0,
    location_filtered: int = 0,
    blocked_reason: Optional[str] = None,
    count_attempt: bool = True,
) -> None:
    row.last_expanded_at = datetime.utcnow()
    if count_attempt:
        row.expansion_attempts += 1
    row.last_expansion_result_count = result_count
    row.visible_yield_count += visible_yield
    row.suppressed_yield_count += suppressed_yield
    row.location_filtered_count += location_filtered
    row.blocked_reason = blocked_reason
    if blocked_reason == "investigate":
        row.expansion_status = "investigate"
        row.utility_score = round(max(row.utility_score, 0.5), 2)
        return
    if result_count == 0:
        row.expansion_status = "empty"
        row.utility_score = round(row.utility_score - 0.8, 2)
    else:
        row.expansion_status = "expanded"
        row.utility_score = round(
            row.utility_score
            + (visible_yield * 1.4)
            - (suppressed_yield * 0.15)
            - (location_filtered * 0.35),
            2,
        )


def build_discovery_status(session: Session) -> DiscoveryStatusResponse:
    from services.discovery_agents import recent_discovery_agent_runs, summarize_expansion_actions

    since = datetime.utcnow() - timedelta(hours=24)
    recent_runs = recent_discovery_agent_runs(session)
    rows = session.scalars(
        select(CompanyDiscovery)
        .order_by(CompanyDiscovery.last_discovered_at.desc(), CompanyDiscovery.utility_score.desc())
        .limit(25)
    ).all()
    planner_run = next((run for run in recent_runs if run["agent_name"] == "Planner"), None)
    triage_run = next((run for run in recent_runs if run["agent_name"] == "Triage"), None)
    learning_run = next((run for run in recent_runs if run["agent_name"] == "Learning"), None)
    latest_metrics_run = next(
        (
            run
            for run in recent_runs
            if run["agent_name"] == "Discovery" and run["action"] == "recorded discovery cycle metrics"
        ),
        None,
    )
    geography_rejections = session.scalars(
        select(Lead)
        .where(Lead.updated_at >= since)
        .order_by(Lead.updated_at.desc())
        .limit(40)
    ).all()
    agentic_leads = session.scalars(
        select(Lead)
        .where(Lead.hidden.is_(False))
        .order_by(Lead.updated_at.desc())
        .limit(30)
    ).all()
    return DiscoveryStatusResponse(
        total_known_companies=session.scalar(select(func.count(CompanyDiscovery.id))) or 0,
        discovered_last_24h=session.scalar(select(func.count(CompanyDiscovery.id)).where(CompanyDiscovery.last_discovered_at >= since)) or 0,
        expanded_last_24h=session.scalar(select(func.count(CompanyDiscovery.id)).where(CompanyDiscovery.last_expanded_at >= since)) or 0,
        latest_planner_run=planner_run,
        recent_plans=recent_runs,
        recent_expansions=summarize_expansion_actions(rows),
        recent_successful_expansions=[
            item for item in summarize_expansion_actions(rows) if int(item.get("last_expansion_result_count", 0) or 0) > 0
        ][:10],
        recent_visible_yield=[
            CompanyDiscoveryRowResponse(
                company_name=row.company_name,
                company_domain=row.company_domain,
                normalized_company_key=row.normalized_company_key,
                discovery_source=row.discovery_source,
                discovery_query=row.discovery_query,
                first_discovered_at=row.first_discovered_at,
                last_discovered_at=row.last_discovered_at,
                last_expanded_at=row.last_expanded_at,
                board_type=row.board_type,
                board_locator=row.board_locator,
                surface_provenance=(row.metadata_json or {}).get("surface_provenance"),
                source_lineage=(row.metadata_json or {}).get("source_lineage"),
                expansion_status=row.expansion_status,
                expansion_attempts=row.expansion_attempts,
                last_expansion_result_count=row.last_expansion_result_count,
                visible_yield_count=row.visible_yield_count,
                suppressed_yield_count=row.suppressed_yield_count,
                location_filtered_count=row.location_filtered_count,
                utility_score=row.utility_score,
                blocked_reason=row.blocked_reason,
                metadata_json=row.metadata_json or {},
            )
            for row in rows
            if row.visible_yield_count > 0
        ][:10],
        blocked_or_cooled_down=[
            CompanyDiscoveryRowResponse(
                company_name=row.company_name,
                company_domain=row.company_domain,
                normalized_company_key=row.normalized_company_key,
                discovery_source=row.discovery_source,
                discovery_query=row.discovery_query,
                first_discovered_at=row.first_discovered_at,
                last_discovered_at=row.last_discovered_at,
                last_expanded_at=row.last_expanded_at,
                board_type=row.board_type,
                board_locator=row.board_locator,
                surface_provenance=(row.metadata_json or {}).get("surface_provenance"),
                source_lineage=(row.metadata_json or {}).get("source_lineage"),
                expansion_status=row.expansion_status,
                expansion_attempts=row.expansion_attempts,
                last_expansion_result_count=row.last_expansion_result_count,
                visible_yield_count=row.visible_yield_count,
                suppressed_yield_count=row.suppressed_yield_count,
                location_filtered_count=row.location_filtered_count,
                utility_score=row.utility_score,
                blocked_reason=row.blocked_reason,
                metadata_json=row.metadata_json or {},
            )
            for row in rows
            if row.blocked_reason or row.expansion_status in {"empty", "investigate"}
        ][:10],
        recent_greenhouse_tokens=[
            {
                "company_name": row.company_name,
                "token": token,
                "board_locator": row.board_locator,
                "surface_provenance": (row.metadata_json or {}).get("surface_provenance"),
                "last_discovered_at": row.last_discovered_at.isoformat(),
                "expansion_status": row.expansion_status,
            }
            for row in rows
            for token in ((row.metadata_json or {}).get("greenhouse_tokens") or ([row.board_locator] if row.board_type == "greenhouse" else []))
        ][:12],
        recent_ashby_identifiers=[
            {
                "company_name": row.company_name,
                "identifier": identifier,
                "board_locator": row.board_locator,
                "surface_provenance": (row.metadata_json or {}).get("surface_provenance"),
                "last_discovered_at": row.last_discovered_at.isoformat(),
                "expansion_status": row.expansion_status,
            }
            for row in rows
            for identifier in ((row.metadata_json or {}).get("ashby_identifiers") or ([row.board_locator] if row.board_type == "ashby" else []))
        ][:12],
        recent_geography_rejections=[
            {
                "company_name": lead.company_name,
                "title": lead.primary_title,
                "location_scope": (lead.evidence_json or {}).get("location_scope"),
                "location_reason": (lead.evidence_json or {}).get("location_reason"),
                "suppression_category": (lead.evidence_json or {}).get("suppression_category"),
                "source_provenance": (lead.evidence_json or {}).get("source_provenance"),
                "source_lineage": (lead.evidence_json or {}).get("source_lineage"),
            }
            for lead in geography_rejections
            if (lead.evidence_json or {}).get("suppression_category") == "location"
        ][:12],
        recent_agentic_leads=[
            {
                "company_name": lead.company_name,
                "title": lead.primary_title,
                "source_platform": (lead.evidence_json or {}).get("source_platform"),
                "source_provenance": (lead.evidence_json or {}).get("source_provenance"),
                "source_lineage": (lead.evidence_json or {}).get("source_lineage"),
                "discovery_source": (lead.evidence_json or {}).get("discovery_source"),
                "rank_label": lead.rank_label,
                "confidence_label": lead.confidence_label,
                "updated_at": lead.updated_at.isoformat() if lead.updated_at else None,
            }
            for lead in agentic_leads
            if (lead.evidence_json or {}).get("discovery_source") == "search_web"
        ][:12],
        next_recommended_queries=[
            note
            for run in recent_runs
            if run["agent_name"] == "Learning"
            for note in (run["metadata_json"].get("next_queries") or [])
        ][:8],
        latest_openai_usage={
            "planner": bool((planner_run or {}).get("metadata_json", {}).get("used_openai")),
            "triage": bool((triage_run or {}).get("metadata_json", {}).get("used_openai")),
            "learning": bool((learning_run or {}).get("metadata_json", {}).get("used_openai")),
        },
        cycle_metrics=dict((latest_metrics_run or {}).get("metadata_json", {}).get("cycle_metrics", {})),
        recent_items=[
            CompanyDiscoveryRowResponse(
                company_name=row.company_name,
                company_domain=row.company_domain,
                normalized_company_key=row.normalized_company_key,
                discovery_source=row.discovery_source,
                discovery_query=row.discovery_query,
                first_discovered_at=row.first_discovered_at,
                last_discovered_at=row.last_discovered_at,
                last_expanded_at=row.last_expanded_at,
                board_type=row.board_type,
                board_locator=row.board_locator,
                surface_provenance=(row.metadata_json or {}).get("surface_provenance"),
                source_lineage=(row.metadata_json or {}).get("source_lineage"),
                expansion_status=row.expansion_status,
                expansion_attempts=row.expansion_attempts,
                last_expansion_result_count=row.last_expansion_result_count,
                visible_yield_count=row.visible_yield_count,
                suppressed_yield_count=row.suppressed_yield_count,
                location_filtered_count=row.location_filtered_count,
                utility_score=row.utility_score,
                blocked_reason=row.blocked_reason,
                metadata_json=row.metadata_json or {},
            )
            for row in rows
        ],
    )


def summarize_source_mix(rows: list[CompanyDiscovery]) -> dict[str, int]:
    return dict(Counter(row.board_type for row in rows))
