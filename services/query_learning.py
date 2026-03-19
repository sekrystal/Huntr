from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import SourceQuery
from services.ops import can_create_generated_queries_today
from services.learning import get_or_create_query_stat


BASELINE_QUERIES = [
    "hiring founding ops",
    "hiring chief of staff",
    "first ops hire",
    "deployment strategist hiring",
    "technical pm hiring",
    "hiring in sf",
    "hiring in nyc",
    "ai infra hiring",
]


def ensure_source_queries(session: Session) -> list[SourceQuery]:
    queries: list[SourceQuery] = []
    for query_text in BASELINE_QUERIES:
        existing = session.scalar(
            select(SourceQuery).where(SourceQuery.query_text == query_text, SourceQuery.source_type == "x")
        )
        if existing:
            queries.append(existing)
            continue
        item = SourceQuery(
            query_text=query_text,
            source_type="x",
            status="active",
            performance_stats_json={"leads_generated": 0, "likes": 0, "applies": 0, "dislikes": 0},
        )
        session.add(item)
        session.flush()
        get_or_create_query_stat(session, source_type="x", query_text=query_text, status="active")
        queries.append(item)
    return queries


def generate_queries_from_preferences(
    titles: list[str],
    domains: list[str],
    role_families: list[str],
    evidence_snippets: list[str],
) -> list[str]:
    candidates = set()
    for title in titles[:3]:
        candidates.add(f"hiring {title}")
        candidates.add(f"{title} startup hiring")
    for domain in domains[:2]:
        candidates.add(f"{domain} hiring ops")
    for family in role_families[:2]:
        if family == "operations":
            candidates.add("first operations hire")
        elif family == "go_to_market":
            candidates.add("deployment hiring startup")
        else:
            candidates.add(f"{family} startup hiring")
    for snippet in evidence_snippets[:2]:
        lowered = snippet.lower()
        if "customer" in lowered:
            candidates.add("customer deployment startup hiring")
        if "systems" in lowered:
            candidates.add("systems builder startup hiring")
    return sorted(candidates)


def upsert_generated_queries(session: Session, query_texts: list[str]) -> list[str]:
    created: list[str] = []
    remaining_budget = can_create_generated_queries_today(session, requested=len(query_texts))
    for query_text in query_texts:
        existing = session.scalar(
            select(SourceQuery).where(SourceQuery.query_text == query_text, SourceQuery.source_type == "x")
        )
        if existing:
            get_or_create_query_stat(session, source_type="x", query_text=query_text, status=existing.status)
            continue
        if remaining_budget <= 0:
            break
        session.add(
            SourceQuery(
                query_text=query_text,
                source_type="x",
                status="generated",
                performance_stats_json={"leads_generated": 0, "likes": 0, "applies": 0, "dislikes": 0},
            )
        )
        get_or_create_query_stat(session, source_type="x", query_text=query_text, status="generated")
        created.append(query_text)
        remaining_budget -= 1
    session.flush()
    return created
