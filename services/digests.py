from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.models import AgentRun, DailyDigest, RunDigest


def record_run_digest(
    session: Session,
    agent_run: AgentRun,
    summary: str,
    new_leads: list[str],
    suppressed_leads: list[str],
    investigations_changed: int,
    follow_ups_created: list[str],
    watchlist_changes: list[str],
    failures: list[str] | None = None,
) -> RunDigest:
    digest = session.scalar(select(RunDigest).where(RunDigest.agent_run_id == agent_run.id))
    if not digest:
        digest = RunDigest(agent_run_id=agent_run.id, run_type="pipeline", summary=summary)
        session.add(digest)
    digest.summary = summary
    digest.new_leads_json = list(dict.fromkeys(new_leads))
    digest.suppressed_leads_json = list(dict.fromkeys(suppressed_leads))
    digest.investigations_changed = investigations_changed
    digest.follow_ups_created_json = list(dict.fromkeys(follow_ups_created))
    digest.watchlist_changes_json = list(dict.fromkeys(watchlist_changes))
    digest.failures_json = list(dict.fromkeys(failures or []))
    digest.is_noop = not any(
        [
            digest.new_leads_json,
            digest.suppressed_leads_json,
            digest.investigations_changed,
            digest.follow_ups_created_json,
            digest.watchlist_changes_json,
            digest.failures_json,
        ]
    )
    session.flush()
    update_daily_digest(session, digest.created_at.date().isoformat())
    return digest


def update_daily_digest(session: Session, digest_date: str) -> DailyDigest:
    run_digests = session.scalars(
        select(RunDigest).where(RunDigest.created_at >= datetime.fromisoformat(digest_date))
    ).all()
    daily = session.scalar(select(DailyDigest).where(DailyDigest.digest_date == digest_date))
    if not daily:
        daily = DailyDigest(digest_date=digest_date, summary="")
        session.add(daily)
    new_leads: list[str] = []
    suppressed: list[str] = []
    follow_ups: list[str] = []
    watchlist: list[str] = []
    failures: list[str] = []
    investigations_changed = 0
    for digest in run_digests:
        new_leads.extend(digest.new_leads_json or [])
        suppressed.extend(digest.suppressed_leads_json or [])
        follow_ups.extend(digest.follow_ups_created_json or [])
        watchlist.extend(digest.watchlist_changes_json or [])
        failures.extend(digest.failures_json or [])
        investigations_changed += digest.investigations_changed or 0
    daily.new_leads_json = list(dict.fromkeys(new_leads))
    daily.suppressed_leads_json = list(dict.fromkeys(suppressed))
    daily.follow_ups_created_json = list(dict.fromkeys(follow_ups))
    daily.watchlist_changes_json = list(dict.fromkeys(watchlist))
    daily.failures_json = list(dict.fromkeys(failures))
    daily.investigations_changed = investigations_changed
    daily.summary = (
        f"Daily digest: {len(daily.new_leads_json)} new leads, "
        f"{len(daily.suppressed_leads_json)} suppressed leads, "
        f"{daily.investigations_changed} investigation changes, "
        f"{len(daily.follow_ups_created_json)} follow-ups, "
        f"{len(daily.watchlist_changes_json)} watchlist/query changes, "
        f"{len(daily.failures_json)} failures."
    )
    session.flush()
    return daily
