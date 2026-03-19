from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import Settings
from core.db import SessionLocal, init_db, reset_sqlite_db
from core.models import AgentActivity, AgentRun, ConnectorHealth, FollowUpTask, Investigation, Lead, WatchlistItem
from scripts.seed_demo_data import main as seed_demo_main
from services.autonomy import build_autonomy_health, build_latest_run_digest, list_connector_health
from services.pipeline import run_full_pipeline


def _collect_connector_events(history: list[dict]) -> tuple[list[str], int, int]:
    transitions: list[str] = []
    circuit_events = 0
    recoveries = 0
    previous: dict[str, tuple[str, str]] = {}
    for snapshot in history:
        for row in snapshot.get("connector_health", []):
            current = (row["status"], row["circuit_state"])
            last = previous.get(row["connector_name"])
            if last and last != current:
                transitions.append(
                    f"cycle {snapshot['cycle']}: {row['connector_name']} {last[0]}/{last[1]} -> {current[0]}/{current[1]}"
                )
                if current[1] == "open":
                    circuit_events += 1
                if current[0] in {"recovering", "healthy"} and last[0] in {"failed", "circuit_open", "degraded"}:
                    recoveries += 1
            previous[row["connector_name"]] = current
    return transitions, circuit_events, recoveries


def _collect_results(
    cycles: int,
    settings: Settings,
    mode: str,
    connector_family: str,
    started_at: float,
    cycle_history: list[dict],
) -> dict:
    with SessionLocal() as session:
        leads = session.query(Lead).all()
        open_investigations = session.query(Investigation).filter(Investigation.status.in_(["open", "rechecking"])).count()
        duplicate_counter = Counter(
            (lead.company_name, lead.primary_title, lead.lead_type, lead.listing_id, lead.signal_id) for lead in leads
        )
        duplicate_keys = [key for key, count in duplicate_counter.items() if count > 1]
        activities = session.query(AgentActivity).order_by(AgentActivity.created_at.desc()).all()
        runs = session.query(AgentRun).order_by(AgentRun.created_at.desc()).all()
        watchlist_count = session.query(WatchlistItem).count()
        follow_up_count = session.query(FollowUpTask).count()
        hidden_visible = [
            f"{lead.company_name} / {lead.primary_title}"
            for lead in leads
            if not lead.hidden and lead.freshness_label in {"stale", "unknown"}
        ]
        latest_digest = build_latest_run_digest(session).model_dump()
        latest_health = build_autonomy_health(session, settings=settings).model_dump()
        connector_health = [row.model_dump() for row in list_connector_health(session)]
        pipeline_runs = [run for run in runs if run.agent_name == "Pipeline" and run.status == "ok"]
        failed_runs = [run for run in runs if run.status == "failed"]
        quarantined_rows = sum(row.quarantine_count for row in session.query(ConnectorHealth).all())
        transitions, circuit_events, recoveries = _collect_connector_events(cycle_history)
        anomalies = []
        if duplicate_keys:
            anomalies.append("duplicate_leads_detected")
        if hidden_visible:
            anomalies.append("visible_stale_rows_detected")
        if failed_runs:
            anomalies.append("failed_runs_detected")
        if watchlist_count > 40:
            anomalies.append("watchlist_growth_unbounded")
        return {
            "mode": mode,
            "connector_family": connector_family,
            "actual_duration_seconds": round(time.time() - started_at, 2),
            "cycles_requested": cycles,
            "pipeline_runs": len(pipeline_runs),
            "successful_runs": len(pipeline_runs),
            "total_failures": len(failed_runs),
            "failed_runs": [run.summary for run in failed_runs][:20],
            "health_transitions": transitions,
            "duplicate_lead_keys": duplicate_keys,
            "visible_stale_rows": hidden_visible,
            "open_investigations": open_investigations,
            "watchlist_count": watchlist_count,
            "follow_up_count": follow_up_count,
            "quarantined_rows": quarantined_rows,
            "circuit_breaker_events": circuit_events,
            "recoveries": recoveries,
            "activity_count": len(activities),
            "anomalies": anomalies,
            "cycle_summaries": cycle_history,
            "latest_digest": latest_digest,
            "latest_health": latest_health,
            "connector_health": connector_health,
        }


def _write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".json":
        path.write_text(json.dumps(payload, indent=2, default=str))
        return
    lines = [
        f"# Live Soak Report",
        "",
        f"- Mode: `{payload['mode']}`",
        f"- Connector family: `{payload['connector_family']}`",
        f"- Actual duration: `{payload['actual_duration_seconds']}s`",
        f"- Cycles: `{payload['successful_runs']}/{payload['cycles_requested']}`",
        f"- Failures: `{payload['total_failures']}`",
        f"- Circuit breaker events: `{payload['circuit_breaker_events']}`",
        f"- Recoveries: `{payload['recoveries']}`",
        f"- Quarantined rows: `{payload['quarantined_rows']}`",
        f"- Visible stale rows: `{len(payload['visible_stale_rows'])}`",
        f"- Duplicate lead keys: `{len(payload['duplicate_lead_keys'])}`",
        "",
        "## Latest Digest",
        payload.get("latest_digest", {}).get("summary") or "No digest recorded.",
        "",
        "## Connector Health",
    ]
    for row in payload.get("connector_health", []):
        lines.append(
            f"- `{row['connector_name']}`: status=`{row['status']}`, circuit=`{row['circuit_state']}`, "
            f"mode=`{row.get('last_mode')}`, items=`{row.get('last_item_count', 0)}`, "
            f"trust=`{row.get('trust_score', 0.0)}`, approved=`{row.get('approved_for_unattended', False)}`"
        )
    if payload.get("health_transitions"):
        lines.extend(["", "## Health Transitions", *[f"- {item}" for item in payload["health_transitions"][:20]]])
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a live or accelerated autonomy soak test.")
    parser.add_argument("--cycles", type=int, default=6)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--initial-delay", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--mode", choices=["demo", "live"], default="demo")
    parser.add_argument("--connector-family", default="greenhouse")
    parser.add_argument("--report-file", default="")
    args = parser.parse_args()

    reset_sqlite_db()
    init_db()
    if args.mode == "demo":
        seed_demo_main()

    settings = Settings(
        enable_scheduler=False,
        sync_interval_seconds=max(int(args.interval), 1),
        scheduler_initial_delay_seconds=max(int(args.initial_delay), 0),
        scheduler_max_cycles=args.cycles,
    )

    enabled_connectors = {args.connector_family}
    strict_live_connectors = enabled_connectors if args.mode == "live" else set()
    source_mode = "live" if args.mode == "live" else "demo"

    time.sleep(args.initial_delay)
    started_at = time.time()
    cycle_history: list[dict] = []
    for cycle in range(1, args.cycles + 1):
        with SessionLocal() as session:
            response = run_full_pipeline(
                session,
                source_mode=source_mode,
                enabled_connectors=enabled_connectors,
                strict_live_connectors=strict_live_connectors,
            )
            session.commit()
            latest_digest = build_latest_run_digest(session).model_dump()
            connector_health = [row.model_dump() for row in list_connector_health(session)]
            cycle_history.append(
                {
                    "cycle": cycle,
                    "run_at": datetime.now(timezone.utc).isoformat(),
                    "summary": response.summary,
                    "digest": latest_digest,
                    "connector_health": connector_health,
                }
            )
        if cycle < args.cycles:
            time.sleep(args.interval)
        if time.time() - started_at > args.timeout:
            break

    payload = _collect_results(len(cycle_history), settings, args.mode, args.connector_family, started_at, cycle_history)
    if args.report_file:
        _write_report(Path(args.report_file), payload)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
