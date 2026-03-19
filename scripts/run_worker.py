from __future__ import annotations

import signal
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import get_settings
from core.db import SessionLocal, init_db
from core.logging import configure_logging, get_logger
from services.activity import log_agent_activity, log_agent_failure
from services.alerts import evaluate_alerts
from services.ops import autonomy_enabled, get_runtime_connector_set
from services.pipeline import run_full_pipeline


logger = get_logger(__name__)
STOP_REQUESTED = False


def _handle_signal(signum, _frame) -> None:  # pragma: no cover
    global STOP_REQUESTED
    STOP_REQUESTED = True
    logger.info("Worker received signal %s and will exit after the current cycle.", signum)


def run_worker_loop() -> None:
    configure_logging()
    init_db()
    settings = get_settings()
    logger.info("Starting Opportunity Scout worker in %s mode.", "demo" if settings.demo_mode else "live")

    while not STOP_REQUESTED:
        settings = get_settings()
        if not autonomy_enabled(settings):
            with SessionLocal() as session:
                log_agent_activity(
                    session,
                    "Worker",
                    "autonomy disabled",
                    "Worker is idle because the global autonomy kill switch is active.",
                    target_type="worker",
                    target_count=0,
                )
                evaluate_alerts(session, settings=settings)
                session.commit()
            time.sleep(settings.worker_interval_seconds)
            continue

        source_mode, enabled_connectors, strict_live_connectors = get_runtime_connector_set(settings)
        if not enabled_connectors:
            with SessionLocal() as session:
                log_agent_activity(
                    session,
                    "Worker",
                    "connector disabled",
                    "Worker is idle because no live connectors are enabled.",
                    target_type="worker",
                    target_count=0,
                )
                evaluate_alerts(session, settings=settings)
                session.commit()
            time.sleep(settings.worker_interval_seconds)
            continue

        try:
            with SessionLocal() as session:
                response = run_full_pipeline(
                    session,
                    source_mode=source_mode,
                    enabled_connectors=enabled_connectors,
                    strict_live_connectors=strict_live_connectors,
                )
                log_agent_activity(
                    session,
                    "Worker",
                    "completed cycle",
                    f"Worker cycle completed at {datetime.utcnow().isoformat()}. {response.summary}",
                    target_type="worker",
                    target_count=1,
                )
                evaluate_alerts(session, settings=settings)
                session.commit()
        except Exception as exc:  # pragma: no cover
            logger.exception("Worker cycle failed: %s", exc)
            with SessionLocal() as session:
                session.rollback()
                log_agent_failure(session, "Worker", "run cycle", f"Worker loop failed: {exc}")
                evaluate_alerts(session, settings=settings)
                session.commit()

        if STOP_REQUESTED:
            break
        time.sleep(settings.worker_interval_seconds)


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    run_worker_loop()


if __name__ == "__main__":
    main()
