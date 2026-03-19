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
from services.activity import log_agent_failure
from services.alerts import evaluate_alerts
from services.runtime_control import effective_worker_interval_seconds, get_runtime_control, mark_worker_state, mark_cycle_error
from services.worker_runtime import run_worker_cycle


logger = get_logger(__name__)
STOP_REQUESTED = False


def _handle_signal(signum, _frame) -> None:  # pragma: no cover
    global STOP_REQUESTED
    STOP_REQUESTED = True
    logger.info("Worker received signal %s and will exit after the current cycle.", signum)
    try:
        with SessionLocal() as session:
            control = get_runtime_control(session)
            mark_worker_state(control, "stopping", f"Shutdown signal {signum} received. Worker is stopping.")
            session.commit()
    except Exception:  # pragma: no cover
        logger.exception("Failed to persist worker stopping state after signal %s.", signum)


def _persist_worker_state(state: str, message: str, sleep_seconds: int | None = None) -> None:
    try:
        with SessionLocal() as session:
            control = get_runtime_control(session)
            mark_worker_state(control, state, message, sleep_seconds=sleep_seconds)
            session.commit()
    except Exception:  # pragma: no cover
        logger.exception("Failed to persist worker state %s.", state)


def _runtime_change_detected(expected_run_state: str) -> bool:
    try:
        with SessionLocal() as session:
            control = get_runtime_control(session)
            if control.run_once_requested:
                return True
            return control.run_state != expected_run_state
    except Exception:  # pragma: no cover
        logger.exception("Failed to inspect runtime control during worker sleep.")
        return False


def _sleep_interruptibly(
    total_seconds: int,
    expected_run_state: str,
    state_label: str,
    status_message: str,
    poll_seconds: float = 1.0,
) -> None:
    deadline = time.time() + max(total_seconds, 0)
    while not STOP_REQUESTED and time.time() < deadline:
        if _runtime_change_detected(expected_run_state):
            return
        remaining = max(deadline - time.time(), 0.0)
        _persist_worker_state(state_label, status_message.format(seconds=round(remaining, 1)), sleep_seconds=int(remaining))
        time.sleep(min(poll_seconds, remaining))


def run_worker_loop() -> None:
    configure_logging()
    init_db()
    settings = get_settings()
    logger.info("Starting Opportunity Scout worker in %s mode.", "demo" if settings.demo_mode else "live")
    _persist_worker_state("idle", "Worker started and is waiting for runtime instructions.")

    while not STOP_REQUESTED:
        settings = get_settings()
        outcome = {"state": "idle", "summary": "Idle.", "ran": False}
        try:
            with SessionLocal() as session:
                outcome = run_worker_cycle(session, settings)
                evaluate_alerts(session, settings=settings)
                session.commit()
        except Exception as exc:  # pragma: no cover
            logger.exception("Worker cycle failed: %s", exc)
            with SessionLocal() as session:
                session.rollback()
                control = get_runtime_control(session)
                mark_cycle_error(control, f"Worker error: {exc}")
                log_agent_failure(session, "Worker", "run cycle", f"Worker loop failed: {exc}")
                evaluate_alerts(session, settings=settings)
                session.commit()
            outcome = {"state": "error", "summary": str(exc), "ran": False}

        if STOP_REQUESTED:
            break
        sleep_seconds = effective_worker_interval_seconds(settings)
        if outcome.get("state") in {"paused", "disabled", "no_connectors"}:
            sleep_seconds = 1
        expected_run_state = "paused" if outcome.get("state") == "paused" else "running"
        state_label = "sleeping"
        status_message = "Worker sleeping for {seconds} more seconds before the next cycle."
        if outcome.get("state") == "paused":
            state_label = "paused"
            status_message = "Worker is paused and polling for control changes. Next check in {seconds} seconds."
        elif outcome.get("state") in {"disabled", "no_connectors"}:
            state_label = "idle"
            status_message = "Worker is idle and polling for control changes. Next check in {seconds} seconds."
        _sleep_interruptibly(
            sleep_seconds,
            expected_run_state=expected_run_state,
            state_label=state_label,
            status_message=status_message,
        )

    _persist_worker_state("stopping", "Worker has stopped.")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    run_worker_loop()


if __name__ == "__main__":
    main()
