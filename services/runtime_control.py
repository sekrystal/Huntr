from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config import Settings, get_settings
from core.models import RuntimeControl
from core.schemas import RuntimeControlResponse


def get_runtime_control(session: Session, settings: Settings | None = None) -> RuntimeControl:
    control = session.scalar(select(RuntimeControl).order_by(RuntimeControl.id.asc()))
    if control:
        return control

    default_state = "running" if settings and settings.autonomy_enabled and not settings.demo_mode else "paused"
    control = RuntimeControl(run_state=default_state)
    session.add(control)
    session.flush()
    return control


def effective_worker_interval_seconds(settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    if settings.demo_mode:
        return max(1, min(settings.worker_interval_seconds, settings.interactive_worker_interval_seconds))
    return max(1, settings.worker_interval_seconds)


def runtime_control_payload(control: RuntimeControl, settings: Settings | None = None) -> RuntimeControlResponse:
    settings = settings or get_settings()
    return RuntimeControlResponse(
        run_state=control.run_state,
        worker_state=control.worker_state,
        run_once_requested=control.run_once_requested,
        last_cycle_started_at=control.last_cycle_started_at,
        last_successful_cycle_at=control.last_successful_cycle_at,
        last_heartbeat_at=control.last_heartbeat_at,
        sleep_until=control.sleep_until,
        next_cycle_at=control.sleep_until,
        current_interval_seconds=control.current_interval_seconds or effective_worker_interval_seconds(settings),
        status_message=control.status_message,
        last_control_action=control.last_control_action,
        last_control_at=control.last_control_at,
        last_cycle_summary=control.last_cycle_summary,
    )


def set_runtime_action(session: Session, action: str, settings: Settings | None = None) -> RuntimeControl:
    control = get_runtime_control(session, settings=settings)
    if action == "play":
        control.run_state = "running"
        control.run_once_requested = False
        control.worker_state = "idle"
        control.sleep_until = None
        control.status_message = "Worker will start the next cycle immediately."
    elif action == "pause":
        control.run_state = "paused"
        control.run_once_requested = False
        control.worker_state = "paused"
        control.sleep_until = None
        control.status_message = "Pause requested. The worker will stop after the current bounded step."
    elif action == "run_once":
        control.run_once_requested = True
        control.worker_state = "idle"
        control.sleep_until = None
        control.status_message = "Single cycle requested."
    control.last_heartbeat_at = datetime.utcnow()
    control.last_control_action = action
    control.last_control_at = control.last_heartbeat_at
    session.flush()
    return control


def determine_cycle_mode(session: Session, settings: Settings) -> tuple[str, RuntimeControl]:
    control = get_runtime_control(session, settings=settings)
    if not settings.autonomy_enabled:
        return "disabled", control
    if control.run_state == "paused" and not control.run_once_requested:
        return "paused", control
    if control.run_once_requested:
        return "run_once", control
    return "running", control


def mark_cycle_started(control: RuntimeControl) -> None:
    control.last_cycle_started_at = datetime.utcnow()
    control.last_heartbeat_at = control.last_cycle_started_at
    control.worker_state = "running_cycle"
    control.sleep_until = None
    control.current_interval_seconds = 0
    control.status_message = "Worker is running a pipeline cycle."


def mark_cycle_success(control: RuntimeControl, summary: str, consume_run_once: bool = True) -> None:
    control.last_successful_cycle_at = datetime.utcnow()
    control.last_heartbeat_at = control.last_successful_cycle_at
    control.worker_state = "idle"
    control.sleep_until = None
    control.current_interval_seconds = 0
    control.status_message = "Worker finished the last cycle successfully."
    control.last_cycle_summary = summary
    if consume_run_once:
        control.run_once_requested = False


def mark_worker_state(
    control: RuntimeControl,
    state: str,
    message: str,
    sleep_seconds: int | None = None,
) -> None:
    control.worker_state = state
    control.last_heartbeat_at = datetime.utcnow()
    control.status_message = message
    control.current_interval_seconds = max(sleep_seconds or 0, 0)
    control.sleep_until = (
        control.last_heartbeat_at + timedelta(seconds=max(sleep_seconds, 0))
        if sleep_seconds is not None
        else None
    )


def mark_cycle_error(control: RuntimeControl, message: str) -> None:
    control.worker_state = "error"
    control.last_heartbeat_at = datetime.utcnow()
    control.sleep_until = None
    control.current_interval_seconds = 0
    control.status_message = message
