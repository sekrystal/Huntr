from __future__ import annotations

from datetime import datetime

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config import Settings
from core.models import Base, CandidateProfile, Lead, Listing
from core.schemas import AgentRunResponse, RuntimeControlRequest
import scripts.run_worker as worker_script
from services import ai_judges
from services.runtime_control import determine_cycle_mode, effective_worker_interval_seconds, get_runtime_control, runtime_control_payload, set_runtime_action
from services.sync import evaluate_critic_decision
from services.worker_runtime import run_worker_cycle


def build_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def seed_profile(session):
    session.add(
        CandidateProfile(
            name="Tester",
            raw_resume_text="Senior operator focused on deployment, business operations, and chief of staff work.",
            core_titles_json=["deployment strategist", "chief of staff"],
            preferred_locations_json=["Remote", "San Francisco"],
            minimum_fit_threshold=2.8,
        )
    )
    session.commit()


def test_openai_wrapper_returns_none_when_disabled(monkeypatch) -> None:
    monkeypatch.setattr(ai_judges, "get_settings", lambda: Settings(database_url="sqlite:///:memory:", openai_enabled=False))
    assert ai_judges.call_openai_json("demo", {"type": "object", "properties": {}, "required": [], "additionalProperties": False}, "system", "user") is None


def test_openai_payload_uses_instructions_and_string_input() -> None:
    payload = ai_judges.build_openai_request_payload(
        schema_name="critic_judgment",
        schema={"type": "object", "properties": {}, "required": [], "additionalProperties": False},
        system_prompt="system prompt",
        user_prompt="user prompt",
        model="gpt-5-mini",
    )
    assert payload["model"] == "gpt-5-mini"
    assert payload["instructions"] == "system prompt"
    assert payload["input"] == "user prompt"
    assert payload["text"]["format"]["type"] == "json_schema"


def test_openai_wrapper_gracefully_handles_request_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        ai_judges,
        "get_settings",
        lambda: Settings(database_url="sqlite:///:memory:", openai_enabled=True, openai_api_key="test-key", openai_max_retries=1),
    )

    def boom(*args, **kwargs):
        raise requests.RequestException("network down")

    monkeypatch.setattr(ai_judges.requests, "post", boom)
    result = ai_judges.call_openai_json(
        "demo",
        {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"], "additionalProperties": False},
        "system",
        "user",
    )
    assert result is None


def test_openai_wrapper_parses_structured_json(monkeypatch) -> None:
    monkeypatch.setattr(
        ai_judges,
        "get_settings",
        lambda: Settings(database_url="sqlite:///:memory:", openai_enabled=True, openai_api_key="test-key", openai_max_retries=1),
    )

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"output_text": '{"classification":"strong_fit","reasons":["resume match"],"matched_profile_fields":["deployment"]}'}

    monkeypatch.setattr(ai_judges.requests, "post", lambda *args, **kwargs: FakeResponse())
    result = ai_judges.judge_fit_with_ai("deployment strategist", "Deployment Strategist", "Mercor", "Remote", "Deploy AI systems")
    assert result is not None
    assert result["classification"] == "strong_fit"


def test_openai_wrapper_logs_error_summary_once_for_repeated_400s(monkeypatch) -> None:
    monkeypatch.setattr(
        ai_judges,
        "get_settings",
        lambda: Settings(database_url="sqlite:///:memory:", openai_enabled=True, openai_api_key="test-key", openai_max_retries=1),
    )
    ai_judges._OPENAI_WARNING_CACHE.clear()
    logged: list[str] = []

    class FakeResponse:
        status_code = 400
        text = '{"error":{"message":"Invalid schema","type":"invalid_request_error","param":"text.format","code":"invalid_json_schema"}}'

        def json(self):
            return {
                "error": {
                    "message": "Invalid schema",
                    "type": "invalid_request_error",
                    "param": "text.format",
                    "code": "invalid_json_schema",
                }
            }

        def raise_for_status(self):
            raise requests.HTTPError("400 Client Error", response=self)

    monkeypatch.setattr(ai_judges.requests, "post", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr(ai_judges.logger, "warning", lambda message: logged.append(message))

    schema = {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"], "additionalProperties": False}
    assert ai_judges.call_openai_json("critic_judgment", schema, "system", "user") is None
    assert ai_judges.call_openai_json("critic_judgment", schema, "system", "user") is None
    assert len(logged) == 1
    assert "Invalid schema" in logged[0]


def test_runtime_control_supports_play_pause_and_run_once() -> None:
    session = build_session()
    settings = Settings(database_url="sqlite:///:memory:", autonomy_enabled=True, demo_mode=True)
    control = get_runtime_control(session, settings)
    assert control.run_state == "paused"

    set_runtime_action(session, "play", settings)
    assert determine_cycle_mode(session, settings)[0] == "running"

    set_runtime_action(session, "pause", settings)
    assert determine_cycle_mode(session, settings)[0] == "paused"

    set_runtime_action(session, "run_once", settings)
    mode, control = determine_cycle_mode(session, settings)
    assert mode == "run_once"
    assert control.run_once_requested is True


def test_runtime_control_request_accepts_direct_run_state() -> None:
    payload = RuntimeControlRequest(run_state="running")
    assert payload.run_state == "running"
    assert payload.action is None


def test_runtime_control_payload_exposes_interval_and_next_cycle() -> None:
    session = build_session()
    settings = Settings(database_url="sqlite:///:memory:", autonomy_enabled=True, demo_mode=True, worker_interval_seconds=900, interactive_worker_interval_seconds=15)
    control = get_runtime_control(session, settings)
    set_runtime_action(session, "play", settings)
    control.sleep_until = datetime.utcnow()
    payload = runtime_control_payload(control, settings)
    assert payload.current_interval_seconds == 15
    assert payload.next_cycle_at is not None
    assert payload.last_control_action == "play"


def test_mark_worker_state_persists_actual_interval() -> None:
    session = build_session()
    settings = Settings(database_url="sqlite:///:memory:", autonomy_enabled=True, demo_mode=True)
    control = get_runtime_control(session, settings)
    from services.runtime_control import mark_worker_state

    mark_worker_state(control, "sleeping", "Sleeping", sleep_seconds=9)
    payload = runtime_control_payload(control, settings)
    assert payload.current_interval_seconds == 9


def test_effective_worker_interval_is_shorter_in_demo_mode() -> None:
    demo_settings = Settings(database_url="sqlite:///:memory:", demo_mode=True, worker_interval_seconds=900, interactive_worker_interval_seconds=12)
    live_settings = Settings(database_url="sqlite:///:memory:", demo_mode=False, worker_interval_seconds=900, interactive_worker_interval_seconds=12)
    assert effective_worker_interval_seconds(demo_settings) == 12
    assert effective_worker_interval_seconds(live_settings) == 900


def test_paused_worker_cycle_does_not_process_pipeline() -> None:
    session = build_session()
    settings = Settings(database_url="sqlite:///:memory:", autonomy_enabled=True, greenhouse_enabled=False, demo_mode=True)
    set_runtime_action(session, "pause", settings)
    outcome = run_worker_cycle(session, settings)
    assert outcome["state"] == "paused"
    assert outcome["ran"] is False
    control = get_runtime_control(session, settings)
    assert control.worker_state == "paused"
    assert "paused" in (control.status_message or "").lower()


def test_running_worker_cycle_updates_state_immediately(monkeypatch) -> None:
    session = build_session()
    seed_profile(session)
    settings = Settings(database_url="sqlite:///:memory:", autonomy_enabled=True, greenhouse_enabled=True, demo_mode=False)
    set_runtime_action(session, "play", settings)

    monkeypatch.setattr("services.worker_runtime.get_runtime_connector_set", lambda settings: ("live", {"greenhouse"}, {"greenhouse"}))
    monkeypatch.setattr(
        "services.worker_runtime.run_full_pipeline",
        lambda session, source_mode, enabled_connectors, strict_live_connectors: AgentRunResponse(agent="full_pipeline", summary="Completed one cycle."),
    )

    outcome = run_worker_cycle(session, settings)
    control = get_runtime_control(session, settings)

    assert outcome["ran"] is True
    assert control.last_cycle_started_at is not None
    assert control.last_successful_cycle_at is not None
    assert control.worker_state == "idle"
    assert "finished" in (control.status_message or "").lower()


def test_worker_signal_marks_stopping_state(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    local_session = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(worker_script, "SessionLocal", local_session)
    worker_script.STOP_REQUESTED = False

    worker_script._handle_signal(2, None)

    with local_session() as session:
        control = get_runtime_control(session)
        assert worker_script.STOP_REQUESTED is True
        assert control.worker_state == "stopping"
        assert "shutdown signal" in (control.status_message or "").lower()


def test_ai_judgment_cannot_override_deterministic_critic_suppression(monkeypatch) -> None:
    session = build_session()
    seed_profile(session)
    listing = Listing(
        company_name="ArchiveCo",
        title="Chief of Staff",
        location="Remote",
        url="https://jobs.example.com/archive",
        source_type="greenhouse",
        posted_at=datetime.utcnow(),
        description_text="This position has been filled.",
        listing_status="expired",
        freshness_days=45,
        metadata_json={"page_text": "job no longer available"},
    )
    session.add(listing)
    session.flush()
    lead = Lead(
        lead_type="listing",
        company_name="ArchiveCo",
        primary_title="Chief of Staff",
        listing_id=listing.id,
        surfaced_at=datetime.utcnow(),
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        explanation="Should be suppressed",
        score_breakdown_json={"composite": 7.0},
        evidence_json={"url": listing.url, "source_type": "greenhouse"},
        hidden=False,
    )
    session.add(lead)
    session.commit()

    monkeypatch.setattr(
        "services.sync.judge_critic_with_ai",
        lambda **kwargs: {"quality_assessment": "live", "reasons": ["looks live"], "flags": {"stale_like": False, "broken_like": False, "duplicate_like": False, "low_info": False}},
    )

    decision = evaluate_critic_decision(session, lead, session.query(CandidateProfile).first())
    assert decision["status"] == "suppressed"
    assert decision["visible"] is False
