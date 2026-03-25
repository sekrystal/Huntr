from __future__ import annotations

import importlib
from datetime import datetime

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from core.config import get_settings
from core.models import Application, Base, Lead
from core.schemas import ApplicationStatusUpdate
from services.applications import update_application_status
from services.learning import generate_follow_up_tasks


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def _lead() -> Lead:
    return Lead(
        lead_type="listing",
        company_name="DemoCo",
        primary_title="Chief of Staff",
        rank_label="strong",
        confidence_label="high",
        freshness_label="fresh",
        title_fit_label="core match",
        qualification_fit_label="strong fit",
        score_breakdown_json={"role_family": "operations"},
        evidence_json={"source_type": "greenhouse"},
    )


def test_application_tracker_supports_richer_lifecycle_states() -> None:
    session = _session()
    lead = _lead()
    session.add(lead)
    session.flush()

    applied_at = datetime(2026, 3, 20, 15, 0, 0)
    application = update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="applied",
            date_applied=applied_at,
            notes="Submitted through ATS",
        ),
    )
    assert application.current_status == "applied"
    assert application.date_applied == applied_at

    application = update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="recruiter screen",
            status_reason_code="awaiting_scheduler_confirmation",
            notes="Recruiter requested times for a first call",
        ),
    )
    assert application.current_status == "recruiter screen"
    assert application.status_reason_code == "awaiting_scheduler_confirmation"
    assert application.notes == "Recruiter requested times for a first call"
    assert application.outcome_code is None
    assert application.outcome_reason_code is None
    assert application.date_applied == applied_at

    application = update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="final round",
            status_reason_code="onsite_completed",
        ),
    )
    assert application.current_status == "final round"
    assert application.status_reason_code == "onsite_completed"


def test_application_tracker_persists_outcomes_and_reason_codes() -> None:
    session = _session()
    lead = _lead()
    session.add(lead)
    session.flush()

    update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="applied",
            date_applied=datetime(2026, 3, 21, 10, 30, 0),
        ),
    )

    application = update_application_status(
        session,
        ApplicationStatusUpdate(
            lead_id=lead.id,
            current_status="rejected",
            status_reason_code="panel_decline",
            outcome_reason_code="insufficient_b2b_saas_depth",
            notes="Strong operator profile, but the panel wanted deeper pricing experience.",
        ),
    )
    session.commit()

    stored = session.query(Application).filter(Application.lead_id == lead.id).one()
    assert stored.current_status == "rejected"
    assert stored.status_reason_code == "panel_decline"
    assert stored.outcome_code == "rejected"
    assert stored.outcome_reason_code == "insufficient_b2b_saas_depth"
    assert stored.notes == "Strong operator profile, but the panel wanted deeper pricing experience."


def test_init_db_upgrades_legacy_sqlite_applications_schema_without_rebuild(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "legacy.sqlite"
    legacy_engine = create_engine(f"sqlite:///{database_path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(legacy_engine)

    with sessionmaker(bind=legacy_engine, expire_on_commit=False)() as seed_session:
        lead = _lead()
        lead.company_name = "LegacyCo"
        lead.primary_title = "Operations Lead"
        seed_session.add(lead)
        seed_session.commit()

    with legacy_engine.begin() as connection:
        connection.execute(text("DROP TABLE applications"))
        connection.execute(
            text(
                """
                CREATE TABLE applications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL UNIQUE,
                    company_name VARCHAR(255) NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    date_saved DATETIME,
                    date_applied DATETIME,
                    current_status VARCHAR(50) NOT NULL,
                    notes TEXT,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    FOREIGN KEY(lead_id) REFERENCES leads (id)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO applications (
                    lead_id,
                    company_name,
                    title,
                    date_saved,
                    date_applied,
                    current_status,
                    notes,
                    created_at,
                    updated_at
                ) VALUES (
                    1,
                    'LegacyCo',
                    'Operations Lead',
                    '2026-03-10 12:00:00.000000',
                    '2026-03-10 12:00:00.000000',
                    'applied',
                    'Imported from legacy DB',
                    '2026-03-10 12:00:00.000000',
                    '2026-03-10 12:00:00.000000'
                )
                """
            )
        )

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database_path}")
    get_settings.cache_clear()
    import core.db as core_db

    core_db = importlib.reload(core_db)
    core_db.init_db()

    inspector = inspect(core_db.engine)
    application_columns = {column["name"] for column in inspector.get_columns("applications")}
    assert {"status_reason_code", "outcome_code", "outcome_reason_code"}.issubset(application_columns)

    with core_db.SessionLocal() as session:
        application = session.query(Application).filter(Application.lead_id == 1).one()
        assert application.company_name == "LegacyCo"
        assert application.status_reason_code is None
        assert application.outcome_code is None
        assert application.outcome_reason_code is None

        created = generate_follow_up_tasks(session, follow_up_days=7)
        assert created == 1

        update_application_status(
            session,
            ApplicationStatusUpdate(
                lead_id=1,
                current_status="rejected",
                status_reason_code="panel_decline",
                outcome_reason_code="insufficient_b2b_saas_depth",
            ),
        )
        session.commit()

        refreshed = session.query(Application).filter(Application.lead_id == 1).one()
        assert refreshed.current_status == "rejected"
        assert refreshed.status_reason_code == "panel_decline"
        assert refreshed.outcome_code == "rejected"
        assert refreshed.outcome_reason_code == "insufficient_b2b_saas_depth"

    monkeypatch.delenv("DATABASE_URL")
    get_settings.cache_clear()
    importlib.reload(core_db)
