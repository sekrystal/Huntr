from __future__ import annotations

from core.config import APP_ROOT, Settings


def test_relative_sqlite_url_resolves_to_project_root() -> None:
    settings = Settings(database_url="sqlite:///./opportunity_scout.db")
    assert settings.database_url == f"sqlite:///{(APP_ROOT / 'opportunity_scout.db').as_posix()}"
