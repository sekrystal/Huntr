from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Response
from sqlalchemy.orm import Session

from api.routes.applications import router as applications_router
from api.routes.agents import router as agents_router
from api.routes.feedback import router as feedback_router
from api.routes.health import router as health_router
from api.routes.opportunities import router as opportunities_router
from api.routes.profile import router as profile_router
from api.routes.search_runs import router as search_runs_router
from core.db import get_db, init_db
from core.logging import configure_logging
from core.schemas import StatsResponse, SyncResult
from services.scheduler import LocalScheduler
from services.sync import get_stats, sync_all


scheduler = LocalScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    init_db()
    scheduler.start()
    yield
    scheduler.stop()


app = FastAPI(title="Opportunity Scout", lifespan=lifespan)
app.include_router(health_router)
app.include_router(opportunities_router)
app.include_router(feedback_router)
app.include_router(applications_router)
app.include_router(profile_router)
app.include_router(agents_router)
app.include_router(search_runs_router)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "name": "Opportunity Scout API",
        "status": "ok",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.post("/sync", response_model=SyncResult)
def run_sync(db: Session = Depends(get_db)) -> SyncResult:
    result = sync_all(db, include_rechecks=True)
    db.commit()
    return result


@app.get("/stats", response_model=StatsResponse)
def stats(db: Session = Depends(get_db)) -> StatsResponse:
    return get_stats(db)
