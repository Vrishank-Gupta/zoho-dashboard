from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .analytics import AnalyticsService
from .models import DashboardFilters
from .pipeline.service import PipelineManager
from .repository import TicketRepository
from .config import settings


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

repository = TicketRepository()
service = AnalyticsService(repository)
pipeline_manager = PipelineManager()

app = FastAPI(title="Qubo Support Health Command Center")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/dashboard")
def dashboard(
    date_preset: str = Query(default="60d"),
    product: str = Query(default="All"),
    department: str = Query(default="All"),
    issue: str = Query(default="All"),
    version: str = Query(default="All"),
    include_hero: bool = Query(default=False),
    include_dirty: bool = Query(default=False),
    history_mode: bool = Query(default=False),
) -> dict:
    filters = DashboardFilters(
        date_preset=date_preset,
        product=product,
        department=department,
        issue=issue,
        version=version,
        include_hero=include_hero,
        include_dirty=include_dirty,
        history_mode=history_mode,
    )
    return service.build_dashboard(filters)


@app.get("/api/issues/{issue_id}")
def issue_details(
    issue_id: str,
    date_preset: str = Query(default="60d"),
    product: str = Query(default="All"),
    department: str = Query(default="All"),
    issue: str = Query(default="All"),
    version: str = Query(default="All"),
    include_hero: bool = Query(default=False),
    include_dirty: bool = Query(default=False),
    history_mode: bool = Query(default=False),
) -> dict:
    filters = DashboardFilters(
        date_preset=date_preset,
        product=product,
        department=department,
        issue=issue,
        version=version,
        include_hero=include_hero,
        include_dirty=include_dirty,
        history_mode=history_mode,
    )
    return service.get_issue_tickets(filters, issue_id)


@app.get("/api/tickets")
def tickets(
    query: str = Query(default=""),
    date_preset: str = Query(default="60d"),
    product: str = Query(default="All"),
    department: str = Query(default="All"),
    issue: str = Query(default="All"),
    version: str = Query(default="All"),
    include_hero: bool = Query(default=False),
    include_dirty: bool = Query(default=False),
    history_mode: bool = Query(default=False),
) -> dict:
    filters = DashboardFilters(
        date_preset=date_preset,
        product=product,
        department=department,
        issue=issue,
        version=version,
        include_hero=include_hero,
        include_dirty=include_dirty,
        history_mode=history_mode,
    )
    return {"tickets": service.search_tickets(filters, query)}


@app.get("/")
def index():
    if settings.serve_frontend:
        return FileResponse(FRONTEND_DIR / "index.html")
    return {
        "service": "Qubo Support Health API",
        "status": "ok",
        "frontend_mode": "external",
        "health_url": "/api/health",
        "dashboard_url": "/api/dashboard",
    }


@app.get("/styles.css")
def frontend_styles():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return FileResponse(FRONTEND_DIR / "styles.css")


@app.get("/app.js")
def frontend_app():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return FileResponse(FRONTEND_DIR / "app.js")


@app.get("/config.js")
def frontend_config():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return FileResponse(FRONTEND_DIR / "config.js")


@app.get("/api/pipeline/status")
def pipeline_status() -> dict:
    return pipeline_manager.status()


@app.post("/api/pipeline/run")
def pipeline_run() -> dict:
    return pipeline_manager.start(requested_by="dashboard")


@app.get("/api/health")
def health() -> dict:
    repo_status = repository.get_connection_status()
    return {
        "status": "ok",
        "data_source": "sample" if settings.use_sample_data or not settings.has_zoho_database else "mysql",
        "zoho_db_configured": repo_status["zoho_configured"],
        "agg_db_configured": repo_status["agg_configured"],
        "serve_frontend": settings.serve_frontend,
        "cors_allowed_origins": settings.cors_allowed_origins,
        "zoho_ticket_table": repo_status["zoho_ticket_table"],
        "agg_tables": repo_status["agg_tables"],
        "pipeline_status": pipeline_manager.status(),
    }
