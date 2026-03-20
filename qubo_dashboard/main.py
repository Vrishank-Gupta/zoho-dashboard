from __future__ import annotations

from datetime import date
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

def _parse_multi(s: str) -> list[str]:
    return [v.strip() for v in s.split(",") if v.strip()] if s else []


@app.get("/api/dashboard")
def dashboard(
    date_preset: str = Query(default="60d"),
    products: str = Query(default=""),
    models: str = Query(default=""),
    fault_codes: str = Query(default=""),
    channels: str = Query(default=""),
    bot_actions: str = Query(default=""),
    quick_exclusions: str = Query(default=""),
) -> dict:
    filters = DashboardFilters(
        date_preset=date_preset,
        products=_parse_multi(products),
        models=_parse_multi(models),
        fault_codes=_parse_multi(fault_codes),
        channels=_parse_multi(channels),
        bot_actions=_parse_multi(bot_actions),
        quick_exclusions=_parse_multi(quick_exclusions),
    )
    return service.build_dashboard(filters)


@app.get("/api/issues/{issue_id}")
def issue_details(
    issue_id: str,
    date_preset: str = Query(default="60d"),
    products: str = Query(default=""),
    models: str = Query(default=""),
    fault_codes: str = Query(default=""),
    channels: str = Query(default=""),
    bot_actions: str = Query(default=""),
    quick_exclusions: str = Query(default=""),
) -> dict:
    filters = DashboardFilters(
        date_preset=date_preset,
        products=_parse_multi(products),
        models=_parse_multi(models),
        fault_codes=_parse_multi(fault_codes),
        channels=_parse_multi(channels),
        bot_actions=_parse_multi(bot_actions),
        quick_exclusions=_parse_multi(quick_exclusions),
    )
    return service.get_issue_tickets(filters, issue_id)


@app.get("/api/period-breakdown")
def period_breakdown(
    start_date: date,
    end_date: date,
    date_preset: str = Query(default="60d"),
    products: str = Query(default=""),
    models: str = Query(default=""),
    fault_codes: str = Query(default=""),
    channels: str = Query(default=""),
    bot_actions: str = Query(default=""),
    quick_exclusions: str = Query(default=""),
) -> dict:
    filters = DashboardFilters(
        date_preset=date_preset,
        products=_parse_multi(products),
        models=_parse_multi(models),
        fault_codes=_parse_multi(fault_codes),
        channels=_parse_multi(channels),
        bot_actions=_parse_multi(bot_actions),
        quick_exclusions=_parse_multi(quick_exclusions),
    )
    return service.get_period_breakdown(filters, start_date, end_date)


@app.get("/api/tickets")
def tickets(
    query: str = Query(default=""),
    date_preset: str = Query(default="60d"),
    products: str = Query(default=""),
    models: str = Query(default=""),
    fault_codes: str = Query(default=""),
    channels: str = Query(default=""),
    quick_exclusions: str = Query(default=""),
) -> dict:
    filters = DashboardFilters(
        date_preset=date_preset,
        products=_parse_multi(products),
        models=_parse_multi(models),
        fault_codes=_parse_multi(fault_codes),
        channels=_parse_multi(channels),
        quick_exclusions=_parse_multi(quick_exclusions),
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
        "data_source": "mysql" if settings.has_zoho_database else "unconfigured",
        "zoho_db_configured": repo_status["zoho_configured"],
        "agg_db_configured": repo_status["agg_configured"],
        "serve_frontend": settings.serve_frontend,
        "cors_allowed_origins": settings.cors_allowed_origins,
        "zoho_ticket_table": repo_status["zoho_ticket_table"],
        "agg_tables": repo_status["agg_tables"],
        "pipeline_status": pipeline_manager.status(),
    }
