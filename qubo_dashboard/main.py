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

app = FastAPI(title="Qubo Support Executive Board")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/dashboard")
def dashboard(
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    categories: list[str] = Query(default=[]),
    products: list[str] = Query(default=[]),
    departments: list[str] = Query(default=[]),
    channels: list[str] = Query(default=[]),
    efcs: list[str] = Query(default=[]),
    issue_details: list[str] = Query(default=[]),
    statuses: list[str] = Query(default=[]),
    bot_actions: list[str] = Query(default=[]),
    include_fc1: list[str] = Query(default=[]),
    exclude_fc1: list[str] = Query(default=[]),
    include_fc2: list[str] = Query(default=[]),
    exclude_fc2: list[str] = Query(default=[]),
    include_bot_action: list[str] = Query(default=[]),
    exclude_bot_action: list[str] = Query(default=[]),
) -> dict:
    filters = DashboardFilters(
        date_start=date_start,
        date_end=date_end,
        categories=categories,
        products=products,
        departments=departments,
        channels=channels,
        efcs=efcs,
        issue_details=issue_details,
        statuses=statuses,
        bot_actions=bot_actions,
        include_fc1=include_fc1,
        exclude_fc1=exclude_fc1,
        include_fc2=include_fc2,
        exclude_fc2=exclude_fc2,
        include_bot_action=include_bot_action,
        exclude_bot_action=exclude_bot_action,
    )
    return service.build_dashboard(filters)


@app.get("/api/issues/{issue_id}")
def issue_details(
    issue_id: str,
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    categories: list[str] = Query(default=[]),
    products: list[str] = Query(default=[]),
    departments: list[str] = Query(default=[]),
    channels: list[str] = Query(default=[]),
    efcs: list[str] = Query(default=[]),
    issue_details: list[str] = Query(default=[]),
    statuses: list[str] = Query(default=[]),
    bot_actions: list[str] = Query(default=[]),
    include_fc1: list[str] = Query(default=[]),
    exclude_fc1: list[str] = Query(default=[]),
    include_fc2: list[str] = Query(default=[]),
    exclude_fc2: list[str] = Query(default=[]),
    include_bot_action: list[str] = Query(default=[]),
    exclude_bot_action: list[str] = Query(default=[]),
) -> dict:
    filters = DashboardFilters(
        date_start=date_start,
        date_end=date_end,
        categories=categories,
        products=products,
        departments=departments,
        channels=channels,
        efcs=efcs,
        issue_details=issue_details,
        statuses=statuses,
        bot_actions=bot_actions,
        include_fc1=include_fc1,
        exclude_fc1=exclude_fc1,
        include_fc2=include_fc2,
        exclude_fc2=exclude_fc2,
        include_bot_action=include_bot_action,
        exclude_bot_action=exclude_bot_action,
    )
    return service.get_issue_tickets(filters, issue_id)


@app.get("/api/drilldown/product")
def product_drilldown(
    category: str = Query(...),
    product_name: str = Query(...),
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    categories: list[str] = Query(default=[]),
    products: list[str] = Query(default=[]),
    departments: list[str] = Query(default=[]),
    channels: list[str] = Query(default=[]),
    efcs: list[str] = Query(default=[]),
    issue_details: list[str] = Query(default=[]),
    statuses: list[str] = Query(default=[]),
    bot_actions: list[str] = Query(default=[]),
    include_fc1: list[str] = Query(default=[]),
    exclude_fc1: list[str] = Query(default=[]),
    include_fc2: list[str] = Query(default=[]),
    exclude_fc2: list[str] = Query(default=[]),
    include_bot_action: list[str] = Query(default=[]),
    exclude_bot_action: list[str] = Query(default=[]),
) -> dict:
    filters = DashboardFilters(
        date_start=date_start,
        date_end=date_end,
        categories=categories,
        products=products,
        departments=departments,
        channels=channels,
        efcs=efcs,
        issue_details=issue_details,
        statuses=statuses,
        bot_actions=bot_actions,
        include_fc1=include_fc1,
        exclude_fc1=exclude_fc1,
        include_fc2=include_fc2,
        exclude_fc2=exclude_fc2,
        include_bot_action=include_bot_action,
        exclude_bot_action=exclude_bot_action,
    )
    return service.get_product_drilldown(filters, category, product_name)


@app.get("/api/drilldown/issue/{issue_id}")
def issue_drilldown(
    issue_id: str,
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    categories: list[str] = Query(default=[]),
    products: list[str] = Query(default=[]),
    departments: list[str] = Query(default=[]),
    channels: list[str] = Query(default=[]),
    efcs: list[str] = Query(default=[]),
    issue_details: list[str] = Query(default=[]),
    statuses: list[str] = Query(default=[]),
    bot_actions: list[str] = Query(default=[]),
    include_fc1: list[str] = Query(default=[]),
    exclude_fc1: list[str] = Query(default=[]),
    include_fc2: list[str] = Query(default=[]),
    exclude_fc2: list[str] = Query(default=[]),
    include_bot_action: list[str] = Query(default=[]),
    exclude_bot_action: list[str] = Query(default=[]),
) -> dict:
    filters = DashboardFilters(
        date_start=date_start,
        date_end=date_end,
        categories=categories,
        products=products,
        departments=departments,
        channels=channels,
        efcs=efcs,
        issue_details=issue_details,
        statuses=statuses,
        bot_actions=bot_actions,
        include_fc1=include_fc1,
        exclude_fc1=exclude_fc1,
        include_fc2=include_fc2,
        exclude_fc2=exclude_fc2,
        include_bot_action=include_bot_action,
        exclude_bot_action=exclude_bot_action,
    )
    return service.get_issue_drilldown(filters, issue_id)


@app.get("/api/tickets")
def tickets(
    query: str = Query(default=""),
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    categories: list[str] = Query(default=[]),
    products: list[str] = Query(default=[]),
    departments: list[str] = Query(default=[]),
    channels: list[str] = Query(default=[]),
    efcs: list[str] = Query(default=[]),
    issue_details: list[str] = Query(default=[]),
    statuses: list[str] = Query(default=[]),
    bot_actions: list[str] = Query(default=[]),
    include_fc1: list[str] = Query(default=[]),
    exclude_fc1: list[str] = Query(default=[]),
    include_fc2: list[str] = Query(default=[]),
    exclude_fc2: list[str] = Query(default=[]),
    include_bot_action: list[str] = Query(default=[]),
    exclude_bot_action: list[str] = Query(default=[]),
) -> dict:
    filters = DashboardFilters(
        date_start=date_start,
        date_end=date_end,
        categories=categories,
        products=products,
        departments=departments,
        channels=channels,
        efcs=efcs,
        issue_details=issue_details,
        statuses=statuses,
        bot_actions=bot_actions,
        include_fc1=include_fc1,
        exclude_fc1=exclude_fc1,
        include_fc2=include_fc2,
        exclude_fc2=exclude_fc2,
        include_bot_action=include_bot_action,
        exclude_bot_action=exclude_bot_action,
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
        "data_source": "sample" if settings.use_sample_data else settings.analytics_backend,
        "analytics_backend": settings.analytics_backend,
        "zoho_db_configured": repo_status["zoho_configured"],
        "clickhouse_configured": settings.has_clickhouse,
        "clickhouse_database": settings.clickhouse.database,
        "agg_db_configured": repo_status["agg_configured"],
        "serve_frontend": settings.serve_frontend,
        "cors_allowed_origins": settings.cors_allowed_origins,
        "zoho_ticket_table": repo_status["zoho_ticket_table"],
        "agg_tables": repo_status["agg_tables"],
        "pipeline_status": pipeline_manager.status(),
    }
