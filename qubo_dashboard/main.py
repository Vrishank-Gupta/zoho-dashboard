from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response

from .analytics import AnalyticsService
from .models import DashboardFilters
from .pipeline.service import PipelineManager
from .repository import TicketRepository
from .config import settings
from .mapping import (
    clear_mapping_cache,
    export_efc_mapping_csv,
    export_product_mapping_csv,
    parse_efc_mapping_csv,
    parse_product_mapping_csv,
    replace_efc_mapping,
    replace_product_mapping,
)


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

repository = TicketRepository()
service = AnalyticsService(repository)
pipeline_manager = PipelineManager()


def build_filters(
    date_start: str | None = None,
    date_end: str | None = None,
    exclude_installation: bool = False,
    exclude_blank_chat: bool = False,
    exclude_unclassified_blank: bool = False,
    categories: list[str] | None = None,
    products: list[str] | None = None,
    departments: list[str] | None = None,
    channels: list[str] | None = None,
    efcs: list[str] | None = None,
    issue_details: list[str] | None = None,
    statuses: list[str] | None = None,
    bot_actions: list[str] | None = None,
    include_fc1: list[str] | None = None,
    exclude_fc1: list[str] | None = None,
    include_fc2: list[str] | None = None,
    exclude_fc2: list[str] | None = None,
    include_bot_action: list[str] | None = None,
    exclude_bot_action: list[str] | None = None,
    mapping_overrides: str | None = None,
) -> DashboardFilters:
    parsed_overrides = _parse_mapping_overrides(mapping_overrides)
    return DashboardFilters(
        date_start=date_start,
        date_end=date_end,
        exclude_installation=exclude_installation,
        exclude_blank_chat=exclude_blank_chat,
        exclude_unclassified_blank=exclude_unclassified_blank,
        categories=categories or [],
        products=products or [],
        departments=departments or [],
        channels=channels or [],
        efcs=efcs or [],
        issue_details=issue_details or [],
        statuses=statuses or [],
        bot_actions=bot_actions or [],
        include_fc1=include_fc1 or [],
        exclude_fc1=exclude_fc1 or [],
        include_fc2=include_fc2 or [],
        exclude_fc2=exclude_fc2 or [],
        include_bot_action=include_bot_action or [],
        exclude_bot_action=exclude_bot_action or [],
        product_category_overrides=parsed_overrides.get("product_category_overrides", {}),
        efc_overrides=parsed_overrides.get("efc_overrides", {}),
    )


def _parse_mapping_overrides(raw: str | None) -> dict[str, dict[str, str]]:
    if not raw:
        return {"product_category_overrides": {}, "efc_overrides": {}}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"product_category_overrides": {}, "efc_overrides": {}}
    product_overrides = {
        str(key).strip().lower(): str(value).strip()
        for key, value in (payload.get("product_category_overrides") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    efc_overrides = {
        str(key).strip().lower(): str(value).strip()
        for key, value in (payload.get("efc_overrides") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    return {"product_category_overrides": product_overrides, "efc_overrides": efc_overrides}

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
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
    exclude_unclassified_blank: bool = Query(default=False),
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
    mapping_overrides: str | None = Query(default=None),
) -> dict:
    filters = build_filters(
        date_start=date_start,
        date_end=date_end,
        exclude_installation=exclude_installation,
        exclude_blank_chat=exclude_blank_chat,
        exclude_unclassified_blank=exclude_unclassified_blank,
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
        mapping_overrides=mapping_overrides,
    )
    return service.build_dashboard(filters)


@app.get("/api/mapping-studio")
def mapping_studio(
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
    exclude_unclassified_blank: bool = Query(default=False),
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
    mapping_overrides: str | None = Query(default=None),
) -> dict:
    filters = build_filters(
        date_start=date_start,
        date_end=date_end,
        exclude_installation=exclude_installation,
        exclude_blank_chat=exclude_blank_chat,
        exclude_unclassified_blank=exclude_unclassified_blank,
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
        mapping_overrides=mapping_overrides,
    )
    return service.get_mapping_studio(filters)


@app.post("/api/admin/mapping/save")
async def save_mapping_overrides(request: Request) -> dict:
    payload = await request.json()
    product_rows = payload.get("product_rows") or []
    fc2_rows = payload.get("fc2_rows") or []
    replace_product_mapping(
        [
            {
                "product_name": str(row.get("product_name") or "").strip(),
                "category": str(row.get("effective_category") or row.get("category") or "").strip(),
            }
            for row in product_rows
            if str(row.get("product_name") or "").strip() and str(row.get("effective_category") or row.get("category") or "").strip()
        ]
    )
    replace_efc_mapping(
        [
            {
                "fault_code_level_2": str(row.get("fault_code_level_2") or "").strip(),
                "executive_fault_code": str(row.get("effective_efc") or row.get("executive_fault_code") or "").strip(),
            }
            for row in fc2_rows
            if str(row.get("fault_code_level_2") or "").strip() and str(row.get("effective_efc") or row.get("executive_fault_code") or "").strip()
        ]
    )
    clear_mapping_cache()
    service.invalidate_cache()
    return {"status": "ok"}


@app.get("/api/admin/mapping/product.csv")
def download_product_mapping_csv() -> Response:
    return Response(
        content=export_product_mapping_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="product-category-mapping.csv"'},
    )


@app.get("/api/admin/mapping/efc.csv")
def download_efc_mapping_csv() -> Response:
    return Response(
        content=export_efc_mapping_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="fc2-efc-mapping.csv"'},
    )


@app.post("/api/admin/mapping/product.csv")
async def upload_product_mapping_csv(request: Request) -> dict:
    content = (await request.body()).decode("utf-8-sig")
    rows = parse_product_mapping_csv(content)
    replace_product_mapping(rows)
    clear_mapping_cache()
    service.invalidate_cache()
    return {"status": "ok", "rows": len(rows)}


@app.post("/api/admin/mapping/efc.csv")
async def upload_efc_mapping_csv(request: Request) -> dict:
    content = (await request.body()).decode("utf-8-sig")
    rows = parse_efc_mapping_csv(content)
    replace_efc_mapping(rows)
    clear_mapping_cache()
    service.invalidate_cache()
    return {"status": "ok", "rows": len(rows)}


@app.get("/api/issues/{issue_id}")
def issue_details(
    issue_id: str,
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
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
    mapping_overrides: str | None = Query(default=None),
) -> dict:
    filters = build_filters(
        date_start=date_start,
        date_end=date_end,
        exclude_installation=exclude_installation,
        exclude_blank_chat=exclude_blank_chat,
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
        mapping_overrides=mapping_overrides,
    )
    return service.get_issue_tickets(filters, issue_id)


@app.get("/api/drilldown/product")
def product_drilldown(
    category: str = Query(...),
    product_name: str = Query(...),
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
    exclude_unclassified_blank: bool = Query(default=False),
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
    mapping_overrides: str | None = Query(default=None),
) -> dict:
    filters = build_filters(
        date_start=date_start,
        date_end=date_end,
        exclude_installation=exclude_installation,
        exclude_blank_chat=exclude_blank_chat,
        exclude_unclassified_blank=exclude_unclassified_blank,
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
        mapping_overrides=mapping_overrides,
    )
    return service.get_product_drilldown(filters, category, product_name)


@app.get("/api/drilldown/category")
def category_drilldown(
    category: str = Query(...),
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
    exclude_unclassified_blank: bool = Query(default=False),
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
    mapping_overrides: str | None = Query(default=None),
) -> dict:
    filters = build_filters(
        date_start=date_start,
        date_end=date_end,
        exclude_installation=exclude_installation,
        exclude_blank_chat=exclude_blank_chat,
        exclude_unclassified_blank=exclude_unclassified_blank,
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
        mapping_overrides=mapping_overrides,
    )
    return service.get_category_drilldown(filters, category)


@app.get("/api/drilldown/issue/{issue_id}")
def issue_drilldown(
    issue_id: str,
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
    exclude_unclassified_blank: bool = Query(default=False),
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
    mapping_overrides: str | None = Query(default=None),
) -> dict:
    filters = build_filters(
        date_start=date_start,
        date_end=date_end,
        exclude_installation=exclude_installation,
        exclude_blank_chat=exclude_blank_chat,
        exclude_unclassified_blank=exclude_unclassified_blank,
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
        mapping_overrides=mapping_overrides,
    )
    return service.get_issue_drilldown(filters, issue_id)


@app.get("/api/tickets")
def tickets(
    query: str = Query(default=""),
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
    exclude_unclassified_blank: bool = Query(default=False),
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
    mapping_overrides: str | None = Query(default=None),
) -> dict:
    filters = build_filters(
        date_start=date_start,
        date_end=date_end,
        exclude_installation=exclude_installation,
        exclude_blank_chat=exclude_blank_chat,
        exclude_unclassified_blank=exclude_unclassified_blank,
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
        mapping_overrides=mapping_overrides,
    )
    return {"tickets": service.search_tickets(filters, query)}


@app.api_route("/", methods=["GET", "HEAD"])
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


@app.api_route("/admin", methods=["GET", "HEAD"])
def admin_index():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return FileResponse(FRONTEND_DIR / "index.html")


@app.api_route("/styles.css", methods=["GET", "HEAD"])
def frontend_styles():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return FileResponse(FRONTEND_DIR / "styles.css")


@app.api_route("/app.js", methods=["GET", "HEAD"])
def frontend_app():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return FileResponse(FRONTEND_DIR / "app.js")


@app.api_route("/config.js", methods=["GET", "HEAD"])
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


@app.api_route("/api/health", methods=["GET", "HEAD"])
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
