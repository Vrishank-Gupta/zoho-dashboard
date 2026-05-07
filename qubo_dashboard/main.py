from __future__ import annotations

import json
from pathlib import Path
from time import perf_counter

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from .analytics import AnalyticsService
from .auth import current_user, logout, request_otp, verify_otp
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
from .logging_utils import log_access, log_audit, make_request_id, runtime_logger, setup_logging


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

setup_logging()
LOGGER = runtime_logger()
repository = TicketRepository()
service = AnalyticsService(repository)
pipeline_manager = PipelineManager(after_success=service.precompute_standard_dashboard_cache)


def build_filters(
    date_start: str | None = None,
    date_end: str | None = None,
    exclude_installation: bool = False,
    exclude_blank_chat: bool = False,
    exclude_unclassified_blank: bool = False,
    categories: list[str] | None = None,
    products: list[str] | None = None,
    device_models: list[str] | None = None,
    software_versions: list[str] | None = None,
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
        device_models=device_models or [],
        software_versions=software_versions or [],
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
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class OtpRequest(BaseModel):
    email: str


class OtpVerifyRequest(BaseModel):
    email: str
    otp: str


AUTH_EXEMPT_API_PATHS = {
    "/api/auth/request-otp",
    "/api/auth/verify",
    "/api/auth/session",
    "/api/auth/logout",
    "/api/health",
}


@app.middleware("http")
async def enforce_dashboard_auth(request: Request, call_next):
    if settings.auth_enabled and request.url.path.startswith("/api/") and request.url.path not in AUTH_EXEMPT_API_PATHS:
        if not current_user(request):
            return JSONResponse({"detail": "Login required."}, status_code=401)
    return await call_next(request)


@app.middleware("http")
async def add_no_store_for_api(request: Request, call_next):
    request_id = make_request_id()
    request.state.request_id = request_id
    started = perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        log_access(
            request,
            status_code=500,
            duration_ms=(perf_counter() - started) * 1000,
            request_id=request_id,
        )
        LOGGER.exception(
            "request_failed",
            extra={"extra_fields": {"event": "request_failed", "request_id": request_id, "path": request.url.path}},
        )
        raise
    response.headers["X-Request-ID"] = request_id
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    log_access(
        request,
        status_code=response.status_code,
        duration_ms=(perf_counter() - started) * 1000,
        request_id=request_id,
    )
    return response


@app.post("/api/auth/request-otp")
def auth_request_otp(payload: OtpRequest) -> dict:
    return request_otp(payload.email)


@app.post("/api/auth/verify")
def auth_verify_otp(payload: OtpVerifyRequest, request: Request, response: Response) -> dict:
    return verify_otp(payload.email, payload.otp, request, response)


@app.get("/api/auth/session")
def auth_session(request: Request) -> dict:
    user = current_user(request)
    return {"authenticated": bool(user), "user": user}


@app.post("/api/auth/logout")
def auth_logout(response: Response) -> dict:
    return logout(response)


@app.get("/api/dashboard")
def dashboard(
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
    exclude_unclassified_blank: bool = Query(default=False),
    categories: list[str] = Query(default=[]),
    products: list[str] = Query(default=[]),
    device_models: list[str] = Query(default=[]),
    software_versions: list[str] = Query(default=[]),
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
        device_models=device_models,
        software_versions=software_versions,
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
    device_models: list[str] = Query(default=[]),
    software_versions: list[str] = Query(default=[]),
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
        device_models=device_models,
        software_versions=software_versions,
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
    log_audit(
        request,
        action="mapping_save",
        details={"product_rows": len(product_rows), "fc2_rows": len(fc2_rows)},
    )
    return {"status": "ok"}


@app.get("/api/admin/mapping/product.csv")
def download_product_mapping_csv(request: Request) -> Response:
    log_audit(request, action="mapping_product_csv_download")
    return Response(
        content=export_product_mapping_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="product-category-mapping.csv"'},
    )


@app.get("/api/admin/mapping/efc.csv")
def download_efc_mapping_csv(request: Request) -> Response:
    log_audit(request, action="mapping_efc_csv_download")
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
    log_audit(request, action="mapping_product_csv_upload", details={"rows": len(rows), "bytes": len(content.encode("utf-8"))})
    return {"status": "ok", "rows": len(rows)}


@app.post("/api/admin/mapping/efc.csv")
async def upload_efc_mapping_csv(request: Request) -> dict:
    content = (await request.body()).decode("utf-8-sig")
    rows = parse_efc_mapping_csv(content)
    replace_efc_mapping(rows)
    clear_mapping_cache()
    service.invalidate_cache()
    log_audit(request, action="mapping_efc_csv_upload", details={"rows": len(rows), "bytes": len(content.encode("utf-8"))})
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
    device_models: list[str] = Query(default=[]),
    software_versions: list[str] = Query(default=[]),
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
        device_models=device_models,
        software_versions=software_versions,
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
    device_models: list[str] = Query(default=[]),
    software_versions: list[str] = Query(default=[]),
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
        device_models=device_models,
        software_versions=software_versions,
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
    device_models: list[str] = Query(default=[]),
    software_versions: list[str] = Query(default=[]),
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
        device_models=device_models,
        software_versions=software_versions,
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
    device_models: list[str] = Query(default=[]),
    software_versions: list[str] = Query(default=[]),
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
        device_models=device_models,
        software_versions=software_versions,
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


@app.get("/api/drilldown/repeat")
def repeat_drilldown(
    kind: str = Query(...),
    label: str = Query(...),
    secondary: str | None = Query(default=None),
    date_start: str | None = Query(default=None),
    date_end: str | None = Query(default=None),
    exclude_installation: bool = Query(default=False),
    exclude_blank_chat: bool = Query(default=False),
    exclude_unclassified_blank: bool = Query(default=False),
    categories: list[str] = Query(default=[]),
    products: list[str] = Query(default=[]),
    device_models: list[str] = Query(default=[]),
    software_versions: list[str] = Query(default=[]),
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
        device_models=device_models,
        software_versions=software_versions,
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
    return service.get_repeat_drilldown(filters, kind, label, secondary)


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
    device_models: list[str] = Query(default=[]),
    software_versions: list[str] = Query(default=[]),
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
        device_models=device_models,
        software_versions=software_versions,
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
        return _frontend_file("index.html")
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
    return _frontend_file("index.html")


@app.api_route("/styles.css", methods=["GET", "HEAD"])
def frontend_styles():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return _frontend_file("styles.css")


@app.api_route("/app.js", methods=["GET", "HEAD"])
def frontend_app():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return _frontend_file("app.js")


@app.api_route("/config.js", methods=["GET", "HEAD"])
def frontend_config():
    if not settings.serve_frontend:
        raise HTTPException(status_code=404, detail="Not Found")
    return _frontend_file("config.js")


def _frontend_file(filename: str) -> FileResponse:
    return FileResponse(
        FRONTEND_DIR / filename,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/pipeline/status")
def pipeline_status(request: Request) -> dict:
    log_audit(request, action="pipeline_status_view")
    return pipeline_manager.status()


@app.post("/api/pipeline/run")
def pipeline_run(request: Request) -> dict:
    result = pipeline_manager.start(requested_by="dashboard")
    log_audit(
        request,
        action="pipeline_run",
        outcome="accepted" if result.get("accepted") else "rejected",
        details={"reason": result.get("reason", "")},
    )
    return result


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
