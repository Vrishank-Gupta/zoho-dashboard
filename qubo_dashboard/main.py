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
from .semantic_service import SemanticService


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

repository = TicketRepository()
service = AnalyticsService(repository)
semantic_service = SemanticService(repository)
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


@app.get("/api/v2/summary")
def v2_summary(
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
    return semantic_service.summary(filters)


@app.get("/api/v2/trend")
def v2_trend(
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
    return semantic_service.trend(filters)


@app.get("/api/v2/product-burden")
def v2_product_burden(
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
    return semantic_service.product_burden(filters)


@app.get("/api/v2/model-breakdown")
def v2_model_breakdown(
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
    return semantic_service.model_breakdown(filters)


@app.get("/api/v2/channel-mix")
def v2_channel_mix(
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
    return semantic_service.channel_mix(filters)


@app.get("/api/v2/drill/product/{product_family}")
def v2_product_drill(
    product_family: str,
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
    return semantic_service.product_drill(filters, product_family)


@app.get("/api/v2/period-breakdown")
def v2_period_breakdown(
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
    return semantic_service.period_breakdown(filters, start_date, end_date)


@app.get("/api/v2/issues")
def v2_issues(
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
    return semantic_service.issues(filters)


@app.get("/api/v2/validate")
def v2_validate(
    date_preset: str = Query(default="60d"),
    products: str = Query(default=""),
    fault_codes: str = Query(default=""),
    channels: str = Query(default=""),
) -> dict:
    """Compare v2 semantic layer numbers against the legacy aggregate path.
    Intentionally omits source-mode filters so the legacy path takes the fast aggregate route."""
    filters = DashboardFilters(
        date_preset=date_preset,
        products=_parse_multi(products),
        fault_codes=_parse_multi(fault_codes),
        channels=_parse_multi(channels),
    )
    v2_summary = semantic_service.summary(filters)
    v2_products = semantic_service.product_burden(filters)
    try:
        legacy = service.build_dashboard(filters)
    except Exception as exc:
        return {"error": str(exc), "v2": {"summary": v2_summary, "products": v2_products}}

    v2_kpis = v2_summary.get("kpis", {})
    leg_kpis = legacy.get("kpis", {})

    def compare(v2_val: float, leg_val: float) -> dict:
        diff = v2_val - leg_val
        pct = diff / leg_val if leg_val else None
        return {
            "v2": round(v2_val, 4),
            "legacy": round(leg_val, 4),
            "diff": round(diff, 4),
            "pct_diff": round(pct, 4) if pct is not None else None,
        }

    kpi_keys = ["total_tickets", "repeat_rate", "bot_deflection_rate", "bot_transfer_rate",
                "repair_field_visit_rate", "installation_field_visit_rate"]
    kpi_comparison = {
        key: compare(
            float(v2_kpis.get(key, {}).get("value", 0) or 0),
            float(leg_kpis.get(key, {}).get("value", 0) or 0),
        )
        for key in kpi_keys
    }

    v2_prod_map = {r["product_family"]: r for r in v2_products.get("rows", [])}
    leg_prod_map = {p["product_family"]: p for p in legacy.get("product_health", [])}
    product_comparison = [
        {
            "product": prod,
            "tickets": compare(float(v2_prod_map.get(prod, {}).get("ticket_volume", 0) or 0), float(leg_prod_map.get(prod, {}).get("ticket_volume", 0) or 0)),
            "repeat_rate": compare(float(v2_prod_map.get(prod, {}).get("repeat_rate", 0) or 0), float(leg_prod_map.get(prod, {}).get("repeat_rate", 0) or 0)),
            "repair_field_visit_rate": compare(float(v2_prod_map.get(prod, {}).get("repair_field_visit_rate", 0) or 0), float(leg_prod_map.get(prod, {}).get("repair_field_visit_rate", 0) or 0)),
            "bot_deflection_rate": compare(float(v2_prod_map.get(prod, {}).get("bot_deflection_rate", 0) or 0), float(leg_prod_map.get(prod, {}).get("bot_deflection_rate", 0) or 0)),
        }
        for prod in sorted(set(v2_prod_map) | set(leg_prod_map))
    ]

    return {
        "meta": {"date_preset": date_preset, "v2_latest_date": v2_summary.get("meta", {}).get("latest_date")},
        "kpis": kpi_comparison,
        "products": product_comparison,
    }
