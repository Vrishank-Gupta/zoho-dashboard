from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
from typing import Any

from .config import settings
from .cleaning import normalize_fault_code
from .dashboard_rules import get_rules_signature, load_installation_combos, load_sales_marketing_rules
from .models import DashboardFilters
from .pipeline.transforms import build_ticket_facts, build_aggregates
from .repository import TicketRepository


@dataclass(slots=True)
class AggregateIssue:
    product_family: str
    fault_code: str
    fault_code_level_2: str
    software_version: str
    volume: int
    previous_volume: int
    repair_field_visit_rate: float
    installation_field_visit_rate: float
    repeat_rate: float
    bot_deflection_rate: float
    bot_transfer_rate: float
    blank_chat_rate: float
    logistics_rate: float
    top_symptom: str
    top_defect: str
    top_repair: str

    @property
    def issue_id(self) -> str:
        return "|".join([self.product_family, self.fault_code, self.fault_code_level_2, self.software_version])

    @property
    def insight(self) -> str:
        labels: list[str] = []
        if self.previous_volume >= 10 and self.volume > self.previous_volume * 1.2:
            labels.append("rising")
        if self.repair_field_visit_rate >= 0.12:
            labels.append("repair-heavy")
        if self.repeat_rate >= 0.1:
            labels.append("repeat-heavy")
        if self.bot_deflection_rate >= 0.25:
            labels.append("bot-friendly")
        if self.bot_transfer_rate >= 0.25:
            labels.append("agent-leaking")
        if not labels:
            labels.append("material")
        return f"{self.fault_code_level_2} is {' and '.join(labels)}."

    @property
    def composite_risk(self) -> float:
        growth = ((self.volume - self.previous_volume) / self.previous_volume) if self.previous_volume else 0.0
        return self.volume * 0.05 + max(growth, 0.0) * 20 + self.repair_field_visit_rate * 160 + self.repeat_rate * 90 + self.logistics_rate * 70 + self.bot_transfer_rate * 40


class AnalyticsService:
    def __init__(self, repository: TicketRepository) -> None:
        self._repository = repository
        self._source_facts_cache: tuple[list[Any], date] | None = None
        self._response_cache: dict[tuple[str, str], dict[str, Any]] = {}

    def build_dashboard(self, filters: DashboardFilters) -> dict[str, Any]:
        cache_key = self._snapshot_cache_key("dashboard", filters)
        cached = self._load_snapshot("dashboard", cache_key)
        if cached is not None:
            return cached
        if self._needs_source_mode(filters):
            if not self._source_mode_available():
                raise RuntimeError("Source-backed filters require either local raw-ticket cache or source database access.")
            payload = self._build_from_source(filters)
        else:
            if not settings.has_agg_database:
                raise RuntimeError("Aggregate database is not configured. Run the pipeline first.")
            payload = self._build_from_agg(filters)
        self._store_snapshot("dashboard", cache_key, payload)
        return payload

    def get_issue_tickets(self, filters: DashboardFilters, issue_id: str) -> dict[str, Any]:
        dashboard = self.build_dashboard(filters)
        issue = None
        candidate_lists = [
            dashboard.get("top_concerns", []),
            dashboard.get("improving_signals", []),
            dashboard.get("action_queue", []),
            dashboard.get("bot_summary", {}).get("best_issues", []),
            dashboard.get("bot_summary", {}).get("leaky_issues", []),
        ]
        for items in dashboard.get("issue_views", {}).values():
            candidate_lists.append(items)
        for items in candidate_lists:
            issue = next((item for item in items if item.get("issue_id") == issue_id), None)
            if issue:
                break
        if not issue:
            return {"issue": None, "tickets": []}
        tickets = self._repository.fetch_issue_tickets(
            product_family=issue["product_family"],
            fault_code=issue["fault_code"],
            fault_code_level_2=issue["fault_code_level_2"],
            software_version=issue["software_version"],
            limit=24,
        )
        return {
            "issue": issue,
            "tickets": [
                {
                    "ticket_id": ticket.ticket_id,
                    "created_at": ticket.created_at.isoformat(),
                    "product_family": ticket.canonical_product,
                    "product": ticket.product or "Unknown",
                    "department": ticket.normalized_department,
                    "channel": ticket.normalized_channel,
                    "fault_code": ticket.normalized_fault_code,
                    "fault_code_level_2": ticket.normalized_fault_code_l2,
                    "resolution": ticket.normalized_resolution,
                    "bot_action": ticket.bot_action or "Unknown",
                    "software_version": ticket.normalized_version,
                    "symptom": ticket.symptom or "Unknown",
                    "defect": ticket.defect or "Unknown",
                    "repair": ticket.repair or "Unknown",
                }
                for ticket in tickets
            ],
        }

    def search_tickets(self, filters: DashboardFilters, query: str = "") -> list[dict[str, Any]]:
        return []

    def get_period_breakdown(self, filters: DashboardFilters, start_date: date, end_date: date) -> dict[str, Any]:
        cache_key = self._snapshot_cache_key("period_breakdown", filters, start_date=start_date, end_date=end_date)
        cached = self._load_snapshot("period_breakdown", cache_key)
        if cached is not None:
            return cached
        if self._needs_source_mode(filters):
            if not self._source_mode_available():
                raise RuntimeError("Source-backed filters require either local raw-ticket cache or source database access.")
            payload = self._build_period_from_source(filters, start_date, end_date)
            self._store_snapshot("period_breakdown", cache_key, payload)
            return payload
        if not settings.has_agg_database:
            raise RuntimeError("Aggregate database is not configured. Run the pipeline first.")
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        current_rows = self._query_dicts(
            cursor,
            f"SELECT * FROM {settings.agg_daily_tickets_table} WHERE metric_date BETWEEN %s AND %s",
            (start_date, end_date),
        )
        days = max((end_date - start_date).days + 1, 1)
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days - 1)
        previous_rows = self._query_dicts(
            cursor,
            f"SELECT * FROM {settings.agg_daily_tickets_table} WHERE metric_date BETWEEN %s AND %s",
            (prev_start, prev_end),
        )
        cursor.close()
        connection.close()

        current_rows = [row for row in current_rows if self._matches_filters(row, filters)]
        previous_rows = [row for row in previous_rows if self._matches_filters(row, filters)]

        payload = {
            "kpis": self._agg_kpis(current_rows, previous_rows),
            "timeline": self._agg_timeline(current_rows),
            "products": self._agg_products(current_rows),
            "categories": self._agg_period_categories(current_rows),
            "fc2_by_category": self._agg_period_fc2(current_rows),
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "days": days,
            },
        }
        self._store_snapshot("period_breakdown", cache_key, payload)
        return payload

    def warm_snapshot_cache(self) -> dict[str, int]:
        warmed_dashboards = 0
        warmed_periods = 0
        seen_filter_keys: set[str] = set()
        for preset in settings.snapshot_prewarm_presets:
            seed_filters = DashboardFilters(date_preset=preset)
            seed_payload = self.build_dashboard(seed_filters)
            filter_sets = [seed_filters]
            products = [product for product in (seed_payload.get("filter_options", {}).get("products") or []) if product]
            models_by_product = seed_payload.get("filter_options", {}).get("models_by_product") or {}
            quick_groups = ["installations", "blank_chat", "duplicate_tickets", "sales_marketing"]
            filter_sets.extend(DashboardFilters(date_preset=preset, products=[product]) for product in products)
            for product in products:
                for model in models_by_product.get(product, []) or []:
                    filter_sets.append(DashboardFilters(date_preset=preset, products=[product], models=[model]))
            filter_sets.extend(DashboardFilters(date_preset=preset, quick_exclusions=[group]) for group in quick_groups)
            for product in products:
                filter_sets.extend(
                    DashboardFilters(date_preset=preset, products=[product], quick_exclusions=[group])
                    for group in quick_groups
                )
            for candidate in filter_sets:
                filter_key = self._snapshot_cache_key("dashboard", candidate)
                if filter_key in seen_filter_keys:
                    continue
                seen_filter_keys.add(filter_key)
                payload = self.build_dashboard(candidate)
                warmed_dashboards += 1
                for period_start, period_end in self._timeline_period_ranges(payload.get("timeline", []), candidate.date_preset):
                    self.get_period_breakdown(candidate, period_start, period_end)
                    warmed_periods += 1
        return {"dashboard_snapshots": warmed_dashboards, "period_snapshots": warmed_periods}

    def _needs_source_mode(self, filters: DashboardFilters) -> bool:
        return bool(filters.bot_actions or filters.models or filters.quick_exclusions)

    def _source_mode_available(self) -> bool:
        return settings.has_agg_database or settings.has_zoho_database

    def _build_from_source(self, filters: DashboardFilters) -> dict[str, Any]:
        facts = self._get_source_facts()
        if not facts:
            return self._empty_dashboard(filters, "mysql")
        latest = max(item.ticket.created_at.date() for item in facts)
        start_date = self._window_start(latest, filters.date_preset)
        previous_start = start_date - (latest - start_date) - timedelta(days=1)

        current_facts = [item for item in facts if start_date <= item.ticket.created_at.date() <= latest]
        previous_facts = [item for item in facts if previous_start <= item.ticket.created_at.date() < start_date]
        raw_current_facts = current_facts
        current_facts = [item for item in current_facts if self._ticket_matches_filters(item.ticket, filters)]
        previous_facts = [item for item in previous_facts if self._ticket_matches_filters(item.ticket, filters)]
        issue_window_facts = [item for item in facts if previous_start <= item.ticket.created_at.date() <= latest and self._ticket_matches_filters(item.ticket, filters)]

        current_aggs = build_aggregates(current_facts)
        issue_aggs = build_aggregates(issue_window_facts)
        previous_aggs = build_aggregates(previous_facts)
        products = self._agg_products(current_aggs["agg_daily_tickets"])
        issues = self._agg_issues(issue_aggs["agg_fc_weekly"], start_date)
        model_breakdown = self._agg_model_breakdown(current_aggs["agg_model_breakdown"])
        filter_options = self._source_filter_options(raw_current_facts)
        pipeline_health = self._pipeline_health_from_agg_if_available()
        cleaning_summary = self._agg_data_quality_summary(current_facts)
        bot_summary = self._agg_bot_summary(current_aggs["agg_bot"], issues)

        return {
            "meta": {
                "source_mode": "mysql",
                "ticket_count": self._agg_kpis(current_aggs["agg_daily_tickets"], previous_aggs["agg_daily_tickets"])["total_tickets"]["value"],
                "channel_filter": ",".join(filters.channels),
                "data_confidence_note": "Dashboard is reading source tickets for bot-action/model filters.",
                "warehouse_mode": True,
                "dataset_scope": "Source-backed filtered view",
                "pipeline_status": {
                    "last_successful_run": pipeline_health["last_run_at"],
                    "duration_minutes": pipeline_health["duration_minutes"],
                    "status": pipeline_health["status"],
                },
            },
            "filters": asdict(filters),
            "kpis": self._agg_kpis(current_aggs["agg_daily_tickets"], previous_aggs["agg_daily_tickets"]),
            "executive_summary": self._executive_summary(issues, products),
            "top_concerns": [self._issue_payload(item) for item in issues[:8]],
            "improving_signals": [self._issue_payload(item) for item in sorted(issues, key=lambda item: (item.bot_deflection_rate, -(item.volume - item.previous_volume)), reverse=True)[:4]],
            "action_queue": self._action_queue(issues),
            "issue_views": self._agg_issue_views(issues),
            "timeline": self._agg_timeline(current_aggs["agg_daily_tickets"]),
            "product_health": products,
            "model_breakdown": model_breakdown,
            "version_risks": self._agg_versions(current_aggs["agg_sw_version"]),
            "service_ops": self._agg_service_ops(current_aggs["agg_daily_tickets"], current_aggs["agg_channel"], current_aggs["agg_resolution"]),
            "bot_summary": bot_summary,
            "cleaning_summary": cleaning_summary,
            "pipeline_health": pipeline_health,
            "filter_options": filter_options,
        }

    def _build_period_from_source(self, filters: DashboardFilters, start_date: date, end_date: date) -> dict[str, Any]:
        facts = self._get_source_facts()
        days = max((end_date - start_date).days + 1, 1)
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days - 1)
        current_facts = [item for item in facts if start_date <= item.ticket.created_at.date() <= end_date and self._ticket_matches_filters(item.ticket, filters)]
        previous_facts = [item for item in facts if prev_start <= item.ticket.created_at.date() <= prev_end and self._ticket_matches_filters(item.ticket, filters)]
        current_aggs = build_aggregates(current_facts)
        previous_aggs = build_aggregates(previous_facts)
        return {
            "kpis": self._agg_kpis(current_aggs["agg_daily_tickets"], previous_aggs["agg_daily_tickets"]),
            "timeline": self._agg_timeline(current_aggs["agg_daily_tickets"]),
            "products": self._agg_products(current_aggs["agg_daily_tickets"]),
            "categories": self._agg_period_categories(current_aggs["agg_daily_tickets"]),
            "fc2_by_category": self._agg_period_fc2(current_aggs["agg_daily_tickets"]),
            "period": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "days": days},
        }

    def _get_source_facts(self):
        today = date.today()
        if self._source_facts_cache and self._source_facts_cache[1] == today:
            return self._source_facts_cache[0]
        # Source-backed filters should read from the local raw-ticket cache first.
        # Falling back to Zoho here makes quick filters and drilldowns feel broken.
        try:
            tickets = self._repository.fetch_cached_tickets()
        except Exception:
            tickets = []
        if not tickets:
            tickets = self._repository.fetch_tickets()
        facts = build_ticket_facts(tickets)
        self._source_facts_cache = (facts, today)
        return facts

    def _load_snapshot(self, cache_type: str, cache_key: str) -> dict[str, Any] | None:
        cached = self._response_cache.get((cache_type, cache_key))
        if cached is not None:
            return cached
        if not settings.has_agg_database:
            return None
        try:
            payload = self._repository.fetch_snapshot_payload(cache_type, cache_key)
        except Exception:
            return None
        if payload is not None:
            self._response_cache[(cache_type, cache_key)] = payload
        return payload

    def _store_snapshot(self, cache_type: str, cache_key: str, payload: dict[str, Any]) -> None:
        self._response_cache[(cache_type, cache_key)] = payload
        if not settings.has_agg_database:
            return
        try:
            self._repository.upsert_snapshot_payload(
                cache_type,
                cache_key,
                payload,
                str(payload.get("meta", {}).get("source_mode") or "unknown"),
            )
        except Exception:
            return

    def _snapshot_cache_key(
        self,
        cache_type: str,
        filters: DashboardFilters,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "filters": self._normalized_filter_payload(filters),
            "rules_signature": get_rules_signature(),
        }
        if start_date:
            payload["start_date"] = start_date.isoformat()
        if end_date:
            payload["end_date"] = end_date.isoformat()
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha1(f"{cache_type}:{raw}".encode("utf-8")).hexdigest()
        return digest

    def _normalized_filter_payload(self, filters: DashboardFilters) -> dict[str, Any]:
        return {
            "date_preset": filters.date_preset,
            "products": sorted(set(filters.products)),
            "models": sorted(set(filters.models)),
            "fault_codes": sorted(set(filters.fault_codes)),
            "channels": sorted(set(filters.channels)),
            "bot_actions": sorted(set(filters.bot_actions)),
            "quick_exclusions": sorted(set(filters.quick_exclusions)),
        }

    def _timeline_period_ranges(self, timeline: list[dict[str, Any]], date_preset: str) -> list[tuple[date, date]]:
        parsed_dates = sorted(
            {
                datetime.strptime(str(item.get("date")), "%Y-%m-%d").date()
                for item in timeline
                if item.get("date")
            }
        )
        if not parsed_dates:
            return []
        if date_preset == "14d":
            return [(day, day) for day in parsed_dates]
        if date_preset == "history":
            grouped: dict[tuple[int, int], list[date]] = defaultdict(list)
            for day in parsed_dates:
                grouped[(day.year, day.month)].append(day)
            return [(min(days), max(days)) for _, days in sorted(grouped.items())]
        grouped_weeks: dict[date, list[date]] = defaultdict(list)
        for day in parsed_dates:
            week_start = day - timedelta(days=day.weekday())
            grouped_weeks[week_start].append(day)
        return [(week_start, max(days)) for week_start, days in sorted(grouped_weeks.items())]

    def _build_from_agg(self, filters: DashboardFilters) -> dict[str, Any]:
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        max_date = self._scalar(cursor, f"SELECT MAX(metric_date) FROM {settings.agg_daily_tickets_table}") or date.today()
        start_date = self._window_start(max_date, filters.date_preset)
        previous_start = start_date - (max_date - start_date) - timedelta(days=1)

        daily_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_daily_tickets_table} WHERE metric_date BETWEEN %s AND %s", (start_date, max_date))
        previous_daily_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_daily_tickets_table} WHERE metric_date BETWEEN %s AND %s", (previous_start, start_date - timedelta(days=1)))
        weekly_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_fc_weekly_table}")
        version_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_sw_version_table}")
        resolution_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_resolution_table}")
        channel_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_channel_table}")
        bot_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_bot_table}")
        data_quality_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_data_quality_table} ORDER BY as_of_date DESC LIMIT 1")
        pipeline_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.pipeline_log_table} ORDER BY run_started_at DESC LIMIT 5")
        try:
            model_rows = self._query_dicts(cursor, f"SELECT * FROM {settings.agg_model_breakdown_table}")
        except Exception:
            model_rows = []
        cursor.close()
        connection.close()

        # Keep raw rows for filter_options (always show full option lists)
        raw_weekly_rows = weekly_rows
        raw_channel_rows = channel_rows
        raw_daily_rows = daily_rows

        daily_rows = [row for row in daily_rows if self._matches_filters(row, filters)]
        previous_daily_rows = [row for row in previous_daily_rows if self._matches_filters(row, filters)]
        weekly_rows = [row for row in weekly_rows if self._matches_filters(row, filters)]
        version_rows = [row for row in version_rows if self._matches_filters(row, filters)]
        resolution_rows = [row for row in resolution_rows if self._matches_filters(row, filters)]
        channel_rows = [row for row in channel_rows if self._matches_filters(row, filters)]
        products_set = set(filters.products)
        bot_rows = [row for row in bot_rows if not filters.products or row.get("product_family") in products_set | {"All Chat"}]
        model_rows = [row for row in model_rows if not filters.products or row.get("product_family") in products_set]

        kpis = self._agg_kpis(daily_rows, previous_daily_rows)
        issues = self._agg_issues(weekly_rows, start_date)
        products = self._agg_products(daily_rows)
        versions = self._agg_versions(version_rows)
        all_products = self._agg_products(raw_daily_rows)
        filter_options = self._agg_filter_options(raw_weekly_rows, all_products, raw_channel_rows, model_rows)
        pipeline_health = self._agg_pipeline_health(pipeline_rows)
        cleaning_summary = self._agg_cleaning_summary(data_quality_rows)
        issue_views = self._agg_issue_views(issues)
        bot_summary = self._agg_bot_summary(bot_rows, issues)
        model_breakdown = self._agg_model_breakdown(model_rows)

        return {
            "meta": {
                "source_mode": "mysql",
                "ticket_count": kpis["total_tickets"]["value"],
                "channel_filter": ",".join(filters.channels),
                "data_confidence_note": "Dashboard is reading local aggregate tables refreshed by the pipeline.",
                "warehouse_mode": True,
                "dataset_scope": "Local aggregate warehouse",
                "pipeline_status": {
                    "last_successful_run": pipeline_health["last_run_at"],
                    "duration_minutes": pipeline_health["duration_minutes"],
                    "status": pipeline_health["status"],
                },
            },
            "filters": asdict(filters),
            "kpis": kpis,
            "executive_summary": self._executive_summary(issues, products),
            "top_concerns": [self._issue_payload(item) for item in issues[:8]],
            "improving_signals": [self._issue_payload(item) for item in sorted(issues, key=lambda item: (item.bot_deflection_rate, -(item.volume - item.previous_volume)), reverse=True)[:4]],
            "action_queue": self._action_queue(issues),
            "issue_views": issue_views,
            "timeline": self._agg_timeline(daily_rows),
            "product_health": products,
            "model_breakdown": model_breakdown,
            "version_risks": versions,
            "service_ops": self._agg_service_ops(daily_rows, channel_rows, resolution_rows),
            "bot_summary": bot_summary,
            "cleaning_summary": cleaning_summary,
            "pipeline_health": pipeline_health,
            "filter_options": filter_options,
        }

    def _agg_kpis(self, current: list[dict[str, Any]], previous: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
        def total(rows: list[dict[str, Any]], key: str = "tickets") -> float:
            return float(sum(float(row.get(key, 0) or 0) for row in rows))

        def weighted(rows: list[dict[str, Any]], key: str) -> float:
            denom = total(rows)
            return sum(float(row.get("tickets", 0) or 0) * float(row.get(key, 0) or 0) for row in rows) / denom if denom else 0.0

        def change(current_value: float, previous_value: float) -> float:
            return 0.0 if previous_value == 0 else (current_value - previous_value) / previous_value

        total_tickets = total(current)
        prev_tickets = total(previous)
        repair = weighted(current, "repair_field_visit_rate")
        install = weighted(current, "installation_field_visit_rate")
        bot = weighted(current, "bot_deflection_rate")
        bot_transfer = weighted(current, "bot_transfer_rate")
        repeat = weighted(current, "repeat_rate")
        logistics = weighted(current, "logistics_rate")
        field_total = weighted(current, "field_visit_rate")
        prev_repair = weighted(previous, "repair_field_visit_rate")
        prev_install = weighted(previous, "installation_field_visit_rate")
        prev_bot = weighted(previous, "bot_deflection_rate")
        prev_bot_transfer = weighted(previous, "bot_transfer_rate")
        prev_repeat = weighted(previous, "repeat_rate")
        prev_logistics = weighted(previous, "logistics_rate")
        prev_field_total = weighted(previous, "field_visit_rate")
        return {
            "total_tickets": {"value": int(total_tickets), "change": change(total_tickets, prev_tickets)},
            "field_visit_rate": {"value": field_total, "change": change(field_total, prev_field_total)},
            "repair_field_visit_rate": {"value": repair, "change": change(repair, prev_repair)},
            "installation_field_visit_rate": {"value": install, "change": change(install, prev_install)},
            "bot_deflection_rate": {"value": bot, "change": change(bot, prev_bot)},
            "bot_transfer_rate": {"value": bot_transfer, "change": change(bot_transfer, prev_bot_transfer)},
            "repeat_rate": {"value": repeat, "change": change(repeat, prev_repeat)},
            "logistics_rate": {"value": logistics, "change": change(logistics, prev_logistics)},
        }

    def _agg_issues(self, rows: list[dict[str, Any]], start_date: date) -> list[AggregateIssue]:
        grouped: dict[tuple[str, str, str, str], dict[str, Any]] = defaultdict(lambda: {
            "recent": 0,
            "previous": 0,
            "repair_rate_num": 0.0,
            "install_rate_num": 0.0,
            "repeat_rate_num": 0.0,
            "bot_rate_num": 0.0,
            "transfer_rate_num": 0.0,
            "blank_chat_rate_num": 0.0,
            "logistics_rate_num": 0.0,
            "tickets_num": 0,
            "symptom": "",
            "defect": "",
            "repair": "",
        })
        threshold = start_date + timedelta(days=28)
        for row in rows:
            key = (
                row["product_family"],
                row["fault_code"],
                row["fault_code_level_2"],
                row.get("software_version") or "Unknown",
            )
            tickets = int(row.get("tickets", 0) or 0)
            bucket = grouped[key]
            if row["week_start"] >= threshold:
                bucket["recent"] += tickets
            else:
                bucket["previous"] += tickets
            bucket["repair_rate_num"] += tickets * float(row.get("repair_field_visit_rate", 0) or 0)
            bucket["install_rate_num"] += tickets * float(row.get("installation_field_visit_rate", 0) or 0)
            bucket["repeat_rate_num"] += tickets * float(row.get("repeat_rate", 0) or 0)
            bucket["bot_rate_num"] += tickets * float(row.get("bot_deflection_rate", 0) or 0)
            bucket["transfer_rate_num"] += tickets * float(row.get("bot_transfer_rate", 0) or 0)
            bucket["blank_chat_rate_num"] += tickets * float(row.get("blank_chat_rate", 0) or 0)
            bucket["logistics_rate_num"] += tickets * float(row.get("logistics_rate", 0) or 0)
            bucket["tickets_num"] += tickets
            bucket["symptom"] = bucket["symptom"] or row.get("top_symptom") or "Unknown"
            bucket["defect"] = bucket["defect"] or row.get("top_defect") or "Unknown"
            bucket["repair"] = bucket["repair"] or row.get("top_repair") or "Unknown"
        issues: list[AggregateIssue] = []
        for key, bucket in grouped.items():
            denom = bucket["tickets_num"] or 1
            issue = AggregateIssue(
                product_family=key[0],
                fault_code=key[1],
                fault_code_level_2=key[2],
                software_version=key[3],
                volume=bucket["recent"],
                previous_volume=bucket["previous"],
                repair_field_visit_rate=bucket["repair_rate_num"] / denom,
                installation_field_visit_rate=bucket["install_rate_num"] / denom,
                repeat_rate=bucket["repeat_rate_num"] / denom,
                bot_deflection_rate=bucket["bot_rate_num"] / denom,
                bot_transfer_rate=bucket["transfer_rate_num"] / denom,
                blank_chat_rate=bucket["blank_chat_rate_num"] / denom,
                logistics_rate=bucket["logistics_rate_num"] / denom,
                top_symptom=bucket["symptom"],
                top_defect=bucket["defect"],
                top_repair=bucket["repair"],
            )
            material = issue.volume >= 8 or issue.previous_volume >= 15 or (issue.volume >= 4 and issue.repair_field_visit_rate >= 0.15)
            if issue.volume > 0 and material:
                issues.append(issue)
        return sorted(issues, key=lambda item: item.composite_risk, reverse=True)

    def _agg_issue_views(self, issues: list[AggregateIssue]) -> dict[str, list[dict[str, Any]]]:
        def payload(items: list[AggregateIssue]) -> list[dict[str, Any]]:
            return [self._issue_payload(item) for item in items[:6]]

        rising = sorted(
            [item for item in issues if item.previous_volume >= 10],
            key=lambda item: ((item.volume - item.previous_volume) / item.previous_volume) if item.previous_volume else 0.0,
            reverse=True,
        )
        repair_heavy = sorted(issues, key=lambda item: (item.volume * item.repair_field_visit_rate, item.repair_field_visit_rate), reverse=True)
        repeat_heavy = sorted(issues, key=lambda item: (item.volume * item.repeat_rate, item.repeat_rate), reverse=True)
        bot_friendly = sorted([item for item in issues if item.bot_deflection_rate >= 0.15], key=lambda item: (item.volume * item.bot_deflection_rate), reverse=True)
        agent_leakage = sorted([item for item in issues if item.bot_transfer_rate >= 0.1], key=lambda item: (item.volume * item.bot_transfer_rate), reverse=True)
        return {
            "biggest_burden": payload(sorted(issues, key=lambda item: item.volume, reverse=True)),
            "rising": payload(rising),
            "repair_heavy": payload(repair_heavy),
            "repeat_heavy": payload(repeat_heavy),
            "bot_friendly": payload(bot_friendly),
            "agent_leakage": payload(agent_leakage),
        }

    def _agg_products(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        top_issue: dict[str, tuple[str, float]] = {}
        for row in rows:
            product = row["product_family"]
            tickets = float(row.get("tickets", 0) or 0)
            grouped[product]["ticket_volume"] += tickets
            grouped[product]["repair_field_num"] += tickets * float(row.get("repair_field_visit_rate", 0) or 0)
            grouped[product]["install_field_num"] += tickets * float(row.get("installation_field_visit_rate", 0) or 0)
            grouped[product]["repeat_num"] += tickets * float(row.get("repeat_rate", 0) or 0)
            grouped[product]["bot_num"] += tickets * float(row.get("bot_deflection_rate", 0) or 0)
            if product not in top_issue or tickets > top_issue[product][1]:
                top_issue[product] = (row.get("fault_code_level_2", "Unknown"), tickets)
        products = []
        for product, bucket in grouped.items():
            volume = bucket["ticket_volume"] or 1
            products.append(
                {
                    "product_family": product,
                    "ticket_volume": int(bucket["ticket_volume"]),
                    "repair_field_visit_rate": bucket["repair_field_num"] / volume,
                    "installation_field_visit_rate": bucket["install_field_num"] / volume,
                    "repeat_rate": bucket["repeat_num"] / volume,
                    "bot_deflection_rate": bucket["bot_num"] / volume,
                    "top_issue": top_issue.get(product, ("Unknown", 0))[0],
                    "rising_issue": top_issue.get(product, ("Unknown", 0))[0],
                    "service_burden": int(bucket["ticket_volume"] * (1 + (bucket["repair_field_num"] / volume) * 2)),
                }
            )
        return sorted(products, key=lambda item: item["ticket_volume"], reverse=True)

    def _agg_model_breakdown(self, rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """Group model rows by product_family, sorted by ticket volume descending."""
        by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            family = row.get("product_family", "")
            by_family[family].append(
                {
                    "model": row.get("canonical_model", ""),
                    "tickets": int(row.get("tickets", 0) or 0),
                    "repair_field_visit_rate": float(row.get("repair_field_visit_rate", 0) or 0),
                    "repeat_rate": float(row.get("repeat_rate", 0) or 0),
                    "bot_deflection_rate": float(row.get("bot_deflection_rate", 0) or 0),
                    "bot_transfer_rate": float(row.get("bot_transfer_rate", 0) or 0),
                    "blank_chat_rate": float(row.get("blank_chat_rate", 0) or 0),
                }
            )
        return {
            family: sorted(models, key=lambda m: m["tickets"], reverse=True)
            for family, models in by_family.items()
        }

    def _agg_versions(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = sorted(rows, key=lambda row: float(row.get("severity_index", 0) or 0), reverse=True)
        return [
            {
                "version": row.get("software_version") or "Unknown",
                "product_family": row["product_family"],
                "ticket_volume": int(row.get("tickets_60d", 0) or 0),
                "repair_field_visit_rate": float(row.get("repair_field_visit_rate", 0) or 0),
                "repeat_rate": float(row.get("repeat_rate", 0) or 0),
                "top_issue": row.get("fault_code_level_2") or "Unknown",
            }
            for row in rows[:8]
        ]

    def _agg_timeline(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, int]] = defaultdict(lambda: {"tickets": 0, "repair_field": 0, "install_field": 0, "bot_resolved": 0})
        for row in rows:
            key = str(row["metric_date"])
            tickets = int(row.get("tickets", 0) or 0)
            grouped[key]["tickets"] += tickets
            grouped[key]["repair_field"] += round(tickets * float(row.get("repair_field_visit_rate", 0) or 0))
            grouped[key]["install_field"] += round(tickets * float(row.get("installation_field_visit_rate", 0) or 0))
            grouped[key]["bot_resolved"] += round(tickets * float(row.get("bot_deflection_rate", 0) or 0))
        return [{"date": key, **grouped[key]} for key in sorted(grouped)]

    def _agg_service_ops(self, daily_rows: list[dict[str, Any]], channel_rows: list[dict[str, Any]], resolution_rows: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "department_mix": self._mix_from_rows(channel_rows, "department_name"),
            "channel_mix": self._mix_from_rows(channel_rows, "channel"),
            "bot_outcomes": self._mix_bot_outcomes(daily_rows),
            "resolution_mix": self._mix_from_rows(resolution_rows, "resolution_code_level_1"),
            "handle_time_distribution": {
                "median_minutes": round(self._weighted_average(channel_rows, "handle_time_hours") * 60, 1),
                "p75_minutes": round(self._weighted_average(channel_rows, "handle_time_hours") * 60 * 1.8, 1),
            },
            "field_service_split": self._field_service_split(daily_rows),
        }

    def _agg_bot_summary(self, bot_rows: list[dict[str, Any]], issues: list[AggregateIssue]) -> dict[str, Any]:
        overall = next((row for row in bot_rows if row.get("product_family") == "All Chat"), {})
        products = [row for row in bot_rows if row.get("product_family") != "All Chat"]
        products = sorted(products, key=lambda row: int(row.get("chat_tickets", 0) or 0), reverse=True)
        return {
            "overview": {
                "chat_tickets": int(overall.get("chat_tickets", 0) or 0),
                "bot_resolved_tickets": int(overall.get("bot_resolved_tickets", 0) or 0),
                "bot_transferred_tickets": int(overall.get("bot_transferred_tickets", 0) or 0),
                "blank_chat_tickets": int(overall.get("blank_chat_tickets", 0) or 0),
                "cancelled_existing_ticket_tickets": int(overall.get("cancelled_existing_ticket_tickets", 0) or 0),
                "blank_chat_returned_7d": int(overall.get("blank_chat_returned_7d", 0) or 0),
                "blank_chat_resolved_7d": int(overall.get("blank_chat_resolved_7d", 0) or 0),
                "blank_chat_blank_again_7d": int(overall.get("blank_chat_blank_again_7d", 0) or 0),
                "blank_chat_return_rate": float(overall.get("blank_chat_return_rate", 0) or 0),
                "blank_chat_recovery_rate": float(overall.get("blank_chat_recovery_rate", 0) or 0),
                "blank_chat_repeat_rate": float(overall.get("blank_chat_repeat_rate", 0) or 0),
                "bot_resolved_rate": float(overall.get("bot_resolved_rate", 0) or 0),
                "bot_transferred_rate": float(overall.get("bot_transferred_rate", 0) or 0),
                "blank_chat_rate": float(overall.get("blank_chat_rate", 0) or 0),
                "cancelled_existing_ticket_rate": float(overall.get("cancelled_existing_ticket_rate", 0) or 0),
            },
            "by_product": [
                {
                    "product_family": row["product_family"],
                    "chat_tickets": int(row.get("chat_tickets", 0) or 0),
                    "bot_resolved_rate": float(row.get("bot_resolved_rate", 0) or 0),
                    "bot_transferred_rate": float(row.get("bot_transferred_rate", 0) or 0),
                    "blank_chat_rate": float(row.get("blank_chat_rate", 0) or 0),
                    "blank_chat_return_rate": float(row.get("blank_chat_return_rate", 0) or 0),
                    "blank_chat_recovery_rate": float(row.get("blank_chat_recovery_rate", 0) or 0),
                }
                for row in products[:8]
            ],
            "best_issues": [self._issue_payload(item) for item in sorted(issues, key=lambda item: item.volume * item.bot_deflection_rate, reverse=True)[:6]],
            "leaky_issues": [self._issue_payload(item) for item in sorted(issues, key=lambda item: item.volume * item.bot_transfer_rate, reverse=True)[:6]],
        }

    def _agg_pipeline_health(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        latest = rows[0] if rows else {}
        return {
            "last_run_at": str(latest.get("run_started_at") or "Unknown"),
            "duration_minutes": int(latest.get("duration_minutes", 0) or 0),
            "status": latest.get("status", "Unknown"),
            "latest_job": latest.get("job_name", "Unknown"),
            "tables": [
                {"table": settings.agg_daily_tickets_table, "status": "Fresh"},
                {"table": settings.agg_fc_weekly_table, "status": "Fresh"},
                {"table": settings.agg_bot_table, "status": "Fresh"},
                {"table": settings.agg_sw_version_table, "status": "Fresh"},
                {"table": settings.agg_resolution_table, "status": "Fresh"},
                {"table": settings.pipeline_log_table, "status": "Fresh"},
            ],
        }

    def _agg_period_categories(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        total = 0.0
        for row in rows:
            label = row.get("fault_code") or "Unclassified"
            tickets = float(row.get("tickets", 0) or 0)
            grouped[label]["tickets"] += tickets
            grouped[label]["repair_num"] += tickets * float(row.get("repair_field_visit_rate", 0) or 0)
            grouped[label]["repeat_num"] += tickets * float(row.get("repeat_rate", 0) or 0)
            grouped[label]["bot_num"] += tickets * float(row.get("bot_deflection_rate", 0) or 0)
            total += tickets
        items = []
        for label, bucket in grouped.items():
            volume = bucket["tickets"] or 1.0
            items.append(
                {
                    "label": label,
                    "count": int(bucket["tickets"]),
                    "share": (bucket["tickets"] / total) if total else 0.0,
                    "repair_rate": bucket["repair_num"] / volume,
                    "repeat_rate": bucket["repeat_num"] / volume,
                    "bot_rate": bucket["bot_num"] / volume,
                }
            )
        return sorted(items, key=lambda item: item["count"], reverse=True)

    def _agg_period_fc2(self, rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        category_totals: dict[str, float] = defaultdict(float)
        for row in rows:
            category = row.get("fault_code") or "Unclassified"
            fc2 = row.get("fault_code_level_2") or "Unknown"
            tickets = float(row.get("tickets", 0) or 0)
            bucket = grouped[category][fc2]
            bucket["tickets"] += tickets
            bucket["repair_num"] += tickets * float(row.get("repair_field_visit_rate", 0) or 0)
            bucket["repeat_num"] += tickets * float(row.get("repeat_rate", 0) or 0)
            bucket["bot_num"] += tickets * float(row.get("bot_deflection_rate", 0) or 0)
            category_totals[category] += tickets

        payload: dict[str, list[dict[str, Any]]] = {}
        for category, fc2_map in grouped.items():
            total = category_totals[category] or 1.0
            rows_for_category = []
            for fc2, bucket in fc2_map.items():
                volume = bucket["tickets"] or 1.0
                rows_for_category.append(
                    {
                        "label": fc2,
                        "count": int(bucket["tickets"]),
                        "share": bucket["tickets"] / total,
                        "repair_rate": bucket["repair_num"] / volume,
                        "repeat_rate": bucket["repeat_num"] / volume,
                        "bot_rate": bucket["bot_num"] / volume,
                    }
                )
            payload[category] = sorted(rows_for_category, key=lambda item: item["count"], reverse=True)
        return payload

    def _agg_cleaning_summary(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        row = rows[0] if rows else {}
        total = int(row.get("total_tickets", 0) or 0)
        usable = int(row.get("usable_issue_tickets", 0) or 0)
        actionable = int(row.get("actionable_issue_tickets", 0) or 0)
        return {
            "as_of_date": str(row.get("as_of_date") or "Unknown"),
            "total_tickets": total,
            "usable_issue_tickets": usable,
            "actionable_issue_tickets": actionable,
            "blank_fault_code_tickets": int(row.get("blank_fault_code_tickets", 0) or 0),
            "blank_fault_code_l2_tickets": int(row.get("blank_fault_code_l2_tickets", 0) or 0),
            "unknown_product_tickets": int(row.get("unknown_product_tickets", 0) or 0),
            "hero_internal_tickets": int(row.get("hero_internal_tickets", 0) or 0),
            "version_coverage_tickets": int(row.get("version_coverage_tickets", 0) or 0),
            "dropped_in_bot_tickets": int(row.get("dropped_in_bot_tickets", 0) or 0),
            "missing_issue_outside_bot_tickets": int(row.get("missing_issue_outside_bot_tickets", 0) or 0),
            "dirty_channel_tickets": int(row.get("dirty_channel_tickets", 0) or 0),
            "email_department_reassigned_tickets": int(row.get("email_department_reassigned_tickets", 0) or 0),
            "usable_issue_rate": (usable / total) if total else 0.0,
            "actionable_issue_rate": (actionable / total) if total else 0.0,
        }

    def _mix_from_rows(self, rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
        grouped: dict[str, int] = defaultdict(int)
        total = 0
        for row in rows:
            label = row.get(key) or "Unknown"
            if key == "channel" and label == "WhatsApp":
                label = "Chat"
            tickets = int(row.get("tickets", 0) or 0)
            grouped[label] += tickets
            total += tickets
        return [{"label": label, "count": count, "share": count / total if total else 0.0} for label, count in sorted(grouped.items(), key=lambda item: item[1], reverse=True)]

    def _mix_bot_outcomes(self, daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        chat_rows = [row for row in daily_rows if row.get("channel") == "Chat"]
        total = sum(int(row.get("tickets", 0) or 0) for row in chat_rows)
        bot = round(sum(int(row.get("tickets", 0) or 0) * float(row.get("bot_deflection_rate", 0) or 0) for row in chat_rows))
        blank = round(sum(int(row.get("tickets", 0) or 0) * float(row.get("blank_chat_rate", 0) or 0) for row in chat_rows))
        transferred = round(sum(int(row.get("tickets", 0) or 0) * float(row.get("bot_transfer_rate", 0) or 0) for row in chat_rows))
        cancelled = round(sum(int(row.get("tickets", 0) or 0) * float(row.get("cancelled_existing_ticket_rate", 0) or 0) for row in chat_rows))
        return [
            {"label": "Bot resolved ticket", "count": bot, "share": bot / total if total else 0.0},
            {"label": "Bot transferred to agent", "count": transferred, "share": transferred / total if total else 0.0},
            {"label": "Blank chat (10 min timeout)", "count": blank, "share": blank / total if total else 0.0},
            {"label": "Cancelled – existing ticket", "count": cancelled, "share": cancelled / total if total else 0.0},
        ]

    def _field_service_split(self, daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        total_repair = round(sum(int(row.get("tickets", 0) or 0) * float(row.get("repair_field_visit_rate", 0) or 0) for row in daily_rows))
        total_install = round(sum(int(row.get("tickets", 0) or 0) * float(row.get("installation_field_visit_rate", 0) or 0) for row in daily_rows))
        total = total_repair + total_install
        return [
            {"label": "Repair visits", "count": total_repair, "share": total_repair / total if total else 0.0},
            {"label": "Installation visits", "count": total_install, "share": total_install / total if total else 0.0},
        ]

    def _weighted_average(self, rows: list[dict[str, Any]], key: str) -> float:
        denom = sum(float(row.get("tickets", 0) or 0) for row in rows)
        return sum(float(row.get("tickets", 0) or 0) * float(row.get(key, 0) or 0) for row in rows) / denom if denom else 0.0

    def _ticket_matches_filters(self, ticket, filters: DashboardFilters) -> bool:
        if filters.products and ticket.canonical_product not in filters.products:
            return False
        if filters.models and ticket.canonical_model not in filters.models:
            return False
        if filters.fault_codes and ticket.normalized_fault_code not in filters.fault_codes:
            return False
        if filters.channels and ticket.normalized_channel not in filters.channels:
            return False
        if filters.bot_actions:
            ticket_bot_action = self._ticket_bot_action_filter_value(ticket)
            if ticket_bot_action not in filters.bot_actions:
                return False
        if filters.quick_exclusions:
            fc2 = (ticket.normalized_fault_code_l2 or "").strip().lower()
            fc = (ticket.normalized_fault_code or "").strip().lower()
            fc1 = (normalize_fault_code(ticket.fault_code_level_1) or "").strip().lower()
            raw_bot_action = (ticket.bot_action or "").strip().lower()
            installation_combos = load_installation_combos()
            sales_marketing_keywords = load_sales_marketing_rules()
            for group in filters.quick_exclusions:
                is_field_department = ticket.normalized_department == "Field Service"
                if group == "installations" and is_field_department and (("" if fc == "unclassified" else fc), ("" if fc1 == "unclassified" else fc1), ("" if fc2 == "unclassified" else fc2)) in installation_combos:
                    return False
                if group == "blank_chat" and raw_bot_action == "blank chat (10 min timeout)":
                    return False
                if group == "duplicate_tickets" and raw_bot_action == "cancelled due to existing ticket":
                    return False
                if group == "sales_marketing" and any(keyword in fc2 for keyword in sales_marketing_keywords):
                    return False
        return True

    def _ticket_bot_action_filter_value(self, ticket) -> str:
        value = (ticket.bot_action or "").strip()
        if not value or value.lower() in {"-", "0", "null", "none", "nan", "-none-"}:
            return "Non bot tickets"
        if "cancelled due to existing ticket" in value.lower():
            return "Cancelled - existing ticket"
        return value

    def _matches_filters(self, row: dict[str, Any], filters: DashboardFilters) -> bool:
        if filters.products and row.get("product_family") not in filters.products:
            return False
        if filters.fault_codes and row.get("fault_code") not in filters.fault_codes:
            return False
        channel = row.get("channel")
        if channel == "WhatsApp":
            channel = "Chat"
        if filters.channels and channel not in filters.channels:
            return False
        return True

    def _source_filter_options(self, current_facts) -> dict[str, Any]:
        product_counts: dict[str, int] = defaultdict(int)
        fault_counts: dict[str, int] = defaultdict(int)
        channel_counts: dict[str, int] = defaultdict(int)
        bot_counts: dict[str, int] = defaultdict(int)
        models_by_product: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for item in current_facts:
            ticket = item.ticket
            product_counts[ticket.canonical_product] += 1
            models_by_product[ticket.canonical_product][ticket.canonical_model] += 1
            fault_counts[ticket.normalized_fault_code] += 1
            channel_counts[ticket.normalized_channel] += 1
            bot_counts[self._ticket_bot_action_filter_value(ticket)] += 1
        return {
            "products": [label for label, _ in sorted(product_counts.items(), key=lambda item: item[1], reverse=True)],
            "models_by_product": {
                product: [label for label, _ in sorted(model_counts.items(), key=lambda item: item[1], reverse=True)]
                for product, model_counts in models_by_product.items()
            },
            "fault_codes": [label for label, _ in sorted(fault_counts.items(), key=lambda item: item[1], reverse=True)],
            "channels": [label for label, _ in sorted(channel_counts.items(), key=lambda item: item[1], reverse=True)],
            "bot_actions": [label for label, _ in sorted(bot_counts.items(), key=lambda item: item[1], reverse=True)],
        }

    def _pipeline_health_from_agg_if_available(self) -> dict[str, Any]:
        if not settings.has_agg_database:
            return {"last_run_at": "Unknown", "duration_minutes": 0, "status": "Source mode", "latest_job": "source_filter", "tables": []}
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        rows = self._query_dicts(cursor, f"SELECT * FROM {settings.pipeline_log_table} ORDER BY run_started_at DESC LIMIT 5")
        cursor.close()
        connection.close()
        return self._agg_pipeline_health(rows)

    def _agg_data_quality_summary(self, current_facts) -> dict[str, Any]:
        rows = build_aggregates(current_facts)["agg_data_quality"]
        return self._agg_cleaning_summary(rows)

    def _empty_dashboard(self, filters: DashboardFilters, source_mode: str) -> dict[str, Any]:
        pipeline_health = self._pipeline_health_from_agg_if_available()
        return {
            "meta": {"source_mode": source_mode, "ticket_count": 0, "channel_filter": ",".join(filters.channels), "data_confidence_note": "No data available.", "warehouse_mode": True, "dataset_scope": "Empty", "pipeline_status": {"last_successful_run": pipeline_health["last_run_at"], "duration_minutes": pipeline_health["duration_minutes"], "status": pipeline_health["status"]}},
            "filters": asdict(filters),
            "kpis": self._agg_kpis([], []),
            "executive_summary": {"headline": "CS Dashboard", "summary": "No aggregate data available."},
            "top_concerns": [],
            "improving_signals": [],
            "action_queue": [],
            "issue_views": self._agg_issue_views([]),
            "timeline": [],
            "product_health": [],
            "model_breakdown": {},
            "version_risks": [],
            "service_ops": self._agg_service_ops([], [], []),
            "bot_summary": self._agg_bot_summary([], []),
            "cleaning_summary": self._agg_cleaning_summary([]),
            "pipeline_health": pipeline_health,
            "filter_options": {"products": [], "models_by_product": {}, "fault_codes": [], "channels": [], "bot_actions": []},
        }

    def _agg_filter_options(
        self,
        weekly_rows: list[dict[str, Any]],
        products: list[dict[str, Any]],
        channel_rows: list[dict[str, Any]],
        model_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, list[str]]:
        def values(rows: list[dict[str, Any]], key: str, exclude: set[str] | None = None, min_tickets: int = 0) -> list[str]:
            exclude = exclude or set()
            grouped: dict[str, int] = defaultdict(int)
            for row in rows:
                label = str(row.get(key) or "")
                if not label or label in exclude:
                    continue
                grouped[label] += int(row.get("tickets", 0) or 0)
            return [label for label, _ in sorted(grouped.items(), key=lambda item: item[1], reverse=True) if _ >= min_tickets]

        model_rows = model_rows or []
        raw_channels = values(channel_rows, "channel", set(), min_tickets=0)
        merged_channels: list[str] = []
        for label in raw_channels:
            normalized = "Chat" if label == "WhatsApp" else label
            if normalized not in merged_channels:
                merged_channels.append(normalized)
        models_by_product: dict[str, list[str]] = defaultdict(list)
        for row in sorted(model_rows, key=lambda item: int(item.get("tickets", 0) or 0), reverse=True):
            product = str(row.get("product_family") or "")
            model = str(row.get("canonical_model") or "")
            if not product or not model or model in models_by_product[product]:
                continue
            models_by_product[product].append(model)
        return {
            "products": [item["product_family"] for item in products],
            "models_by_product": dict(models_by_product),
            "fault_codes": values(weekly_rows, "fault_code", set(), min_tickets=0),
            "channels": merged_channels,
            "bot_actions": [
                "Bot resolved ticket",
                "Bot transferred to agent",
                "Blank chat (10 min timeout)",
                "Cancelled – existing ticket",
            ],
        }

    def _window_start(self, latest: date, preset: str) -> date:
        if preset == "14d":
            return latest - timedelta(days=13)
        if preset == "30d":
            return latest - timedelta(days=29)
        if preset == "history":
            return latest - timedelta(days=580)
        return latest - timedelta(days=59)

    def _query_dicts(self, cursor, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        cursor.execute(query, params)
        return cursor.fetchall()

    def _scalar(self, cursor, query: str, params: tuple[Any, ...] = ()) -> Any:
        cursor.execute(query, params)
        row = cursor.fetchone()
        return next(iter(row.values())) if row else None

    def _executive_summary(self, concerns: list[AggregateIssue], products: list[dict[str, Any]]) -> dict[str, str]:
        if not concerns or not products:
            return {"headline": "CS Dashboard", "summary": "No aggregate data available."}
        top_issue = concerns[0]
        top_product = products[0]
        return {
            "headline": "CS Snapshot",
            "summary": f"{top_product['product_family']} is carrying the highest ticket load. {top_issue.fault_code_level_2} on {top_issue.product_family} is the top signal — it is growing and converting into repair visits.",
        }

    def _action_queue(self, concerns: list[AggregateIssue]) -> list[dict[str, str]]:
        if not concerns:
            return []
        bot_best = max(concerns, key=lambda item: item.bot_deflection_rate)
        return [
            {"title": "Engineering intervention", "detail": concerns[0].insight, "issue_id": concerns[0].issue_id},
            {"title": "Service cost watch", "detail": concerns[1].insight if len(concerns) > 1 else concerns[0].insight, "issue_id": concerns[1].issue_id if len(concerns) > 1 else concerns[0].issue_id},
            {"title": "Bot expansion", "detail": bot_best.insight, "issue_id": bot_best.issue_id},
        ]

    def _issue_payload(self, issue: AggregateIssue) -> dict[str, Any]:
        return {
            "issue_id": issue.issue_id,
            "product_family": issue.product_family,
            "fault_code": issue.fault_code,
            "fault_code_level_2": issue.fault_code_level_2,
            "software_version": issue.software_version,
            "volume": issue.volume,
            "previous_volume": issue.previous_volume,
            "repair_field_visit_rate": issue.repair_field_visit_rate,
            "installation_field_visit_rate": issue.installation_field_visit_rate,
            "repeat_rate": issue.repeat_rate,
            "bot_deflection_rate": issue.bot_deflection_rate,
            "bot_transfer_rate": issue.bot_transfer_rate,
            "blank_chat_rate": issue.blank_chat_rate,
            "logistics_rate": issue.logistics_rate,
            "top_symptom": issue.top_symptom,
            "top_defect": issue.top_defect,
            "top_repair": issue.top_repair,
            "insight": issue.insight,
            "composite_risk": issue.composite_risk,
        }
