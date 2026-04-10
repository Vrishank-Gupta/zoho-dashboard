from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
import json
import time
from typing import Any
from zoneinfo import ZoneInfo

from .clickhouse_analytics import ClickHouseAnalyticsRepository
from .config import settings
from .mapping import load_mappings, map_executive_fault_code, map_product_category
from .models import DashboardFilters
from .repository import TicketRepository


OPEN_STATUS_MARKERS = ("open", "escal", "pending", "progress", "wip")


@dataclass(slots=True)
class AggregateIssue:
    product_category: str
    product_name: str
    executive_fault_code: str
    fault_code: str
    fault_code_level_1: str
    fault_code_level_2: str
    volume: int
    previous_volume: int
    installation_rate: float
    repeat_rate: float
    bot_resolved_rate: float
    bot_transfer_rate: float
    blank_chat_rate: float
    fcr_rate: float
    logistics_rate: float
    top_symptom: str
    top_defect: str
    top_repair: str

    @property
    def issue_id(self) -> str:
        return "|".join([self.product_category, self.product_name, self.executive_fault_code, self.fault_code_level_2])

    @property
    def delta_rate(self) -> float:
        if self.previous_volume <= 0:
            return 0.0
        return (self.volume - self.previous_volume) / self.previous_volume

    @property
    def insight(self) -> str:
        return f"{self.fault_code_level_2} drives {self.volume:,} tickets in {self.product_name}. Installation tickets are {self.installation_rate:.1%} and bot transfer is {self.bot_transfer_rate:.1%}."


class AnalyticsService:
    def __init__(self, repository: TicketRepository) -> None:
        self._repository = repository
        self._clickhouse = ClickHouseAnalyticsRepository() if settings.has_clickhouse else None
        self._display_tz = ZoneInfo(settings.normalized_etl_timezone)
        self._cache_ttl_seconds = 20
        self._cache: dict[str, tuple[float, Any]] = {}
        self._freshness_ttl_seconds = 300
        self._freshness_cache: tuple[float, dict[str, str]] | None = None

    def invalidate_cache(self) -> None:
        self._cache.clear()
        self._freshness_cache = None

    def build_dashboard(self, filters: DashboardFilters) -> dict[str, Any]:
        cached = self._cache_get("dashboard", filters)
        if cached is not None:
            return cached
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            try:
                payload = self._build_from_clickhouse(filters)
                self._cache_set("dashboard", filters, payload)
                return payload
            except Exception as exc:
                return self._build_seeded(filters, str(exc))
        return self._build_seeded(filters, "ClickHouse is not configured.")

    def get_issue_tickets(self, filters: DashboardFilters, issue_id: str) -> dict[str, Any]:
        issue = self._find_issue(filters, issue_id)
        if not issue:
            return {"issue": None, "tickets": []}
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            tickets = [self._remap_row(ticket, filters) for ticket in self._clickhouse.fetch_issue_tickets(filters, issue_id, limit=24)]
            return {"issue": issue, "tickets": [self._format_clickhouse_ticket(ticket) for ticket in tickets if self._matches_mapping_filters(ticket, filters)]}
        return {"issue": issue, "tickets": []}

    def get_product_drilldown(self, filters: DashboardFilters, category: str, product_name: str) -> dict[str, Any]:
        cached = self._cache_get("product_drilldown", filters, {"category": category, "product_name": product_name})
        if cached is not None:
            return cached
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            drilldown = self._remap_product_drilldown(
                self._clickhouse.fetch_product_drilldown(filters, category, product_name),
                filters,
            )
            payload = {
                "meta": {"category": category, "product_name": product_name},
                "drilldown": drilldown,
            }
            self._cache_set("product_drilldown", filters, payload, {"category": category, "product_name": product_name})
            return payload
        return {"meta": {"category": category, "product_name": product_name}, "drilldown": {}}

    def get_category_drilldown(self, filters: DashboardFilters, category: str) -> dict[str, Any]:
        cached = self._cache_get("category_drilldown", filters, {"category": category})
        if cached is not None:
            return cached
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            products = [
                item["product_name"]
                for item in self._agg_product_health(
                    self._mapped_daily_rows(
                        self._clickhouse.fetch_daily_rows(*self._resolve_window(filters, self._clickhouse.fetch_min_metric_date(), self._clickhouse.fetch_max_metric_date()), filters),
                        filters,
                    ),
                    [],
                )
                if item["product_category"] == category
            ]
            drilldown = self._remap_category_drilldown(
                self._clickhouse.fetch_category_drilldown(filters, category, products),
                filters,
            )
            payload = {
                "meta": {"category": category},
                "drilldown": drilldown,
            }
            self._cache_set("category_drilldown", filters, payload, {"category": category})
            return payload
        return {"meta": {"category": category}, "drilldown": {}}

    def get_issue_drilldown(self, filters: DashboardFilters, issue_id: str) -> dict[str, Any]:
        issue = self._find_issue(filters, issue_id)
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            return {
                "issue": issue,
                "drilldown": self._clickhouse.fetch_issue_drilldown(filters, issue_id),
            }
        return {"issue": issue, "drilldown": {}}

    def get_repeat_drilldown(self, filters: DashboardFilters, kind: str, label: str, secondary: str | None = None) -> dict[str, Any]:
        if not (self._clickhouse and settings.analytics_backend == "clickhouse"):
            return {"meta": {"kind": kind, "label": label, "secondary": secondary}, "drilldown": {}}
        min_date = self._clickhouse.fetch_min_metric_date()
        max_date = self._clickhouse.fetch_max_metric_date()
        if not min_date or not max_date:
            return {"meta": {"kind": kind, "label": label, "secondary": secondary}, "drilldown": {}}
        start_date, end_date = self._resolve_window(filters, min_date, max_date)
        previous_start, previous_end = self._previous_period(start_date, end_date)
        current_rows = self._filter_repeat_drilldown_rows(self._clickhouse.fetch_repeat_rows(start_date, end_date, filters), kind, label, secondary)
        previous_rows = self._filter_repeat_drilldown_rows(self._clickhouse.fetch_repeat_rows(previous_start, previous_end, filters), kind, label, secondary)
        return {
            "meta": {"kind": kind, "label": label, "secondary": secondary},
            "drilldown": {
                "overview": self._agg_repeat_drilldown_overview(current_rows, previous_rows),
                "aging": self._agg_repeat_aging(current_rows),
                "same_issue_mix": self._agg_repeat_same_issue_mix(current_rows),
                "trend": self._agg_repeat_trend(current_rows),
                "return_efcs": self._agg_repeat_dimension(current_rows, previous_rows, "return_executive_fault_code", "return_fault_code_level_2", self._repeat_base_lookup(current_rows, "return_executive_fault_code"), self._repeat_base_lookup(previous_rows, "return_executive_fault_code")),
                "return_fc2": self._agg_repeat_dimension(current_rows, previous_rows, "return_fault_code_level_2", "return_resolution", self._repeat_base_lookup(current_rows, "return_fault_code_level_2"), self._repeat_base_lookup(previous_rows, "return_fault_code_level_2")),
                "resolution_fallout": self._agg_repeat_resolution_fallout(current_rows),
                "transitions": self._agg_repeat_transitions(current_rows),
                "channel_transitions": self._agg_repeat_channel_transitions(current_rows),
            },
        }

    def search_tickets(self, filters: DashboardFilters, query: str = "") -> list[dict[str, Any]]:
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            return self._clickhouse.search_tickets(filters, query)
        return []

    def _build_from_clickhouse(self, filters: DashboardFilters) -> dict[str, Any]:
        if not self._clickhouse:
            raise RuntimeError("ClickHouse is not configured.")
        max_date = self._clickhouse.fetch_max_metric_date()
        if not max_date:
            return self._build_seeded(filters, "ClickHouse has no loaded metrics yet.")
        min_date = self._clickhouse.fetch_min_metric_date()
        start_date, end_date = self._resolve_window(filters, min_date, max_date)
        previous_end = start_date - timedelta(days=1)
        previous_start = previous_end - timedelta(days=(end_date - start_date).days)
        option_filters = self._mapping_option_filters(filters, start_date, end_date)
        needs_expanded_option_queries = self._needs_expanded_option_queries(filters)

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                "current_daily": pool.submit(self._clickhouse.fetch_daily_rows, start_date, end_date, filters),
                "current_issue": pool.submit(self._clickhouse.fetch_issue_rows, start_date, end_date, filters),
                "current_repeat": pool.submit(self._clickhouse.fetch_repeat_rows, start_date, end_date, filters),
                "bot_rows": pool.submit(self._clickhouse.fetch_bot_rows, start_date, end_date, filters),
                "pipeline_rows": pool.submit(self._clickhouse.fetch_pipeline_rows),
            }
            if needs_expanded_option_queries:
                futures["option_daily"] = pool.submit(self._clickhouse.fetch_daily_rows, start_date, end_date, option_filters)
                futures["option_issue"] = pool.submit(self._clickhouse.fetch_issue_rows, start_date, end_date, option_filters)
            if previous_end >= previous_start:
                futures["previous_daily"] = pool.submit(self._clickhouse.fetch_daily_rows, previous_start, previous_end, filters)
                futures["previous_issue"] = pool.submit(self._clickhouse.fetch_issue_rows, previous_start, previous_end, filters)
                futures["previous_repeat"] = pool.submit(self._clickhouse.fetch_repeat_rows, previous_start, previous_end, filters)
            current_daily_rows = futures["current_daily"].result()
            current_issue_rows = futures["current_issue"].result()
            current_repeat_rows = futures["current_repeat"].result()
            bot_rows = futures["bot_rows"].result()
            pipeline_rows = futures["pipeline_rows"].result()
            option_daily_rows = futures["option_daily"].result() if "option_daily" in futures else current_daily_rows
            option_issue_rows = futures["option_issue"].result() if "option_issue" in futures else current_issue_rows
            previous_daily_rows = futures["previous_daily"].result() if "previous_daily" in futures else []
            previous_issue_rows = futures["previous_issue"].result() if "previous_issue" in futures else []
            previous_repeat_rows = futures["previous_repeat"].result() if "previous_repeat" in futures else []
        current_daily_rows = self._mapped_daily_rows(current_daily_rows, filters)
        previous_daily_rows = self._mapped_daily_rows(previous_daily_rows, filters)
        current_issue_rows = self._mapped_issue_rows(current_issue_rows, filters)
        previous_issue_rows = self._mapped_issue_rows(previous_issue_rows, filters)
        bot_rows = self._mapped_bot_rows(bot_rows, filters)
        option_daily_rows = self._mapped_daily_rows(option_daily_rows, option_filters, apply_mapping_filters=False)
        option_issue_rows = self._mapped_issue_rows(option_issue_rows, option_filters, apply_mapping_filters=False)

        issues = self._agg_issues(current_issue_rows, previous_issue_rows)
        repeat_analysis = self._agg_repeat_analysis(
            current_repeat_rows,
            previous_repeat_rows,
            current_daily_rows,
            previous_daily_rows,
            current_issue_rows,
            previous_issue_rows,
        )
        category_health = self._agg_category_health(current_daily_rows, previous_daily_rows)
        product_health = self._agg_product_health(current_daily_rows, previous_daily_rows)
        service_ops = self._agg_service_ops(current_daily_rows)
        kpis = self._agg_kpis(current_daily_rows, previous_daily_rows)
        filter_options = self._agg_filter_options(option_daily_rows, self._agg_issues(option_issue_rows, []), min_date, max_date)
        pipeline_health = self._agg_pipeline_health(pipeline_rows)
        freshness = self._build_freshness(max_date)

        return {
            "meta": {
                "source_mode": "clickhouse",
                "clickhouse_mode": True,
                "title": "Qubo Support Executive Board",
                "subtitle": "Executive view of ticket volume, installation tickets, issue concentration, and bot outcomes.",
                "window_start": start_date.isoformat(),
                "window_end": end_date.isoformat(),
                "freshness": freshness,
            },
            "filters": asdict(filters),
            "filter_options": filter_options,
            "kpis": kpis,
            "timeline": self._agg_timeline(current_daily_rows),
            "repeat_analysis": repeat_analysis,
            "category_health": category_health,
            "product_health": product_health,
            "issue_views": self._agg_issue_views(issues),
            "rising_signals": self._agg_rising_signals(current_issue_rows),
            "service_ops": service_ops,
            "bot_summary": self._agg_bot_summary(bot_rows, issues),
            "pipeline_health": pipeline_health,
        }

    def get_mapping_studio(self, filters: DashboardFilters) -> dict[str, Any]:
        cached = self._cache_get("mapping_studio", filters)
        if cached is not None:
            return cached
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            max_date = self._clickhouse.fetch_max_metric_date()
            min_date = self._clickhouse.fetch_min_metric_date()
            if not max_date or not min_date:
                return {
                    "mapping_studio": {
                        "product_rows": [],
                        "fc2_rows": [],
                        "category_options": [],
                        "efc_options": [],
                        "active_overrides": {"products": 0, "efcs": 0},
                    }
                }
            start_date, end_date = self._resolve_window(filters, min_date, max_date)
            option_filters = replace(
                filters,
                products=[],
                efcs=[],
                departments=[],
                channels=[],
                bot_actions=[],
                include_fc1=[],
                exclude_fc1=[],
                include_fc2=[],
                exclude_fc2=[],
                include_bot_action=[],
                exclude_bot_action=[],
            )
            option_daily_rows = self._mapped_daily_rows(
                self._clickhouse.fetch_daily_rows(start_date, end_date, option_filters),
                option_filters,
                apply_mapping_filters=False,
            )
            option_issue_rows = self._mapped_issue_rows(
                self._clickhouse.fetch_issue_rows(start_date, end_date, option_filters),
                option_filters,
                apply_mapping_filters=False,
            )
            payload = {"mapping_studio": self._build_mapping_studio(option_daily_rows, option_issue_rows, filters)}
            self._cache_set("mapping_studio", filters, payload)
            return payload
        payload = {
            "mapping_studio": {
                "product_rows": [],
                "fc2_rows": [],
                "category_options": [],
                "efc_options": [],
                "active_overrides": {"products": 0, "efcs": 0},
            }
        }
        self._cache_set("mapping_studio", filters, payload)
        return payload

    def _cache_key(self, namespace: str, filters: DashboardFilters, extra: dict[str, Any] | None = None) -> str:
        return json.dumps(
            {
                "ns": namespace,
                "filters": asdict(filters),
                "extra": extra or {},
            },
            sort_keys=True,
            default=str,
        )

    def _cache_get(self, namespace: str, filters: DashboardFilters, extra: dict[str, Any] | None = None) -> Any | None:
        key = self._cache_key(namespace, filters, extra)
        cached = self._cache.get(key)
        if not cached:
            return None
        cached_at, value = cached
        if time.time() - cached_at > self._cache_ttl_seconds:
            self._cache.pop(key, None)
            return None
        return deepcopy(value)

    def _cache_set(self, namespace: str, filters: DashboardFilters, value: Any, extra: dict[str, Any] | None = None) -> None:
        key = self._cache_key(namespace, filters, extra)
        self._cache[key] = (time.time(), deepcopy(value))

    def _find_issue(self, filters: DashboardFilters, issue_id: str) -> dict[str, Any] | None:
        dashboard = self.build_dashboard(filters)
        for items in dashboard.get("issue_views", {}).values():
            for item in items:
                if item.get("issue_id") == issue_id:
                    return item
        for key in ("best_issues", "leaky_issues"):
            for item in dashboard.get("bot_summary", {}).get(key, []):
                if item.get("issue_id") == issue_id:
                    return item
        return None

    def _agg_kpis(self, current: list[dict[str, Any]], previous: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
        current_counts = self._sum_counts(current)
        previous_counts = self._sum_counts(previous)
        return {
            "tickets": self._metric(current_counts["tickets"], previous_counts["tickets"]),
            "installation_tickets": self._metric(current_counts["installation_field_tickets"], previous_counts["installation_field_tickets"]),
            "bot_resolved": self._metric(current_counts["bot_resolved_tickets"], previous_counts["bot_resolved_tickets"]),
            "repeat_tickets": self._metric(current_counts["repeat_tickets"], previous_counts["repeat_tickets"]),
            "no_reopen_rate": self._metric_ratio(current_counts["fcr_tickets"], current_counts["tickets"], previous_counts["fcr_tickets"], previous_counts["tickets"]),
        }

    def _agg_timeline(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, int]] = defaultdict(lambda: {"tickets": 0, "installation_tickets": 0, "bot_resolved_tickets": 0, "repeat_tickets": 0})
        for row in rows:
            key = row["metric_date"].isoformat() if hasattr(row["metric_date"], "isoformat") else str(row["metric_date"])
            grouped[key]["tickets"] += int(row.get("tickets", 0) or 0)
            grouped[key]["installation_tickets"] += int(row.get("installation_field_tickets", 0) or 0)
            grouped[key]["bot_resolved_tickets"] += int(row.get("bot_resolved_tickets", 0) or 0)
            grouped[key]["repeat_tickets"] += int(row.get("repeat_tickets", 0) or 0)
        return [{"date": day, **metrics} for day, metrics in sorted(grouped.items())]

    def _agg_repeat_analysis(
        self,
        current_rows: list[dict[str, Any]],
        previous_rows: list[dict[str, Any]],
        current_daily_rows: list[dict[str, Any]],
        previous_daily_rows: list[dict[str, Any]],
        current_issue_rows: list[dict[str, Any]],
        previous_issue_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        current_returns = sum(int(row.get("repeat_returns", 0) or 0) for row in current_rows)
        previous_returns = sum(int(row.get("repeat_returns", 0) or 0) for row in previous_rows)
        current_ticket_total = sum(int(row.get("tickets", 0) or 0) for row in current_daily_rows)
        previous_ticket_total = sum(int(row.get("tickets", 0) or 0) for row in previous_daily_rows)
        current_customers = {
            (str(row.get("product_name") or ""), str(row.get("metric_date") or ""), str(row.get("return_fault_code_level_2") or ""))
            for row in current_rows
            if int(row.get("repeat_returns", 0) or 0) > 0
        }
        previous_customers = {
            (str(row.get("product_name") or ""), str(row.get("metric_date") or ""), str(row.get("return_fault_code_level_2") or ""))
            for row in previous_rows
            if int(row.get("repeat_returns", 0) or 0) > 0
        }
        overview = {
            "repeat_returns": self._metric(current_returns, previous_returns),
            "repeat_rate": self._metric_ratio(current_returns, current_ticket_total, previous_returns, previous_ticket_total),
            "repeat_customer_events": self._metric(len(current_customers), len(previous_customers)),
            "median_return_days": self._metric(self._median_days(current_rows), self._median_days(previous_rows)),
            "within_7d_share": self._metric_ratio(
                self._bucket_total(current_rows, {"0-7 days"}),
                current_returns,
                self._bucket_total(previous_rows, {"0-7 days"}),
                previous_returns,
            ),
            "within_30d_share": self._metric_ratio(
                self._bucket_total(current_rows, {"0-7 days", "8-15 days", "16-30 days"}),
                current_returns,
                self._bucket_total(previous_rows, {"0-7 days", "8-15 days", "16-30 days"}),
                previous_returns,
            ),
        }
        base_counts = {
            "product_name": self._repeat_base_lookup(current_daily_rows, "product_name"),
            "product_category": self._repeat_base_lookup(current_daily_rows, "product_category"),
            "return_executive_fault_code": self._repeat_base_lookup(current_issue_rows, "executive_fault_code"),
            "return_fault_code_level_2": self._repeat_base_lookup(current_issue_rows, "fault_code_level_2"),
        }
        previous_base_counts = {
            "product_name": self._repeat_base_lookup(previous_daily_rows, "product_name"),
            "product_category": self._repeat_base_lookup(previous_daily_rows, "product_category"),
            "return_executive_fault_code": self._repeat_base_lookup(previous_issue_rows, "executive_fault_code"),
            "return_fault_code_level_2": self._repeat_base_lookup(previous_issue_rows, "fault_code_level_2"),
        }
        return {
            "overview": overview,
            "aging": self._agg_repeat_aging(current_rows),
            "same_issue_mix": self._agg_repeat_same_issue_mix(current_rows),
            "trend": self._agg_repeat_trend(current_rows),
            "products": self._agg_repeat_dimension(current_rows, previous_rows, "product_name", "return_executive_fault_code", base_counts["product_name"], previous_base_counts["product_name"]),
            "categories": self._agg_repeat_dimension(current_rows, previous_rows, "product_category", "return_executive_fault_code", base_counts["product_category"], previous_base_counts["product_category"]),
            "efcs": self._agg_repeat_dimension(current_rows, previous_rows, "return_executive_fault_code", "return_fault_code_level_2", base_counts["return_executive_fault_code"], previous_base_counts["return_executive_fault_code"]),
            "fc2": self._agg_repeat_dimension(current_rows, previous_rows, "return_fault_code_level_2", "return_resolution", base_counts["return_fault_code_level_2"], previous_base_counts["return_fault_code_level_2"]),
            "resolution_fallout": self._agg_repeat_resolution_fallout(current_rows),
            "transitions": self._agg_repeat_transitions(current_rows),
        }

    def _agg_repeat_aging(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        order = ["0-2 days", "3-7 days", "8-15 days", "16-30 days", "31-60 days", "60+ days"]
        grouped: dict[str, int] = defaultdict(int)
        total = 0
        for row in rows:
            bucket = str(row.get("aging_bucket") or "90+ days")
            value = int(row.get("repeat_returns", 0) or 0)
            grouped[bucket] += value
            total += value
        return [
            {"label": bucket, "count": grouped.get(bucket, 0), "share": self._ratio(grouped.get(bucket, 0), total)}
            for bucket in order
            if grouped.get(bucket, 0) > 0
        ]

    def _agg_repeat_same_issue_mix(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        total = sum(int(row.get("repeat_returns", 0) or 0) for row in rows)
        same_fc2 = sum(int(row.get("repeat_returns", 0) or 0) for row in rows if int(row.get("same_fc2", 0) or 0) == 1)
        same_efc_only = sum(
            int(row.get("repeat_returns", 0) or 0)
            for row in rows
            if int(row.get("same_fc2", 0) or 0) == 0 and int(row.get("same_efc", 0) or 0) == 1
        )
        different = max(total - same_fc2 - same_efc_only, 0)
        return [
            {"label": "Same FC2", "count": same_fc2, "share": self._ratio(same_fc2, total)},
            {"label": "Same EFC, different FC2", "count": same_efc_only, "share": self._ratio(same_efc_only, total)},
            {"label": "Different issue", "count": different, "share": self._ratio(different, total)},
        ]

    def _agg_repeat_trend(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, int]] = defaultdict(lambda: {"repeat_returns": 0, "same_fc2_returns": 0})
        for row in rows:
            key = row["metric_date"].isoformat() if hasattr(row.get("metric_date"), "isoformat") else str(row.get("metric_date"))
            value = int(row.get("repeat_returns", 0) or 0)
            grouped[key]["repeat_returns"] += value
            if int(row.get("same_fc2", 0) or 0) == 1:
                grouped[key]["same_fc2_returns"] += value
        return [{"date": key, **value} for key, value in sorted(grouped.items())]

    def _agg_repeat_dimension(
        self,
        current_rows: list[dict[str, Any]],
        previous_rows: list[dict[str, Any]],
        dimension: str,
        secondary_dimension: str,
        base_counts: dict[str, int],
        previous_base_counts: dict[str, int],
    ) -> list[dict[str, Any]]:
        current = self._group_repeat_dimension(current_rows, dimension, secondary_dimension)
        previous = self._group_repeat_dimension(previous_rows, dimension, secondary_dimension)
        items: list[dict[str, Any]] = []
        for key, bucket in current.items():
            previous_bucket = previous.get(key, {})
            items.append(
                {
                    "label": key,
                    "repeat_returns": bucket["repeat_returns"],
                    "repeat_rate": self._ratio(bucket["repeat_returns"], base_counts.get(key, 0)),
                    "median_days": self._weighted_median_days(bucket["days"]),
                    "same_issue_share": self._ratio(bucket["same_fc2_returns"], bucket["repeat_returns"]),
                    "change_rate": self._metric(bucket["repeat_returns"], previous_bucket.get("repeat_returns", 0))["change"],
                    "repeat_rate_change": self._metric(
                        self._ratio(bucket["repeat_returns"], base_counts.get(key, 0)),
                        self._ratio(previous_bucket.get("repeat_returns", 0), previous_base_counts.get(key, 0)),
                    )["change"],
                    "top_secondary": max(bucket["secondary"].items(), key=lambda item: item[1])[0] if bucket["secondary"] else "Unknown",
                    "top_first_resolution": max(bucket["first_resolution"].items(), key=lambda item: item[1])[0] if bucket["first_resolution"] else "Unknown",
                }
            )
        return sorted(items, key=lambda item: item["repeat_returns"], reverse=True)[:12]

    def _group_repeat_dimension(self, rows: list[dict[str, Any]], dimension: str, secondary_dimension: str) -> dict[str, dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            label = str(row.get(dimension) or "Unknown")
            current = grouped.get(label)
            if current is None:
                current = {
                    "repeat_returns": 0,
                    "same_fc2_returns": 0,
                    "days": [],
                    "secondary": defaultdict(int),
                    "first_resolution": defaultdict(int),
                }
                grouped[label] = current
            value = int(row.get("repeat_returns", 0) or 0)
            current["repeat_returns"] += value
            current["same_fc2_returns"] += value if int(row.get("same_fc2", 0) or 0) == 1 else 0
            current["days"].append((int(row.get("days_to_return", 0) or 0), value))
            current["secondary"][str(row.get(secondary_dimension) or "Unknown")] += value
            current["first_resolution"][str(row.get("first_resolution") or "Unknown")] += value
        return grouped

    def _repeat_base_lookup(self, rows: list[dict[str, Any]], dimension: str) -> dict[str, int]:
        grouped: dict[str, int] = defaultdict(int)
        for row in rows:
            label = str(row.get(dimension) or "Unknown")
            grouped[label] += int(row.get("tickets", 0) or 0)
        return dict(grouped)

    def _agg_repeat_resolution_fallout(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            label = str(row.get("first_resolution") or "Unknown")
            current = grouped.get(label)
            if current is None:
                current = {"repeat_returns": 0, "days": [], "return_resolution": defaultdict(int), "return_efc": defaultdict(int)}
                grouped[label] = current
            value = int(row.get("repeat_returns", 0) or 0)
            current["repeat_returns"] += value
            current["days"].append((int(row.get("days_to_return", 0) or 0), value))
            current["return_resolution"][str(row.get("return_resolution") or "Unknown")] += value
            current["return_efc"][str(row.get("return_executive_fault_code") or "Unknown")] += value
        items = []
        for label, bucket in grouped.items():
            items.append(
                {
                    "label": label,
                    "repeat_returns": bucket["repeat_returns"],
                    "median_days": self._weighted_median_days(bucket["days"]),
                    "top_return_resolution": max(bucket["return_resolution"].items(), key=lambda item: item[1])[0] if bucket["return_resolution"] else "Unknown",
                    "top_return_efc": max(bucket["return_efc"].items(), key=lambda item: item[1])[0] if bucket["return_efc"] else "Unknown",
                }
            )
        return sorted(items, key=lambda item: item["repeat_returns"], reverse=True)[:10]

    def _agg_repeat_transitions(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            key = (
                str(row.get("first_executive_fault_code") or "Unknown"),
                str(row.get("return_executive_fault_code") or "Unknown"),
            )
            current = grouped.get(key)
            if current is None:
                current = {"repeat_returns": 0, "days": []}
                grouped[key] = current
            value = int(row.get("repeat_returns", 0) or 0)
            current["repeat_returns"] += value
            current["days"].append((int(row.get("days_to_return", 0) or 0), value))
        items = []
        for (first_efc, return_efc), bucket in grouped.items():
            items.append(
                {
                    "first_efc": first_efc,
                    "return_efc": return_efc,
                    "repeat_returns": bucket["repeat_returns"],
                    "median_days": self._weighted_median_days(bucket["days"]),
                }
            )
        return sorted(items, key=lambda item: item["repeat_returns"], reverse=True)[:12]

    def _filter_repeat_drilldown_rows(self, rows: list[dict[str, Any]], kind: str, label: str, secondary: str | None = None) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        for row in rows:
            if kind == "product" and str(row.get("product_name") or "") == label:
                filtered.append(row)
            elif kind == "efc" and str(row.get("return_executive_fault_code") or "") == label:
                filtered.append(row)
            elif kind == "resolution" and str(row.get("first_resolution") or "") == label:
                filtered.append(row)
            elif kind == "transition" and str(row.get("first_executive_fault_code") or "") == label and str(row.get("return_executive_fault_code") or "") == str(secondary or ""):
                filtered.append(row)
        return filtered

    def _agg_repeat_drilldown_overview(self, current_rows: list[dict[str, Any]], previous_rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
        current_returns = sum(int(row.get("repeat_returns", 0) or 0) for row in current_rows)
        previous_returns = sum(int(row.get("repeat_returns", 0) or 0) for row in previous_rows)
        current_customers = len({(str(row.get("customer_key") or ""), str(row.get("return_ticket_id") or "")) for row in current_rows})
        previous_customers = len({(str(row.get("customer_key") or ""), str(row.get("return_ticket_id") or "")) for row in previous_rows})
        return {
            "repeat_returns": self._metric(current_returns, previous_returns),
            "repeat_customer_events": self._metric(current_customers, previous_customers),
            "median_return_days": self._metric(self._median_days(current_rows), self._median_days(previous_rows)),
            "within_7d_share": self._metric_ratio(
                self._bucket_total(current_rows, {"0-2 days", "3-7 days"}),
                current_returns,
                self._bucket_total(previous_rows, {"0-2 days", "3-7 days"}),
                previous_returns,
            ),
        }

    def _agg_repeat_channel_transitions(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], int] = defaultdict(int)
        for row in rows:
            grouped[(str(row.get("first_channel") or "Unknown"), str(row.get("return_channel") or "Unknown"))] += int(row.get("repeat_returns", 0) or 0)
        return [
            {"first_channel": first_channel, "return_channel": return_channel, "repeat_returns": repeat_returns}
            for (first_channel, return_channel), repeat_returns in sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:12]
        ]

    def _bucket_total(self, rows: list[dict[str, Any]], accepted_buckets: set[str]) -> int:
        return sum(int(row.get("repeat_returns", 0) or 0) for row in rows if str(row.get("aging_bucket") or "") in accepted_buckets)

    def _median_days(self, rows: list[dict[str, Any]]) -> float:
        weighted = [(int(row.get("days_to_return", 0) or 0), int(row.get("repeat_returns", 0) or 0)) for row in rows]
        return self._weighted_median_days(weighted)

    def _weighted_median_days(self, weighted: list[tuple[int, int]]) -> float:
        if not weighted:
            return 0.0
        ordered = sorted((days, weight) for days, weight in weighted if weight > 0)
        total_weight = sum(weight for _, weight in ordered)
        if total_weight <= 0:
            return 0.0
        midpoint = total_weight / 2
        cumulative = 0
        for days, weight in ordered:
            cumulative += weight
            if cumulative >= midpoint:
                return float(days)
        return float(ordered[-1][0])

    def _agg_category_health(self, rows: list[dict[str, Any]], previous_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        previous_grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row.get("product_category") or "Other")].append(row)
        for row in previous_rows:
            previous_grouped[str(row.get("product_category") or "Other")].append(row)
        items = []
        for category, category_rows in grouped.items():
            counts = self._sum_counts(category_rows)
            previous_counts = self._sum_counts(previous_grouped.get(category, []))
            issue_rows = [row for row in category_rows if row.get("fault_code_level_2") not in {None, "", "Unclassified"}]
            top_issue = max(issue_rows, key=lambda row: int(row.get("tickets", 0) or 0), default=None)
            efc_totals: dict[str, int] = defaultdict(int)
            for row in category_rows:
                efc_totals[str(row.get("executive_fault_code") or "Others")] += int(row.get("tickets", 0) or 0)
            items.append(
                {
                    "product_category": category,
                    "tickets": counts["tickets"],
                    "repeat_rate": self._ratio(counts["repeat_tickets"], counts["tickets"]),
                    "bot_resolved_rate": self._ratio(counts["bot_resolved_tickets"], counts["tickets"]),
                    "change_rate": self._metric(counts["tickets"], previous_counts["tickets"])["change"],
                    "top_efc": max(efc_totals.items(), key=lambda item: item[1])[0] if efc_totals else "Others",
                    "top_issue_detail": top_issue.get("fault_code_level_2") if top_issue else "Unclassified",
                }
            )
        return sorted(items, key=lambda item: item["tickets"], reverse=True)

    def _agg_product_health(self, rows: list[dict[str, Any]], previous_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        previous_grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[(str(row.get("product_category") or "Other"), str(row.get("product_name") or "Other"))].append(row)
        for row in previous_rows:
            previous_grouped[(str(row.get("product_category") or "Other"), str(row.get("product_name") or "Other"))].append(row)
        items = []
        for (category, product_name), product_rows in grouped.items():
            counts = self._sum_counts(product_rows)
            previous_counts = self._sum_counts(previous_grouped.get((category, product_name), []))
            efc_totals: dict[str, int] = defaultdict(int)
            for row in product_rows:
                efc_totals[str(row.get("executive_fault_code") or "Others")] += int(row.get("tickets", 0) or 0)
            top_issue = max(
                [row for row in product_rows if row.get("fault_code_level_2") not in {None, "", "Unclassified"}],
                key=lambda row: int(row.get("tickets", 0) or 0),
                default=None,
            )
            items.append(
                {
                    "product_category": category,
                    "product_name": product_name,
                    "tickets": counts["tickets"],
                    "repeat_rate": self._ratio(counts["repeat_tickets"], counts["tickets"]),
                    "bot_resolved_rate": self._ratio(counts["bot_resolved_tickets"], counts["tickets"]),
                    "change_rate": self._metric(counts["tickets"], previous_counts["tickets"])["change"],
                    "top_efc": max(efc_totals.items(), key=lambda item: item[1])[0] if efc_totals else "Others",
                    "top_issue_detail": top_issue.get("fault_code_level_2") if top_issue else "Unclassified",
                }
            )
        return sorted(items, key=lambda item: item["tickets"], reverse=True)

    def _agg_issues(self, current_rows: list[dict[str, Any]], previous_rows: list[dict[str, Any]]) -> list[AggregateIssue]:
        grouped_current = self._group_issue_rows(current_rows)
        grouped_previous = self._group_issue_rows(previous_rows)
        items: list[AggregateIssue] = []
        for key in set(grouped_current) | set(grouped_previous):
            current = grouped_current.get(key, {})
            previous = grouped_previous.get(key, {})
            tickets = int(current.get("tickets", 0) or 0)
            previous_tickets = int(previous.get("tickets", 0) or 0)
            if tickets <= 0 and previous_tickets <= 0:
                continue
            items.append(
                AggregateIssue(
                    product_category=key[0],
                    product_name=key[1],
                    executive_fault_code=key[2],
                    fault_code=key[3],
                    fault_code_level_1=key[4],
                    fault_code_level_2=key[5],
                    volume=tickets,
                    previous_volume=previous_tickets,
                    installation_rate=self._ratio(int(current.get("installation_field_tickets", 0) or 0), tickets),
                    repeat_rate=self._ratio(int(current.get("repeat_tickets", 0) or 0), tickets),
                    bot_resolved_rate=self._ratio(int(current.get("bot_resolved_tickets", 0) or 0), tickets),
                    bot_transfer_rate=self._ratio(int(current.get("bot_transferred_tickets", 0) or 0), tickets),
                    blank_chat_rate=self._ratio(int(current.get("blank_chat_tickets", 0) or 0), tickets),
                    fcr_rate=self._ratio(int(current.get("fcr_tickets", 0) or 0), tickets),
                    logistics_rate=self._ratio(int(current.get("logistics_tickets", 0) or 0), tickets),
                    top_symptom=str(current.get("top_symptom") or "Unknown"),
                    top_defect=str(current.get("top_defect") or "Unknown"),
                    top_repair=str(current.get("top_repair") or "Unknown"),
                )
            )
        return sorted(items, key=lambda item: (item.volume, item.installation_rate, item.bot_transfer_rate), reverse=True)

    def _agg_issue_views(self, issues: list[AggregateIssue]) -> dict[str, list[dict[str, Any]]]:
        return {
            "highest_volume": [self._issue_payload(issue) for issue in issues[:10]],
            "repeat_heavy": [self._issue_payload(issue) for issue in sorted(issues, key=lambda item: (item.volume * item.repeat_rate, item.volume), reverse=True)[:10]],
            "bot_leakage": [self._issue_payload(issue) for issue in sorted(issues, key=lambda item: (item.volume * item.bot_transfer_rate, item.volume), reverse=True)[:10]],
        }

    def _agg_rising_signals(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        weekly: dict[tuple[str, str, str, str], dict[date, int]] = defaultdict(lambda: defaultdict(int))
        meta: dict[tuple[str, str, str, str], dict[str, str]] = {}
        excluded_category_labels = {"other", "others", "blank product", "blankproduct", "-", ""}
        excluded_exact_labels = {"others", "blank", "unclassified", "blank chat"}
        excluded_keywords = (
            "sales",
            "marketing",
            "logistics",
            "order",
            "fulfilment",
            "fulfillment",
            "delivery",
            "subscription",
            "billing",
            "monetisation",
            "monetization",
            "promotional",
            "promotion",
            "pre-purchase",
            "pre purchase",
            "enquiry",
            "inquiry",
        )
        for row in rows:
            product_category = str(row.get("product_category") or "Other")
            product_name = str(row.get("product_name") or "Other")
            efc = str(row.get("executive_fault_code") or "Others")
            fc1 = str(row.get("fault_code_level_1") or "Unclassified")
            fc2 = str(row.get("fault_code_level_2") or "Unclassified")
            normalized_category = product_category.strip().lower()
            normalized_product = product_name.strip().lower()
            normalized_efc = efc.strip().lower()
            normalized_fc1 = fc1.strip().lower()
            normalized_fc2 = fc2.strip().lower()
            if normalized_category in excluded_category_labels:
                continue
            if normalized_product in {"blank product", "blankproduct", "-", ""}:
                continue
            if normalized_efc in excluded_exact_labels:
                continue
            if normalized_fc2 in excluded_exact_labels:
                continue
            if any(keyword in normalized_efc for keyword in excluded_keywords):
                continue
            if any(keyword in normalized_fc1 for keyword in excluded_keywords):
                continue
            if any(keyword in normalized_fc2 for keyword in excluded_keywords):
                continue
            if "instal" in normalized_fc1 or "instal" in normalized_fc2:
                continue
            if str(row.get("normalized_bot_action") or "").strip().lower() == "blank chat":
                continue
            metric_date = row.get("metric_date")
            if not isinstance(metric_date, date):
                continue
            week_start = metric_date - timedelta(days=metric_date.weekday())
            key = (product_category, product_name, efc, fc2)
            weekly[key][week_start] += int(row.get("tickets", 0) or 0)
            meta[key] = {
                "product_category": product_category,
                "product_name": product_name,
                "executive_fault_code": efc,
                "fault_code_level_1": fc1,
                "fault_code_level_2": fc2,
            }

        signals: list[dict[str, Any]] = []
        for key, counts in weekly.items():
            ordered = sorted(counts.items(), key=lambda item: item[0])
            if len(ordered) < 2:
                continue
            latest_week, latest_value = ordered[-1]
            previous_value = ordered[-2][1]
            if latest_value <= 0 or previous_value <= 0:
                continue
            delta = (latest_value - previous_value) / previous_value
            streak = len(ordered) >= 3 and ordered[-3][1] < ordered[-2][1] < ordered[-1][1]
            if latest_value < 20:
                continue
            if not (delta >= 0.35 or streak):
                continue
            payload = {
                **meta[key],
                "issue_id": "|".join([key[0], key[1], key[2], key[3]]),
                "volume": latest_value,
                "delta_rate": delta,
                "week_start": latest_week.isoformat(),
                "streak": streak,
            }
            signals.append(payload)
        return sorted(signals, key=lambda item: (item["delta_rate"], item["volume"]), reverse=True)[:6]

    def _agg_service_ops(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "category_mix": self._mix(rows, "product_category"),
            "department_mix": self._mix(rows, "department_name"),
            "channel_mix": self._mix(rows, "channel"),
            "bot_action_mix": self._mix(rows, "normalized_bot_action", exclude_labels={"No bot action"}),
            "installation_mix": self._installation_mix(rows),
        }

    def _agg_bot_summary(self, rows: list[dict[str, Any]], issues: list[AggregateIssue]) -> dict[str, Any]:
        by_product = []
        total = {
            "chat_tickets": 0,
            "bot_resolved_tickets": 0,
            "bot_transferred_tickets": 0,
            "blank_chat_tickets": 0,
            "blank_chat_returned_7d": 0,
            "blank_chat_resolved_7d": 0,
            "blank_chat_transferred_7d": 0,
            "blank_chat_blank_again_7d": 0,
        }
        for row in rows:
            item = {
                "product_category": row.get("product_category") or "Other",
                "product_name": row.get("product_name") or row.get("product_family") or "Other",
                "chat_tickets": int(row.get("chat_tickets", 0) or 0),
                "bot_resolved_tickets": int(row.get("bot_resolved_tickets", 0) or 0),
                "bot_transferred_tickets": int(row.get("bot_transferred_tickets", 0) or 0),
                "blank_chat_tickets": int(row.get("blank_chat_tickets", 0) or 0),
                "blank_chat_returned_7d": int(row.get("blank_chat_returned_7d", 0) or 0),
                "blank_chat_resolved_7d": int(row.get("blank_chat_resolved_7d", 0) or 0),
                "blank_chat_transferred_7d": int(row.get("blank_chat_transferred_7d", 0) or 0),
                "blank_chat_blank_again_7d": int(row.get("blank_chat_blank_again_7d", 0) or 0),
            }
            item["bot_resolved_rate"] = self._ratio(item["bot_resolved_tickets"], item["chat_tickets"])
            item["bot_transferred_rate"] = self._ratio(item["bot_transferred_tickets"], item["chat_tickets"])
            item["blank_chat_rate"] = self._ratio(item["blank_chat_tickets"], item["chat_tickets"])
            item["blank_chat_return_rate"] = self._ratio(item["blank_chat_returned_7d"], item["blank_chat_tickets"])
            by_product.append(item)
            for key in total:
                total[key] += item[key]
        overview = {
            **total,
            "bot_resolved_rate": self._ratio(total["bot_resolved_tickets"], total["chat_tickets"]),
            "bot_transferred_rate": self._ratio(total["bot_transferred_tickets"], total["chat_tickets"]),
            "blank_chat_rate": self._ratio(total["blank_chat_tickets"], total["chat_tickets"]),
            "blank_chat_return_rate": self._ratio(total["blank_chat_returned_7d"], total["blank_chat_tickets"]),
            "blank_chat_recovery_rate": self._ratio(total["blank_chat_resolved_7d"], total["blank_chat_tickets"]),
        }
        return {
            "overview": overview,
            "by_product": sorted(by_product, key=lambda item: item["chat_tickets"], reverse=True),
            "best_issues": [self._issue_payload(issue) for issue in sorted(issues, key=lambda item: (item.bot_resolved_rate, item.volume), reverse=True)[:6]],
            "leaky_issues": [self._issue_payload(issue) for issue in sorted(issues, key=lambda item: (item.bot_transfer_rate, item.volume), reverse=True)[:6]],
        }

    def _agg_data_quality(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        row = rows[0] if rows else {}
        total_tickets = int(row.get("total_tickets", 0) or 0)
        usable = int(row.get("usable_issue_tickets", 0) or 0)
        actionable = int(row.get("actionable_issue_tickets", 0) or 0)
        return {
            "as_of_date": row.get("as_of_date").isoformat() if hasattr(row.get("as_of_date"), "isoformat") else str(row.get("as_of_date") or ""),
            "total_tickets": total_tickets,
            "usable_issue_tickets": usable,
            "actionable_issue_tickets": actionable,
            "blank_fault_code_tickets": int(row.get("blank_fault_code_tickets", 0) or 0),
            "blank_fault_code_l1_tickets": int(row.get("blank_fault_code_l1_tickets", 0) or 0),
            "blank_fault_code_l2_tickets": int(row.get("blank_fault_code_l2_tickets", 0) or 0),
            "other_product_tickets": int(row.get("other_product_tickets", 0) or 0),
            "hero_internal_tickets": int(row.get("hero_internal_tickets", 0) or 0),
            "dropped_in_bot_tickets": int(row.get("dropped_in_bot_tickets", 0) or 0),
            "missing_issue_outside_bot_tickets": int(row.get("missing_issue_outside_bot_tickets", 0) or 0),
            "dirty_channel_tickets": int(row.get("dirty_channel_tickets", 0) or 0),
            "email_department_reassigned_tickets": int(row.get("email_department_reassigned_tickets", 0) or 0),
            "usable_issue_rate": self._ratio(usable, total_tickets),
            "actionable_issue_rate": self._ratio(actionable, total_tickets),
        }

    def _agg_pipeline_health(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        latest = rows[0] if rows else {}
        return {
            "status": latest.get("status") or "Unknown",
            "last_run_at": self._format_timestamp(latest.get("run_finished_at")) or "Unknown",
            "duration_minutes": int(latest.get("duration_minutes", 0) or 0),
            "rows_fetched": int(latest.get("source_rows", 0) or 0),
            "rows_inserted": int(latest.get("rows_inserted", 0) or 0),
            "recent_runs": [
                {
                    "started_at": self._format_timestamp(row.get("run_started_at")),
                    "finished_at": self._format_timestamp(row.get("run_finished_at")),
                    "duration_minutes": int(row.get("duration_minutes", 0) or 0),
                    "status": row.get("status") or "Unknown",
                    "rows_fetched": int(row.get("source_rows", 0) or 0),
                    "rows_inserted": int(row.get("rows_inserted", 0) or 0),
                    "message": row.get("message") or "",
                }
                for row in rows
            ],
        }

    def _format_timestamp(self, value: Any) -> str:
        if not value:
            return ""
        if not hasattr(value, "tzinfo"):
            return str(value)
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(self._display_tz).isoformat()

    def _agg_filter_options(self, daily_rows: list[dict[str, Any]], issues: list[AggregateIssue], min_date: date | None, max_date: date | None) -> dict[str, Any]:
        def ordered_counts(rows: list[dict[str, Any]], key: str) -> list[str]:
            grouped: dict[str, int] = defaultdict(int)
            for row in rows:
                label = str(row.get(key) or "").strip()
                if not label:
                    continue
                grouped[label] += int(row.get("tickets", 0) or 0)
            return [label for label, _ in sorted(grouped.items(), key=lambda item: item[1], reverse=True)]

        efc_counts: dict[str, int] = defaultdict(int)
        detail_counts: dict[str, int] = defaultdict(int)
        products_by_category: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        bot_action_counts: dict[str, int] = defaultdict(int)
        for issue in issues:
            efc_counts[issue.executive_fault_code] += issue.volume
            detail_counts[issue.fault_code_level_2] += issue.volume
        for row in daily_rows:
            category = str(row.get("product_category") or "Other")
            product_name = str(row.get("product_name") or row.get("product_family") or "Other")
            tickets = int(row.get("tickets", 0) or 0)
            products_by_category[category][product_name] += tickets
            bot_action_counts[str(row.get("normalized_bot_action") or "No bot action")] += tickets
        return {
            "date_bounds": {
                "min": min_date.isoformat() if min_date else "",
                "max": max_date.isoformat() if max_date else "",
            },
            "categories": ordered_counts(daily_rows, "product_category"),
            "products": ordered_counts(daily_rows, "product_name"),
            "products_by_category": {
                category: [label for label, _ in sorted(items.items(), key=lambda item: item[1], reverse=True)]
                for category, items in sorted(products_by_category.items())
            },
            "departments": ordered_counts(daily_rows, "department_name"),
            "channels": ordered_counts(daily_rows, "channel"),
            "efcs": [label for label, _ in sorted(efc_counts.items(), key=lambda item: item[1], reverse=True)],
            "issue_details": [label for label, _ in sorted(detail_counts.items(), key=lambda item: item[1], reverse=True)],
            "statuses": ordered_counts(daily_rows, "status"),
            "fc1": ordered_counts(daily_rows, "fault_code_level_1"),
            "fc2": ordered_counts(daily_rows, "fault_code_level_2"),
            "bot_actions": [label for label, _ in sorted(bot_action_counts.items(), key=lambda item: item[1], reverse=True)],
        }

    def _mapping_option_filters(self, filters: DashboardFilters, start_date: date, end_date: date) -> DashboardFilters:
        return DashboardFilters(
            date_start=start_date.isoformat(),
            date_end=end_date.isoformat(),
            exclude_installation=filters.exclude_installation,
            exclude_blank_chat=filters.exclude_blank_chat,
            exclude_unclassified_blank=filters.exclude_unclassified_blank,
            departments=list(filters.departments),
            channels=list(filters.channels),
            statuses=list(filters.statuses),
            bot_actions=list(filters.bot_actions),
            include_fc1=list(filters.include_fc1),
            exclude_fc1=list(filters.exclude_fc1),
            include_fc2=list(filters.include_fc2),
            exclude_fc2=list(filters.exclude_fc2),
            include_bot_action=list(filters.include_bot_action),
            exclude_bot_action=list(filters.exclude_bot_action),
            product_category_overrides=dict(filters.product_category_overrides),
            efc_overrides=dict(filters.efc_overrides),
        )

    def _needs_expanded_option_queries(self, filters: DashboardFilters) -> bool:
        return any(
            (
                filters.categories,
                filters.products,
                filters.efcs,
            )
        )

    def _mapped_daily_rows(self, rows: list[dict[str, Any]], filters: DashboardFilters, apply_mapping_filters: bool = True) -> list[dict[str, Any]]:
        remapped = [self._remap_row(row, filters) for row in rows]
        if apply_mapping_filters:
            remapped = [row for row in remapped if self._matches_mapping_filters(row, filters)]
        key_fields = (
            "metric_date", "product_category", "product_name", "product_family", "executive_fault_code",
            "fault_code", "fault_code_level_1", "fault_code_level_2", "department_name", "channel",
            "normalized_bot_action", "bot_outcome", "status",
        )
        sum_fields = (
            "tickets", "field_visit_tickets", "repair_field_tickets", "installation_field_tickets", "bot_resolved_tickets",
            "bot_transferred_tickets", "blank_chat_tickets", "fcr_tickets", "repeat_tickets", "logistics_tickets",
            "young_device_tickets", "usable_issue_tickets", "actionable_issue_tickets", "other_product_tickets",
            "hero_internal_tickets", "missing_issue_outside_bot_tickets", "dirty_channel_tickets",
            "email_department_reassigned_tickets", "total_handle_time_minutes", "handle_time_ticket_count",
        )
        return self._collapse_rows(remapped, key_fields, sum_fields)

    def _mapped_issue_rows(self, rows: list[dict[str, Any]], filters: DashboardFilters, apply_mapping_filters: bool = True) -> list[dict[str, Any]]:
        remapped = [self._remap_row(row, filters) for row in rows]
        if apply_mapping_filters:
            remapped = [row for row in remapped if self._matches_mapping_filters(row, filters)]
        key_fields = (
            "metric_date", "product_category", "product_name", "product_family", "executive_fault_code",
            "fault_code", "fault_code_level_1", "fault_code_level_2", "department_name", "channel", "normalized_bot_action",
        )
        sum_fields = (
            "tickets", "repair_field_tickets", "installation_field_tickets", "repeat_tickets", "bot_resolved_tickets",
            "bot_transferred_tickets", "blank_chat_tickets", "fcr_tickets", "logistics_tickets",
        )
        return self._collapse_rows(remapped, key_fields, sum_fields, text_fields=("top_symptom", "top_defect", "top_repair"))

    def _mapped_bot_rows(self, rows: list[dict[str, Any]], filters: DashboardFilters) -> list[dict[str, Any]]:
        remapped = [self._remap_row(row, filters) for row in rows]
        remapped = [row for row in remapped if self._matches_mapping_filters(row, filters)]
        return self._collapse_rows(
            remapped,
            ("product_category", "product_name", "product_family"),
            (
                "chat_tickets", "bot_resolved_tickets", "bot_transferred_tickets", "blank_chat_tickets",
                "blank_chat_returned_7d", "blank_chat_resolved_7d", "blank_chat_transferred_7d", "blank_chat_blank_again_7d",
            ),
        )

    def _remap_row(self, row: dict[str, Any], filters: DashboardFilters) -> dict[str, Any]:
        mapped = dict(row)
        product_name = str(mapped.get("product_name") or mapped.get("product") or mapped.get("product_family") or "Blank Product")
        product_family = str(mapped.get("product_family") or mapped.get("canonical_product") or "")
        mapped["product_name"] = product_name
        mapped["product_category"] = map_product_category(product_name, product_family, filters.product_category_overrides)
        mapped["executive_fault_code"] = map_executive_fault_code(
            mapped.get("fault_code_level_1") or mapped.get("normalized_fault_code_l1"),
            mapped.get("fault_code_level_2") or mapped.get("normalized_fault_code_l2"),
            filters.efc_overrides,
        )
        return mapped

    def _matches_mapping_filters(self, row: dict[str, Any], filters: DashboardFilters) -> bool:
        if filters.categories and str(row.get("product_category") or "Other") not in filters.categories:
            return False
        if filters.efcs and str(row.get("executive_fault_code") or "Others") not in filters.efcs:
            return False
        if filters.exclude_unclassified_blank:
            product_name = str(row.get("product_name") or "").strip().lower()
            product_category = str(row.get("product_category") or "").strip().lower()
            efc = str(row.get("executive_fault_code") or "").strip().lower()
            if product_name in {"blank product", "blankproduct", "-"}:
                return False
            if product_category in {"blank product", "blankproduct", "-"}:
                return False
            if efc in {"blank", "unclassified"}:
                return False
        return True

    def _collapse_rows(
        self,
        rows: list[dict[str, Any]],
        key_fields: tuple[str, ...],
        sum_fields: tuple[str, ...],
        text_fields: tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            key = tuple(row.get(field) for field in key_fields)
            current = grouped.get(key)
            if current is None:
                current = {field: row.get(field) for field in key_fields}
                for field in sum_fields:
                    current[field] = 0
                for field in text_fields:
                    current[field] = "Unknown"
                grouped[key] = current
            for field in sum_fields:
                current[field] += int(row.get(field, 0) or 0)
            for field in text_fields:
                value = str(row.get(field) or "").strip()
                if value and current[field] == "Unknown":
                    current[field] = value
        return list(grouped.values())

    def _build_mapping_studio(self, daily_rows: list[dict[str, Any]], issue_rows: list[dict[str, Any]], filters: DashboardFilters) -> dict[str, Any]:
        product_counts: dict[str, int] = defaultdict(int)
        for row in daily_rows:
            product_counts[str(row.get("product_name") or "Blank Product")] += int(row.get("tickets", 0) or 0)
        fc2_counts: dict[tuple[str, str], int] = defaultdict(int)
        for row in issue_rows:
            fc2_counts[(str(row.get("fault_code_level_2") or "Unclassified"), str(row.get("fault_code_level_1") or "Unclassified"))] += int(row.get("tickets", 0) or 0)

        product_rows = []
        for product_name, tickets in sorted(product_counts.items(), key=lambda item: (-item[1], item[0])):
            base_category = map_product_category(product_name)
            effective_category = map_product_category(product_name, overrides=filters.product_category_overrides)
            product_rows.append({
                "product_name": product_name,
                "base_category": base_category,
                "effective_category": effective_category,
                "tickets": tickets,
                "overridden": base_category != effective_category,
            })

        fc2_rows = []
        for (fc2, fc1), tickets in sorted(fc2_counts.items(), key=lambda item: (-item[1], item[0][0])):
            base_efc = map_executive_fault_code(fc1, fc2)
            effective_efc = map_executive_fault_code(fc1, fc2, filters.efc_overrides)
            fc2_rows.append({
                "fault_code_level_2": fc2,
                "fault_code_level_1": fc1,
                "base_efc": base_efc,
                "effective_efc": effective_efc,
                "tickets": tickets,
                "overridden": base_efc != effective_efc,
            })

        category_options = sorted({row["effective_category"] for row in product_rows} | {row["base_category"] for row in product_rows} | set(load_mappings().product_to_category.values()))
        efc_options = sorted({row["effective_efc"] for row in fc2_rows} | {row["base_efc"] for row in fc2_rows} | set(load_mappings().fc2_to_efc.values()))
        return {
            "product_rows": product_rows,
            "fc2_rows": fc2_rows,
            "category_options": [item for item in category_options if item],
            "efc_options": [item for item in efc_options if item],
            "active_overrides": {
                "products": len(product_rows),
                "efcs": len(fc2_rows),
            },
        }

    def _remap_product_drilldown(self, drilldown: dict[str, Any], filters: DashboardFilters) -> dict[str, Any]:
        mapped = deepcopy(drilldown)
        issue_totals: dict[tuple[str, str], dict[str, Any]] = {}
        efc_totals: dict[str, int] = defaultdict(int)
        for row in mapped.get("issue_matrix", []):
            issue_detail = str(row.get("issue_detail") or row.get("label") or "")
            effective_efc = map_executive_fault_code(None, issue_detail, filters.efc_overrides)
            key = (effective_efc, issue_detail)
            current = issue_totals.get(key)
            if current is None:
                current = {
                    "executive_fault_code": effective_efc,
                    "issue_detail": issue_detail,
                    "tickets": 0,
                    "bot_resolved_tickets": 0,
                    "installation_tickets": 0,
                }
                issue_totals[key] = current
            current["tickets"] += int(row.get("tickets", 0) or 0)
            current["bot_resolved_tickets"] += int(row.get("bot_resolved_tickets", 0) or 0)
            current["installation_tickets"] += int(row.get("installation_tickets", 0) or 0)
            efc_totals[effective_efc] += int(row.get("tickets", 0) or 0)
        mapped["issue_matrix"] = sorted(issue_totals.values(), key=lambda item: item["tickets"], reverse=True)
        mapped["efcs"] = [
            {"label": label, "tickets": tickets}
            for label, tickets in sorted(efc_totals.items(), key=lambda item: item[1], reverse=True)
        ]
        return mapped

    def _remap_category_drilldown(self, drilldown: dict[str, Any], filters: DashboardFilters) -> dict[str, Any]:
        mapped = deepcopy(drilldown)
        issue_totals: dict[tuple[str, str], dict[str, Any]] = {}
        efc_totals: dict[str, int] = defaultdict(int)
        product_fault_totals: dict[tuple[str, str, str, str], int] = defaultdict(int)
        for row in mapped.get("issues", []):
            issue_detail = str(row.get("label") or row.get("fault_code_level_2") or "")
            original_efc = str(row.get("executive_fault_code") or "Others")
            effective_efc = original_efc if original_efc and original_efc != "Others" else map_executive_fault_code(None, issue_detail, filters.efc_overrides)
            key = (effective_efc, issue_detail)
            current = issue_totals.get(key)
            if current is None:
                current = {
                    "executive_fault_code": effective_efc,
                    "label": issue_detail,
                    "tickets": 0,
                    "bot_resolved_tickets": 0,
                }
                issue_totals[key] = current
            current["tickets"] += int(row.get("tickets", 0) or 0)
            current["bot_resolved_tickets"] += int(row.get("bot_resolved_tickets", 0) or 0)
            efc_totals[effective_efc] += int(row.get("tickets", 0) or 0)
        for row in mapped.get("product_fault_daily", []):
            issue_detail = str(row.get("fault_code_level_2") or "")
            effective_efc = map_executive_fault_code(None, issue_detail, filters.efc_overrides)
            key = (
                str(row.get("metric_date") or ""),
                str(row.get("product_name") or ""),
                effective_efc,
                str(row.get("fault_code_level_1") or "Unclassified"),
                issue_detail,
                str(row.get("resolution") or "Unknown"),
            )
            product_fault_totals[key] += int(row.get("tickets", 0) or 0)
            efc_totals[effective_efc] += 0
        mapped["issues"] = sorted(issue_totals.values(), key=lambda item: item["tickets"], reverse=True)
        mapped["efcs"] = [
            {"label": label, "tickets": tickets}
            for label, tickets in sorted(efc_totals.items(), key=lambda item: item[1], reverse=True)
        ]
        mapped["product_fault_daily"] = [
            {
                "metric_date": metric_date,
                "product_name": product_name,
                "executive_fault_code": executive_fault_code,
                "fault_code_level_1": fault_code_level_1,
                "fault_code_level_2": fault_code_level_2,
                "resolution": resolution,
                "tickets": tickets,
            }
            for (metric_date, product_name, executive_fault_code, fault_code_level_1, fault_code_level_2, resolution), tickets in sorted(product_fault_totals.items())
        ]
        return mapped

    def _sum_counts(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        keys = ("tickets", "field_visit_tickets", "repair_field_tickets", "installation_field_tickets", "bot_resolved_tickets", "bot_transferred_tickets", "blank_chat_tickets", "fcr_tickets", "repeat_tickets", "logistics_tickets")
        totals = {key: 0 for key in keys}
        for row in rows:
            for key in keys:
                totals[key] += int(row.get(key, 0) or 0)
        return totals

    def _group_issue_rows(self, rows: list[dict[str, Any]]) -> dict[tuple[str, str, str, str, str, str], dict[str, Any]]:
        grouped: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = defaultdict(lambda: {"tickets": 0, "repair_field_tickets": 0, "installation_field_tickets": 0, "repeat_tickets": 0, "bot_resolved_tickets": 0, "bot_transferred_tickets": 0, "blank_chat_tickets": 0, "fcr_tickets": 0, "logistics_tickets": 0, "top_symptom": "Unknown", "top_defect": "Unknown", "top_repair": "Unknown"})
        for row in rows:
            key = (
                str(row.get("product_category") or "Other"),
                str(row.get("product_name") or row.get("product_family") or "Other"),
                str(row.get("executive_fault_code") or "Others"),
                str(row.get("fault_code") or "Unclassified"),
                str(row.get("fault_code_level_1") or "Unclassified"),
                str(row.get("fault_code_level_2") or "Unclassified"),
            )
            current = grouped[key]
            for metric in ("tickets", "repair_field_tickets", "installation_field_tickets", "repeat_tickets", "bot_resolved_tickets", "bot_transferred_tickets", "blank_chat_tickets", "fcr_tickets", "logistics_tickets"):
                current[metric] += int(row.get(metric, 0) or 0)
            for text_key in ("top_symptom", "top_defect", "top_repair"):
                value = str(row.get(text_key) or "").strip()
                if value and current[text_key] == "Unknown":
                    current[text_key] = value
        return grouped

    def _mix(self, rows: list[dict[str, Any]], key: str, exclude_labels: set[str] | None = None) -> list[dict[str, Any]]:
        grouped: dict[str, int] = defaultdict(int)
        total = 0
        exclude_labels = exclude_labels or set()
        for row in rows:
            label = str(row.get(key) or "Unknown")
            if label in exclude_labels:
                continue
            value = int(row.get("tickets", 0) or 0)
            grouped[label] += value
            total += value
        return [{"label": label, "count": count, "share": self._ratio(count, total)} for label, count in sorted(grouped.items(), key=lambda item: item[1], reverse=True)]

    def _installation_mix(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        install = sum(int(row.get("installation_field_tickets", 0) or 0) for row in rows)
        non_install = max(sum(int(row.get("tickets", 0) or 0) for row in rows) - install, 0)
        total = install + non_install
        return [
            {"label": "Installation tickets", "count": install, "share": self._ratio(install, total)},
            {"label": "Non-installation tickets", "count": non_install, "share": self._ratio(non_install, total)},
        ]

    def _open_ticket_count(self, rows: list[dict[str, Any]]) -> int:
        total = 0
        for row in rows:
            status = str(row.get("status") or "").lower()
            if any(marker in status for marker in OPEN_STATUS_MARKERS):
                total += int(row.get("tickets", 0) or 0)
        return total

    def _open_rate(self, rows: list[dict[str, Any]]) -> float:
        total = sum(int(row.get("tickets", 0) or 0) for row in rows)
        open_total = 0
        for row in rows:
            status = str(row.get("status") or "").lower()
            if any(marker in status for marker in OPEN_STATUS_MARKERS):
                open_total += int(row.get("tickets", 0) or 0)
        return self._ratio(open_total, total)

    def _metric(self, current: float, previous: float) -> dict[str, float]:
        return {"value": current, "change": 0.0 if previous == 0 else (current - previous) / previous}

    def _metric_ratio(self, current_numerator: int, current_denominator: int, previous_numerator: int, previous_denominator: int) -> dict[str, float]:
        return self._metric(self._ratio(current_numerator, current_denominator), self._ratio(previous_numerator, previous_denominator))

    def _ratio(self, numerator: int, denominator: int) -> float:
        return numerator / denominator if denominator else 0.0

    def _previous_period(self, start_date: date, end_date: date) -> tuple[date, date]:
        previous_end = start_date - timedelta(days=1)
        previous_start = previous_end - timedelta(days=(end_date - start_date).days)
        return previous_start, previous_end

    def _issue_payload(self, issue: AggregateIssue) -> dict[str, Any]:
        return {
            "issue_id": issue.issue_id,
            "product_category": issue.product_category,
            "product_name": issue.product_name,
            "product_context": f"{issue.product_category} / {issue.product_name}",
            "executive_fault_code": issue.executive_fault_code,
            "fault_code": issue.fault_code,
            "fault_code_level_1": issue.fault_code_level_1,
            "fault_code_level_2": issue.fault_code_level_2,
            "volume": issue.volume,
            "previous_volume": issue.previous_volume,
            "delta_rate": issue.delta_rate,
            "installation_rate": issue.installation_rate,
            "repeat_rate": issue.repeat_rate,
            "bot_resolved_rate": issue.bot_resolved_rate,
            "bot_transfer_rate": issue.bot_transfer_rate,
            "blank_chat_rate": issue.blank_chat_rate,
            "fcr_rate": issue.fcr_rate,
            "logistics_rate": issue.logistics_rate,
            "top_symptom": issue.top_symptom,
            "top_defect": issue.top_defect,
            "top_repair": issue.top_repair,
            "insight": issue.insight,
        }

    def _format_clickhouse_ticket(self, item: dict[str, Any]) -> dict[str, Any]:
        created_at = item.get("created_at")
        return {
            "ticket_id": item.get("ticket_id"),
            "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
            "product_category": item.get("product_category") or "Other",
            "product_name": item.get("product_name") or item.get("product_family") or "Other",
            "product": item.get("product") or item.get("product_name") or item.get("product_family") or "Other",
            "department": item.get("department") or "Unknown",
            "channel": item.get("channel") or "Unknown",
            "executive_fault_code": item.get("executive_fault_code") or "Others",
            "fault_code": item.get("fault_code") or "Unclassified",
            "fault_code_level_1": item.get("fault_code_level_1") or "Unclassified",
            "fault_code_level_2": item.get("fault_code_level_2") or "Unclassified",
            "resolution": item.get("resolution") or "Unknown",
            "bot_action": item.get("bot_action") or "No bot action",
            "status": item.get("status") or "Unknown",
            "symptom": item.get("symptom") or "Unknown",
            "defect": item.get("defect") or "Unknown",
            "repair": item.get("repair") or "Unknown",
            "device_serial_number": item.get("device_serial_number") or "",
        }

    def _window_start(self, latest: date, preset: str) -> date:
        if preset == "14d":
            return latest - timedelta(days=13)
        if preset == "30d":
            return latest - timedelta(days=29)
        if preset == "history":
            return latest - timedelta(days=580)
        return latest - timedelta(days=59)

    def _build_seeded(self, filters: DashboardFilters, error: str) -> dict[str, Any]:
        return {
            "meta": {
                "source_mode": "sample",
                "clickhouse_mode": False,
                "title": "Qubo Support Executive Board",
                "subtitle": "No ClickHouse data is available right now.",
                "window_start": "",
                "window_end": "",
                "data_confidence_note": error,
                "freshness": {"source_max_date": "", "clickhouse_max_date": "", "status": "Unavailable"},
            },
            "filters": asdict(filters),
            "filter_options": {"date_bounds": {"min": "", "max": ""}, "categories": [], "products": [], "products_by_category": {}, "departments": [], "channels": [], "efcs": [], "issue_details": [], "statuses": [], "fc1": [], "fc2": [], "bot_actions": []},
            "kpis": {
                "tickets": {"value": 0, "change": 0.0},
                "installation_tickets": {"value": 0.0, "change": 0.0},
                "bot_resolved": {"value": 0.0, "change": 0.0},
                "repeat_tickets": {"value": 0.0, "change": 0.0},
                "no_reopen_rate": {"value": 0.0, "change": 0.0},
            },
              "timeline": [],
              "repeat_analysis": {"overview": {}, "aging": [], "same_issue_mix": [], "trend": [], "products": [], "categories": [], "efcs": [], "fc2": [], "resolution_fallout": [], "transitions": []},
              "category_health": [],
              "product_health": [],
              "issue_views": {"highest_volume": [], "installation_tickets": [], "repeat_heavy": [], "bot_leakage": []},
              "rising_signals": [],
              "service_ops": {"category_mix": [], "department_mix": [], "channel_mix": [], "bot_action_mix": [], "installation_mix": []},
            "bot_summary": {"overview": {}, "by_product": [], "best_issues": [], "leaky_issues": []},
            "pipeline_health": {"status": "Unavailable", "last_run_at": "Unknown", "duration_minutes": 0, "rows_fetched": 0, "rows_inserted": 0, "recent_runs": []},
            "mapping_studio": {"product_rows": [], "fc2_rows": [], "category_options": [], "efc_options": [], "active_overrides": {"products": 0, "efcs": 0}},
        }

    def _build_freshness(self, clickhouse_max_date: date | None) -> dict[str, str]:
        now = time.time()
        if self._freshness_cache and (now - self._freshness_cache[0]) <= self._freshness_ttl_seconds:
            cached = dict(self._freshness_cache[1])
            cached["clickhouse_max_date"] = clickhouse_max_date.isoformat() if clickhouse_max_date else ""
            source_max_date = cached.get("source_max_date", "")
            clickhouse_date = cached.get("clickhouse_max_date", "")
            cached["status"] = "In sync" if source_max_date and clickhouse_date and source_max_date == clickhouse_date else "Source ahead" if source_max_date and clickhouse_date and source_max_date > clickhouse_date else "Unavailable"
            return cached
        source_max = None
        try:
            source_max = self._repository.fetch_source_max_created_time()
        except Exception:
            source_max = None
        source_max_date = source_max.date().isoformat() if source_max else ""
        clickhouse_date = clickhouse_max_date.isoformat() if clickhouse_max_date else ""
        status = "In sync" if source_max_date and clickhouse_date and source_max_date == clickhouse_date else "Source ahead" if source_max_date and clickhouse_date and source_max_date > clickhouse_date else "Unavailable"
        payload = {
            "source_max_date": source_max_date,
            "clickhouse_max_date": clickhouse_date,
            "status": status,
        }
        self._freshness_cache = (now, dict(payload))
        return payload

    def _resolve_window(self, filters: DashboardFilters, min_date: date | None, max_date: date | None) -> tuple[date, date]:
        if not max_date:
            today = date.today()
            return today - timedelta(days=59), today
        if not min_date:
            min_date = max_date
        start_date = self._parse_date(filters.date_start) or max(min_date, max_date - timedelta(days=59))
        end_date = self._parse_date(filters.date_end) or max_date
        if start_date < min_date:
            start_date = min_date
        if end_date > max_date:
            end_date = max_date
        if start_date > end_date:
            start_date = end_date
        return start_date, end_date

    def _parse_date(self, value: str | None) -> date | None:
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
