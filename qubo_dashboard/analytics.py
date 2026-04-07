from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

from .clickhouse_analytics import ClickHouseAnalyticsRepository
from .config import settings
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

    def build_dashboard(self, filters: DashboardFilters) -> dict[str, Any]:
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            try:
                return self._build_from_clickhouse(filters)
            except Exception as exc:
                return self._build_seeded(filters, str(exc))
        return self._build_seeded(filters, "ClickHouse is not configured.")

    def get_issue_tickets(self, filters: DashboardFilters, issue_id: str) -> dict[str, Any]:
        issue = self._find_issue(filters, issue_id)
        if not issue:
            return {"issue": None, "tickets": []}
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            tickets = self._clickhouse.fetch_issue_tickets(filters, issue_id, limit=24)
            return {"issue": issue, "tickets": [self._format_clickhouse_ticket(ticket) for ticket in tickets]}
        return {"issue": issue, "tickets": []}

    def get_product_drilldown(self, filters: DashboardFilters, category: str, product_name: str) -> dict[str, Any]:
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            return {
                "meta": {"category": category, "product_name": product_name},
                "drilldown": self._clickhouse.fetch_product_drilldown(filters, category, product_name),
            }
        return {"meta": {"category": category, "product_name": product_name}, "drilldown": {}}

    def get_issue_drilldown(self, filters: DashboardFilters, issue_id: str) -> dict[str, Any]:
        issue = self._find_issue(filters, issue_id)
        if self._clickhouse and settings.analytics_backend == "clickhouse":
            return {
                "issue": issue,
                "drilldown": self._clickhouse.fetch_issue_drilldown(filters, issue_id),
            }
        return {"issue": issue, "drilldown": {}}

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

        current_daily_rows = self._clickhouse.fetch_daily_rows(start_date, end_date, filters)
        previous_daily_rows = self._clickhouse.fetch_daily_rows(previous_start, previous_end, filters) if previous_end >= previous_start else []
        current_issue_rows = self._clickhouse.fetch_issue_rows(start_date, end_date, filters)
        previous_issue_rows = self._clickhouse.fetch_issue_rows(previous_start, previous_end, filters) if previous_end >= previous_start else []
        bot_rows = self._clickhouse.fetch_bot_rows(start_date, end_date, filters)
        pipeline_rows = self._clickhouse.fetch_pipeline_rows()

        issues = self._agg_issues(current_issue_rows, previous_issue_rows)
        category_health = self._agg_category_health(current_daily_rows)
        product_health = self._agg_product_health(current_daily_rows)
        service_ops = self._agg_service_ops(current_daily_rows)
        kpis = self._agg_kpis(current_daily_rows, previous_daily_rows)
        filter_options = self._agg_filter_options(current_daily_rows, issues, min_date, max_date)
        pipeline_health = self._agg_pipeline_health(pipeline_rows)

        return {
            "meta": {
                "source_mode": "clickhouse",
                "clickhouse_mode": True,
                "title": "Qubo Support Executive Board",
                "subtitle": "Executive view of ticket volume, installation tickets, issue concentration, and bot outcomes.",
                "window_start": start_date.isoformat(),
                "window_end": end_date.isoformat(),
            },
            "filters": asdict(filters),
            "filter_options": filter_options,
            "kpis": kpis,
            "spotlight": self._spotlight_cards(category_health, issues, bot_rows),
            "timeline": self._agg_timeline(current_daily_rows),
            "category_health": category_health,
            "product_health": product_health,
            "issue_views": self._agg_issue_views(issues),
            "service_ops": service_ops,
            "bot_summary": self._agg_bot_summary(bot_rows, issues),
            "pipeline_health": pipeline_health,
        }

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
            "open_tickets": self._metric(self._open_ticket_count(current), self._open_ticket_count(previous)),
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

    def _agg_category_health(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row.get("product_category") or "Other")].append(row)
        items = []
        for category, category_rows in grouped.items():
            counts = self._sum_counts(category_rows)
            issue_rows = [row for row in category_rows if row.get("fault_code_level_2") not in {None, "", "Unclassified"}]
            top_issue = max(issue_rows, key=lambda row: int(row.get("tickets", 0) or 0), default=None)
            efc_totals: dict[str, int] = defaultdict(int)
            for row in category_rows:
                efc_totals[str(row.get("executive_fault_code") or "Blank")] += int(row.get("tickets", 0) or 0)
            items.append(
                {
                    "product_category": category,
                    "tickets": counts["tickets"],
                    "installation_tickets": counts["installation_field_tickets"],
                    "installation_rate": self._ratio(counts["installation_field_tickets"], counts["tickets"]),
                    "repeat_rate": self._ratio(counts["repeat_tickets"], counts["tickets"]),
                    "bot_resolved_rate": self._ratio(counts["bot_resolved_tickets"], counts["tickets"]),
                    "open_rate": self._open_rate(category_rows),
                    "top_efc": max(efc_totals.items(), key=lambda item: item[1])[0] if efc_totals else "Blank",
                    "top_issue_detail": top_issue.get("fault_code_level_2") if top_issue else "Unclassified",
                }
            )
        return sorted(items, key=lambda item: item["tickets"], reverse=True)

    def _agg_product_health(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[(str(row.get("product_category") or "Other"), str(row.get("product_name") or "Other"))].append(row)
        items = []
        for (category, product_name), product_rows in grouped.items():
            counts = self._sum_counts(product_rows)
            efc_totals: dict[str, int] = defaultdict(int)
            for row in product_rows:
                efc_totals[str(row.get("executive_fault_code") or "Blank")] += int(row.get("tickets", 0) or 0)
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
                    "installation_tickets": counts["installation_field_tickets"],
                    "installation_rate": self._ratio(counts["installation_field_tickets"], counts["tickets"]),
                    "repeat_rate": self._ratio(counts["repeat_tickets"], counts["tickets"]),
                    "bot_resolved_rate": self._ratio(counts["bot_resolved_tickets"], counts["tickets"]),
                    "open_rate": self._open_rate(product_rows),
                    "top_efc": max(efc_totals.items(), key=lambda item: item[1])[0] if efc_totals else "Blank",
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
            "installation_tickets": [self._issue_payload(issue) for issue in sorted(issues, key=lambda item: (item.volume * item.installation_rate, item.volume), reverse=True)[:10]],
            "repeat_heavy": [self._issue_payload(issue) for issue in sorted(issues, key=lambda item: (item.volume * item.repeat_rate, item.volume), reverse=True)[:10]],
            "bot_leakage": [self._issue_payload(issue) for issue in sorted(issues, key=lambda item: (item.volume * item.bot_transfer_rate, item.volume), reverse=True)[:10]],
        }

    def _agg_service_ops(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "category_mix": self._mix(rows, "product_category"),
            "department_mix": self._mix(rows, "department_name"),
            "channel_mix": self._mix(rows, "channel"),
            "bot_action_mix": self._mix(rows, "normalized_bot_action"),
            "status_mix": self._mix(rows, "status"),
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
            "last_run_at": latest.get("run_finished_at").isoformat() if hasattr(latest.get("run_finished_at"), "isoformat") else str(latest.get("run_finished_at") or "Unknown"),
            "duration_minutes": int(latest.get("duration_minutes", 0) or 0),
            "rows_fetched": int(latest.get("source_rows", 0) or 0),
            "rows_inserted": int(latest.get("rows_inserted", 0) or 0),
            "recent_runs": [
                {
                    "started_at": row.get("run_started_at").isoformat() if hasattr(row.get("run_started_at"), "isoformat") else str(row.get("run_started_at") or ""),
                    "finished_at": row.get("run_finished_at").isoformat() if hasattr(row.get("run_finished_at"), "isoformat") else str(row.get("run_finished_at") or ""),
                    "duration_minutes": int(row.get("duration_minutes", 0) or 0),
                    "status": row.get("status") or "Unknown",
                    "rows_fetched": int(row.get("source_rows", 0) or 0),
                    "rows_inserted": int(row.get("rows_inserted", 0) or 0),
                    "message": row.get("message") or "",
                }
                for row in rows
            ],
        }

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

    def _spotlight_cards(self, categories: list[dict[str, Any]], issues: list[AggregateIssue], bot_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
        top_category = categories[0] if categories else None
        top_installation = max(issues, key=lambda item: item.volume * item.installation_rate, default=None)
        top_leak = max(issues, key=lambda item: item.volume * item.bot_transfer_rate, default=None)
        items = []
        if top_category:
            items.append({"title": "Highest-volume category", "detail": f"{top_category['product_category']} carries {top_category['tickets']:,} tickets in the selected period."})
        if top_installation:
            items.append({"title": "Top installation issue", "detail": f"{top_installation.fault_code_level_2} in {top_installation.product_name} has the highest share of installation tickets at {top_installation.installation_rate:.1%}.", "issue_id": top_installation.issue_id})
        if top_leak:
            items.append({"title": "Largest bot transfer issue", "detail": f"{top_leak.fault_code_level_2} in {top_leak.product_name} transfers out of bot at {top_leak.bot_transfer_rate:.1%}.", "issue_id": top_leak.issue_id})
        return items

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
                str(row.get("executive_fault_code") or "Blank"),
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

    def _mix(self, rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
        grouped: dict[str, int] = defaultdict(int)
        total = 0
        for row in rows:
            label = str(row.get(key) or "Unknown")
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
            "executive_fault_code": item.get("executive_fault_code") or "Blank",
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
            },
            "filters": asdict(filters),
            "filter_options": {"date_bounds": {"min": "", "max": ""}, "categories": [], "products": [], "products_by_category": {}, "departments": [], "channels": [], "efcs": [], "issue_details": [], "statuses": [], "fc1": [], "fc2": [], "bot_actions": []},
            "kpis": {
                "tickets": {"value": 0, "change": 0.0},
                "installation_tickets": {"value": 0.0, "change": 0.0},
                "bot_resolved": {"value": 0.0, "change": 0.0},
                "repeat_tickets": {"value": 0.0, "change": 0.0},
                "open_tickets": {"value": 0.0, "change": 0.0},
                "no_reopen_rate": {"value": 0.0, "change": 0.0},
            },
            "spotlight": [],
            "timeline": [],
            "category_health": [],
            "product_health": [],
            "issue_views": {"highest_volume": [], "installation_tickets": [], "repeat_heavy": [], "bot_leakage": []},
            "service_ops": {"category_mix": [], "department_mix": [], "channel_mix": [], "bot_action_mix": [], "status_mix": [], "installation_mix": []},
            "bot_summary": {"overview": {}, "by_product": [], "best_issues": [], "leaky_issues": []},
            "pipeline_health": {"status": "Unavailable", "last_run_at": "Unknown", "duration_minutes": 0, "rows_fetched": 0, "rows_inserted": 0, "recent_runs": []},
        }

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
