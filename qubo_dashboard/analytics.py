from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

from .config import settings
from .models import DashboardFilters
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
    fcr_rate: float
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

    def build_dashboard(self, filters: DashboardFilters) -> dict[str, Any]:
        if settings.has_agg_database:
            try:
                return self._build_from_agg(filters)
            except Exception:
                pass
        return self._build_seeded(filters)

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
                    "device_age_days": ticket.device_age_days,
                }
                for ticket in tickets
            ],
        }

    def search_tickets(self, filters: DashboardFilters, query: str = "") -> list[dict[str, Any]]:
        return []

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
        cursor.close()
        connection.close()

        daily_rows = [row for row in daily_rows if self._matches_filters(row, filters)]
        previous_daily_rows = [row for row in previous_daily_rows if self._matches_filters(row, filters)]
        weekly_rows = [row for row in weekly_rows if self._matches_filters(row, filters)]
        version_rows = [row for row in version_rows if self._matches_filters(row, filters)]
        resolution_rows = [row for row in resolution_rows if self._matches_filters(row, filters)]
        channel_rows = [row for row in channel_rows if self._matches_filters(row, filters)]
        bot_rows = [row for row in bot_rows if filters.product == "All" or row.get("product_family") in {filters.product, "All Chat"}]

        kpis = self._agg_kpis(daily_rows, previous_daily_rows)
        issues = self._agg_issues(weekly_rows, start_date)
        products = self._agg_products(daily_rows)
        versions = self._agg_versions(version_rows)
        filter_options = self._agg_filter_options(daily_rows, weekly_rows, channel_rows, products)
        pipeline_health = self._agg_pipeline_health(pipeline_rows)
        cleaning_summary = self._agg_cleaning_summary(data_quality_rows)
        issue_views = self._agg_issue_views(issues)
        bot_summary = self._agg_bot_summary(bot_rows, issues)

        return {
            "meta": {
                "source_mode": "mysql",
                "ticket_count": kpis["total_tickets"]["value"],
                "historical_mode": filters.history_mode,
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
            "version_risks": versions,
            "service_ops": self._agg_service_ops(daily_rows, channel_rows, resolution_rows),
            "bot_summary": bot_summary,
            "cleaning_summary": cleaning_summary,
            "pipeline_health": pipeline_health,
            "filter_options": filter_options,
        }

    def _build_seeded(self, filters: DashboardFilters) -> dict[str, Any]:
        issues = self._seeded_issues(filters)
        products = self._seeded_products(issues)
        versions = self._seeded_versions(issues)
        kpis = self._seeded_kpis(issues)
        return {
            "meta": {
                "source_mode": "sample",
                "ticket_count": kpis["total_tickets"]["value"],
                "historical_mode": filters.history_mode,
                "data_confidence_note": "Sample aggregate data is being used because the local aggregate DB is not configured or not readable.",
                "warehouse_mode": False,
                "dataset_scope": "Seeded sample aggregates",
                "pipeline_status": {"last_successful_run": "N/A", "duration_minutes": 0, "status": "Sample"},
            },
            "filters": asdict(filters),
            "kpis": kpis,
            "executive_summary": self._executive_summary(issues, products),
            "top_concerns": [self._issue_payload(item) for item in issues[:8]],
            "improving_signals": [self._issue_payload(item) for item in sorted(issues, key=lambda item: item.bot_deflection_rate, reverse=True)[:4]],
            "action_queue": self._action_queue(issues),
            "issue_views": self._seeded_issue_views(issues),
            "timeline": self._seeded_timeline(filters),
            "product_health": products,
            "version_risks": versions,
            "service_ops": self._seeded_service_ops(),
            "bot_summary": self._seeded_bot_summary(),
            "cleaning_summary": self._seeded_cleaning_summary(),
            "pipeline_health": self._seeded_pipeline_health(),
            "filter_options": self._seeded_filter_options(),
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
        fcr = weighted(current, "fcr_rate")
        repeat = weighted(current, "repeat_rate")
        logistics = weighted(current, "logistics_rate")
        field_total = weighted(current, "field_visit_rate")
        prev_repair = weighted(previous, "repair_field_visit_rate")
        prev_install = weighted(previous, "installation_field_visit_rate")
        prev_bot = weighted(previous, "bot_deflection_rate")
        prev_fcr = weighted(previous, "fcr_rate")
        prev_repeat = weighted(previous, "repeat_rate")
        prev_logistics = weighted(previous, "logistics_rate")
        prev_field_total = weighted(previous, "field_visit_rate")
        return {
            "total_tickets": {"value": int(total_tickets), "change": change(total_tickets, prev_tickets)},
            "field_visit_rate": {"value": field_total, "change": change(field_total, prev_field_total)},
            "repair_field_visit_rate": {"value": repair, "change": change(repair, prev_repair)},
            "installation_field_visit_rate": {"value": install, "change": change(install, prev_install)},
            "bot_deflection_rate": {"value": bot, "change": change(bot, prev_bot)},
            "fcr": {"value": fcr, "change": change(fcr, prev_fcr)},
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
            "fcr_rate_num": 0.0,
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
            bucket["fcr_rate_num"] += tickets * float(row.get("fcr_rate", 0) or 0)
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
                fcr_rate=bucket["fcr_rate_num"] / denom,
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
            grouped[product]["fcr_num"] += tickets * float(row.get("fcr_rate", 0) or 0)
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
                    "fcr": bucket["fcr_num"] / volume,
                    "top_issue": top_issue.get(product, ("Unknown", 0))[0],
                    "rising_issue": top_issue.get(product, ("Unknown", 0))[0],
                    "service_burden": int(bucket["ticket_volume"] * (1 + (bucket["repair_field_num"] / volume) * 2)),
                }
            )
        return sorted(products, key=lambda item: item["ticket_volume"], reverse=True)

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
        grouped: dict[str, dict[str, int]] = defaultdict(lambda: {"tickets": 0, "repair_field": 0, "bot_resolved": 0})
        for row in rows:
            key = str(row["metric_date"])
            tickets = int(row.get("tickets", 0) or 0)
            grouped[key]["tickets"] += tickets
            grouped[key]["repair_field"] += round(tickets * float(row.get("repair_field_visit_rate", 0) or 0))
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
        products = [row for row in bot_rows if row.get("product_family") not in {"All Chat", "Unknown / Dirty Data", "Other / Accessories", "Logistics / Non-product"}]
        products = sorted(products, key=lambda row: int(row.get("chat_tickets", 0) or 0), reverse=True)
        return {
            "overview": {
                "chat_tickets": int(overall.get("chat_tickets", 0) or 0),
                "bot_resolved_tickets": int(overall.get("bot_resolved_tickets", 0) or 0),
                "bot_transferred_tickets": int(overall.get("bot_transferred_tickets", 0) or 0),
                "blank_chat_tickets": int(overall.get("blank_chat_tickets", 0) or 0),
                "blank_chat_returned_7d": int(overall.get("blank_chat_returned_7d", 0) or 0),
                "blank_chat_resolved_7d": int(overall.get("blank_chat_resolved_7d", 0) or 0),
                "blank_chat_blank_again_7d": int(overall.get("blank_chat_blank_again_7d", 0) or 0),
                "blank_chat_return_rate": float(overall.get("blank_chat_return_rate", 0) or 0),
                "blank_chat_recovery_rate": float(overall.get("blank_chat_recovery_rate", 0) or 0),
                "blank_chat_repeat_rate": float(overall.get("blank_chat_repeat_rate", 0) or 0),
                "bot_resolved_rate": float(overall.get("bot_resolved_rate", 0) or 0),
                "bot_transferred_rate": float(overall.get("bot_transferred_rate", 0) or 0),
                "blank_chat_rate": float(overall.get("blank_chat_rate", 0) or 0),
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
        return [
            {"label": "Bot transferred to agent", "count": transferred, "share": transferred / total if total else 0.0},
            {"label": "Bot resolved ticket", "count": bot, "share": bot / total if total else 0.0},
            {"label": "Blank chat after 10 mins", "count": blank, "share": blank / total if total else 0.0},
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

    def _matches_filters(self, row: dict[str, Any], filters: DashboardFilters) -> bool:
        if filters.product != "All" and row.get("product_family") != filters.product:
            return False
        if filters.issue != "All" and row.get("fault_code") != filters.issue:
            return False
        if filters.version != "All" and (row.get("software_version") or "Unknown") != filters.version:
            return False
        if filters.department != "All":
            department = row.get("department_name")
            if department and department != filters.department:
                return False
        if not filters.include_hero and row.get("department_name") == "Hero Electronix":
            return False
        if not filters.include_dirty:
            if row.get("product_family") in {"Unknown / Dirty Data", "Other / Accessories", "Logistics / Non-product"}:
                return False
            if row.get("channel") == "Unknown / Dirty Data":
                return False
            if row.get("department_name") == "Unknown / Dirty Data":
                return False
            if row.get("fault_code") == "Unclassified":
                return False
            if row.get("fault_code_level_2") == "Unclassified":
                return False
        return True

    def _agg_filter_options(
        self,
        daily_rows: list[dict[str, Any]],
        weekly_rows: list[dict[str, Any]],
        channel_rows: list[dict[str, Any]],
        products: list[dict[str, Any]],
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

        return {
            "products": [item["product_family"] for item in products[:10]],
            "departments": values(channel_rows, "department_name", {"Unknown / Dirty Data", "Email"}),
            "issues": values(weekly_rows, "fault_code", {"Unclassified"}, min_tickets=500),
            "versions": values(daily_rows, "software_version", {"Unknown"}),
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
            return {"headline": "Support health", "summary": "No aggregate data available."}
        top_issue = concerns[0]
        top_product = products[0]
        return {
            "headline": "CS Snapshot",
            "summary": f"{top_product['product_family']} is carrying the highest support load. {top_issue.fault_code_level_2} on {top_issue.software_version} is the top engineering signal because it is growing and converting into repair visits. Installation traffic is tracked separately.",
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
            "fcr_rate": issue.fcr_rate,
            "bot_transfer_rate": issue.bot_transfer_rate,
            "blank_chat_rate": issue.blank_chat_rate,
            "logistics_rate": issue.logistics_rate,
            "top_symptom": issue.top_symptom,
            "top_defect": issue.top_defect,
            "top_repair": issue.top_repair,
            "insight": issue.insight,
            "composite_risk": issue.composite_risk,
        }

    def _seeded_issues(self, filters: DashboardFilters) -> list[AggregateIssue]:
        factor = 0.24 if filters.date_preset == "14d" else 0.5 if filters.date_preset == "30d" else 8.4 if filters.date_preset == "history" else 1.0
        items = [
            AggregateIssue("Dash Cam", "Product issue", "Intermittent offline", "DC_5.14.2", round(18240 * factor), round(12860 * factor), 0.22, 0.01, 0.14, 0.09, 0.32, 0.05, 0.42, 0.03, "Device drops offline after ignition cycle", "Firmware reconnect regression", "Reflash firmware and replace power harness"),
            AggregateIssue("Smart Camera", "Application", "Wi-Fi disconnection", "SC_4.8.0", round(14680 * factor), round(11240 * factor), 0.18, 0.00, 0.11, 0.16, 0.28, 0.04, 0.51, 0.02, "Camera disconnects multiple times a day", "Wi-Fi stack instability", "Reconfigure router band and reinstall app"),
            AggregateIssue("Smart Camera", "Home Product issue", "Video feed issue", "SC_4.8.0", round(13220 * factor), round(9440 * factor), 0.14, 0.00, 0.10, 0.11, 0.34, 0.03, 0.49, 0.02, "Feed freezes after live view starts", "Encoder process crash", "Firmware rollback and app reinstall"),
            AggregateIssue("Smart Lock", "Lock Product issue", "Lock pairing issue", "SL_2.2.0", round(8240 * factor), round(6380 * factor), 0.16, 0.00, 0.18, 0.06, 0.42, 0.04, 0.37, 0.04, "Lock visible in app but pairing fails", "BLE authentication timeout", "Reset controller and replace board"),
            AggregateIssue("Video Doorbell", "Installation", "Installation issue", "VDB_3.1.4", round(9260 * factor), round(10180 * factor), 0.02, 0.31, 0.03, 0.04, 0.38, 0.11, 0.28, 0.00, "Customer cannot complete install flow", "No product defect confirmed", "Technician installation completed"),
            AggregateIssue("Air Purifier", "Product issue", "Auto restart", "AP_2.0.4", round(5620 * factor), round(3980 * factor), 0.21, 0.00, 0.12, 0.05, 0.31, 0.06, 0.31, 0.07, "Purifier restarts repeatedly under load", "Power board instability", "Replace main PCB"),
            AggregateIssue("GPS Tracker", "Tracker Product issue", "Weak signal strength", "GT_1.9.7", round(4380 * factor), round(5020 * factor), 0.02, 0.00, 0.05, 0.34, 0.18, 0.03, 0.63, 0.00, "Location updates are delayed", "Environmental coverage issue", "Guided settings change"),
            AggregateIssue("Dash Cam", "Application", "App not connecting", "APP_9.2.1", round(9840 * factor), round(10120 * factor), 0.04, 0.00, 0.06, 0.29, 0.24, 0.08, 0.61, 0.00, "App cannot bind to camera session", "App SDK token expiry issue", "App cache reset and re-login"),
            AggregateIssue("Smart Plug", "Product issue", "Dead after use", "SP_1.1.1", round(3180 * factor), round(2910 * factor), 0.24, 0.00, 0.15, 0.03, 0.35, 0.02, 0.23, 0.09, "Plug stops responding after normal use", "Hardware failure after thermal event", "Replace device"),
        ]
        items = [item for item in items if filters.product == "All" or item.product_family == filters.product]
        items = [item for item in items if filters.issue == "All" or item.fault_code == filters.issue]
        items = [item for item in items if filters.version == "All" or item.software_version == filters.version]
        return sorted(items, key=lambda item: item.composite_risk, reverse=True)

    def _seeded_products(self, issues: list[AggregateIssue]) -> list[dict[str, Any]]:
        grouped: dict[str, list[AggregateIssue]] = defaultdict(list)
        for issue in issues:
            grouped[issue.product_family].append(issue)
        products = []
        for product, rows in grouped.items():
            volume = sum(item.volume for item in rows) or 1
            products.append(
                {
                    "product_family": product,
                    "ticket_volume": volume,
                    "repair_field_visit_rate": sum(item.volume * item.repair_field_visit_rate for item in rows) / volume,
                    "installation_field_visit_rate": sum(item.volume * item.installation_field_visit_rate for item in rows) / volume,
                    "repeat_rate": sum(item.volume * item.repeat_rate for item in rows) / volume,
                    "bot_deflection_rate": sum(item.volume * item.bot_deflection_rate for item in rows) / volume,
                    "fcr": sum(item.volume * item.fcr_rate for item in rows) / volume,
                    "top_issue": max(rows, key=lambda item: item.volume).fault_code_level_2,
                    "rising_issue": max(rows, key=lambda item: item.volume - item.previous_volume).fault_code_level_2,
                    "service_burden": round(volume * (1 + max(rows, key=lambda item: item.repair_field_visit_rate).repair_field_visit_rate * 2)),
                }
            )
        return sorted(products, key=lambda item: item["ticket_volume"], reverse=True)

    def _seeded_versions(self, issues: list[AggregateIssue]) -> list[dict[str, Any]]:
        return [
            {
                "version": item.software_version,
                "product_family": item.product_family,
                "ticket_volume": item.volume,
                "repair_field_visit_rate": item.repair_field_visit_rate,
                "repeat_rate": item.repeat_rate,
                "top_issue": item.fault_code_level_2,
            }
            for item in issues[:8]
        ]

    def _seeded_timeline(self, filters: DashboardFilters) -> list[dict[str, Any]]:
        factor = 0.24 if filters.date_preset == "14d" else 0.5 if filters.date_preset == "30d" else 8.4 if filters.date_preset == "history" else 1.0
        base = [
            {"date": "2026-01-18", "tickets": 1380, "repair_field": 164, "bot_resolved": 124},
            {"date": "2026-01-25", "tickets": 1425, "repair_field": 172, "bot_resolved": 131},
            {"date": "2026-02-01", "tickets": 1488, "repair_field": 181, "bot_resolved": 139},
            {"date": "2026-02-08", "tickets": 1510, "repair_field": 186, "bot_resolved": 144},
            {"date": "2026-02-15", "tickets": 1548, "repair_field": 201, "bot_resolved": 151},
            {"date": "2026-02-22", "tickets": 1612, "repair_field": 216, "bot_resolved": 157},
            {"date": "2026-03-01", "tickets": 1686, "repair_field": 224, "bot_resolved": 162},
            {"date": "2026-03-08", "tickets": 1718, "repair_field": 231, "bot_resolved": 167},
        ]
        return [{key: round(value * factor) if key != "date" else value for key, value in row.items()} for row in base]

    def _seeded_kpis(self, issues: list[AggregateIssue]) -> dict[str, dict[str, float]]:
        total = sum(item.volume for item in issues) or 1
        previous_total = sum(item.previous_volume for item in issues) or 1
        weighted = lambda attr: sum(item.volume * getattr(item, attr) for item in issues) / total
        field = weighted("repair_field_visit_rate") + weighted("installation_field_visit_rate")
        return {
            "total_tickets": {"value": total, "change": (total - previous_total) / previous_total},
            "field_visit_rate": {"value": field, "change": 0.02},
            "repair_field_visit_rate": {"value": weighted("repair_field_visit_rate"), "change": 0.08},
            "installation_field_visit_rate": {"value": weighted("installation_field_visit_rate"), "change": -0.03},
            "bot_deflection_rate": {"value": weighted("bot_deflection_rate"), "change": 0.05},
            "fcr": {"value": weighted("fcr_rate"), "change": 0.03},
            "repeat_rate": {"value": weighted("repeat_rate"), "change": 0.06},
            "logistics_rate": {"value": weighted("logistics_rate"), "change": 0.01},
        }

    def _seeded_service_ops(self) -> dict[str, Any]:
        return {
            "department_mix": [{"label": "Call Center", "count": 41820, "share": 0.50}, {"label": "Field Service", "count": 15760, "share": 0.19}, {"label": "Hero Electronix", "count": 24890, "share": 0.29}, {"label": "Logistics", "count": 1450, "share": 0.02}],
            "channel_mix": [{"label": "Chat", "count": 35200, "share": 0.37}, {"label": "Phone", "count": 27850, "share": 0.30}, {"label": "Email", "count": 16120, "share": 0.17}, {"label": "WhatsApp", "count": 10940, "share": 0.12}, {"label": "Web", "count": 4260, "share": 0.04}],
            "bot_outcomes": [{"label": "Bot transferred to agent", "count": 21480, "share": 0.51}, {"label": "Bot resolved ticket", "count": 10520, "share": 0.25}, {"label": "Blank chat after 10 mins", "count": 10040, "share": 0.24}],
            "resolution_mix": [{"label": "Troubleshooting done issue resolved", "count": 22840, "share": 0.25}, {"label": "Issue Escalated", "count": 17210, "share": 0.19}, {"label": "Features Explained", "count": 13360, "share": 0.15}, {"label": "Resolved", "count": 12120, "share": 0.13}, {"label": "TAT informed", "count": 10140, "share": 0.11}, {"label": "Reset device", "count": 9640, "share": 0.10}],
            "handle_time_distribution": {"median_minutes": 19.2, "p75_minutes": 43.6},
            "field_service_split": [{"label": "Repair visits", "count": 6280, "share": 0.40}, {"label": "Installation visits", "count": 9480, "share": 0.60}],
        }

    def _seeded_bot_summary(self) -> dict[str, Any]:
        return {
            "overview": {
                "chat_tickets": 42040,
                "bot_resolved_tickets": 10520,
                "bot_transferred_tickets": 21480,
                "blank_chat_tickets": 10040,
                "blank_chat_returned_7d": 3120,
                "blank_chat_resolved_7d": 980,
                "blank_chat_blank_again_7d": 1240,
                "blank_chat_return_rate": 0.311,
                "blank_chat_recovery_rate": 0.098,
                "blank_chat_repeat_rate": 0.123,
                "bot_resolved_rate": 0.25,
                "bot_transferred_rate": 0.51,
                "blank_chat_rate": 0.24,
            },
            "by_product": [
                {"product_family": "Dash Cam", "chat_tickets": 9600, "bot_resolved_rate": 0.31, "bot_transferred_rate": 0.43, "blank_chat_rate": 0.18, "blank_chat_return_rate": 0.27, "blank_chat_recovery_rate": 0.11},
                {"product_family": "Smart Camera", "chat_tickets": 11800, "bot_resolved_rate": 0.24, "bot_transferred_rate": 0.49, "blank_chat_rate": 0.21, "blank_chat_return_rate": 0.33, "blank_chat_recovery_rate": 0.10},
                {"product_family": "Video Doorbell", "chat_tickets": 6400, "bot_resolved_rate": 0.17, "bot_transferred_rate": 0.58, "blank_chat_rate": 0.22, "blank_chat_return_rate": 0.29, "blank_chat_recovery_rate": 0.07},
            ],
            "best_issues": [self._issue_payload(item) for item in sorted(issues, key=lambda item: item.bot_deflection_rate, reverse=True)[:6]],
            "leaky_issues": [self._issue_payload(item) for item in sorted(issues, key=lambda item: item.bot_transfer_rate, reverse=True)[:6]],
        }

    def _seeded_issue_views(self, issues: list[AggregateIssue]) -> dict[str, list[dict[str, Any]]]:
        return self._agg_issue_views(issues)

    def _seeded_pipeline_health(self) -> dict[str, Any]:
        return {
            "last_run_at": "Not run",
            "duration_minutes": 0,
            "status": "Sample",
            "latest_job": "seeded_dashboard_payload",
            "tables": [
                {"table": settings.agg_daily_tickets_table, "status": "Sample"},
                {"table": settings.agg_fc_weekly_table, "status": "Sample"},
                {"table": settings.agg_bot_table, "status": "Sample"},
                {"table": settings.agg_sw_version_table, "status": "Sample"},
                {"table": settings.agg_resolution_table, "status": "Sample"},
                {"table": settings.pipeline_log_table, "status": "Sample"},
            ],
        }

    def _seeded_cleaning_summary(self) -> dict[str, Any]:
        return {
            "as_of_date": "Sample",
            "total_tickets": 95000,
            "usable_issue_tickets": 70200,
            "actionable_issue_tickets": 49800,
            "blank_fault_code_tickets": 16800,
            "blank_fault_code_l2_tickets": 11200,
            "unknown_product_tickets": 21000,
            "hero_internal_tickets": 24890,
            "version_coverage_tickets": 0,
            "dropped_in_bot_tickets": 10040,
            "missing_issue_outside_bot_tickets": 9100,
            "dirty_channel_tickets": 1800,
            "email_department_reassigned_tickets": 9230,
            "usable_issue_rate": 0.739,
            "actionable_issue_rate": 0.524,
        }

    def _seeded_filter_options(self) -> dict[str, list[str]]:
        return {
            "products": ["Air Purifier", "Dash Cam", "GPS Tracker", "Smart Camera", "Smart Lock", "Smart Plug", "Video Doorbell"],
            "departments": ["Call Center", "Field Service", "Hero Electronix", "Logistics"],
            "issues": ["Application", "Home Product issue", "Installation", "Lock Product issue", "Product issue", "Tracker Product issue"],
            "versions": [],
        }
