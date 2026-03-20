from __future__ import annotations

from dataclasses import asdict
from datetime import date, timedelta
from typing import Any

from .config import settings
from .models import DashboardFilters
from .repository import TicketRepository


class SemanticService:
    def __init__(self, repository: TicketRepository) -> None:
        self._repository = repository

    def summary(self, filters: DashboardFilters) -> dict[str, Any]:
        latest = self._latest_date()
        if latest is None:
            return {"meta": {"dataset_scope": "semantic_v2", "latest_date": None}, "filters": asdict(filters), "kpis": {}}
        start_date = self._window_start(latest, filters.date_preset)
        previous_start = start_date - (latest - start_date) - timedelta(days=1)
        current = self._fetch_metric_counts(filters, start_date, latest)
        previous = self._fetch_metric_counts(filters, previous_start, start_date - timedelta(days=1))
        return {
            "meta": {"dataset_scope": "semantic_v2", "latest_date": latest.isoformat()},
            "filters": asdict(filters),
            "kpis": self._kpi_payload(current, previous),
        }

    def trend(self, filters: DashboardFilters) -> dict[str, Any]:
        latest = self._latest_date()
        if latest is None:
            return {"meta": {"dataset_scope": "semantic_v2", "latest_date": None}, "filters": asdict(filters), "timeline": []}
        start_date = self._window_start(latest, filters.date_preset)
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        where_sql, params = self._base_where(filters, start_date, latest)
        cursor.execute(
            f"""
            SELECT
                event_date AS metric_date,
                COUNT(*) AS tickets,
                SUM(is_repair_visit) AS repair_visit_tickets,
                SUM(is_installation_visit) AS installation_visit_tickets,
                SUM(is_bot_resolved) AS bot_resolved_tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY event_date
            ORDER BY event_date
            """,
            params,
        )
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return {
            "meta": {"dataset_scope": "semantic_v2", "latest_date": latest.isoformat()},
            "filters": asdict(filters),
            "timeline": [
                {
                    "date": str(row["metric_date"]),
                    "tickets": int(row.get("tickets", 0) or 0),
                    "repair_field": int(row.get("repair_visit_tickets", 0) or 0),
                    "install_field": int(row.get("installation_visit_tickets", 0) or 0),
                    "bot_resolved": int(row.get("bot_resolved_tickets", 0) or 0),
                }
                for row in rows
            ],
        }

    def product_burden(self, filters: DashboardFilters) -> dict[str, Any]:
        latest = self._latest_date()
        if latest is None:
            return {"meta": {"dataset_scope": "semantic_v2", "latest_date": None}, "filters": asdict(filters), "rows": []}
        start_date = self._window_start(latest, filters.date_preset)
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        where_sql, params = self._base_where(filters, start_date, latest)
        cursor.execute(
            f"""
            SELECT
                product_family,
                COUNT(*) AS tickets,
                SUM(repeat_flag) AS repeat_tickets,
                SUM(is_bot_resolved) AS bot_resolved_tickets,
                SUM(is_bot_transferred) AS bot_transferred_tickets,
                SUM(is_repair_visit) AS repair_visit_tickets,
                SUM(is_installation_visit) AS installation_visit_tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY product_family
            ORDER BY tickets DESC
            """,
            params,
        )
        product_rows = cursor.fetchall()
        result = []
        for row in product_rows:
            product = row["product_family"]
            cursor.execute(
                f"""
                SELECT fault_code_level_2, COUNT(*) AS tickets
                FROM {settings.ticket_facts_table}
                WHERE {where_sql} AND product_family = %s
                GROUP BY fault_code_level_2
                ORDER BY tickets DESC, fault_code_level_2
                LIMIT 1
                """,
                (*params, product),
            )
            top_issue_row = cursor.fetchone() or {}
            tickets = int(row.get("tickets", 0) or 0)
            result.append(
                {
                    "product_family": product,
                    "ticket_volume": tickets,
                    "repeat_rate": self._rate(row.get("repeat_tickets"), tickets),
                    "bot_deflection_rate": self._rate(row.get("bot_resolved_tickets"), tickets),
                    "bot_transfer_rate": self._rate(row.get("bot_transferred_tickets"), tickets),
                    "repair_field_visit_rate": self._rate(row.get("repair_visit_tickets"), tickets),
                    "installation_field_visit_rate": self._rate(row.get("installation_visit_tickets"), tickets),
                    "top_issue": top_issue_row.get("fault_code_level_2") or "Unknown",
                }
            )
        cursor.close()
        connection.close()
        return {
            "meta": {"dataset_scope": "semantic_v2", "latest_date": latest.isoformat()},
            "filters": asdict(filters),
            "rows": result,
        }

    def model_breakdown(self, filters: DashboardFilters) -> dict[str, Any]:
        latest = self._latest_date()
        if latest is None:
            return {"meta": {"dataset_scope": "semantic_v2", "latest_date": None}, "filters": asdict(filters), "rows": []}
        start_date = self._window_start(latest, filters.date_preset)
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        where_sql, params = self._base_where(filters, start_date, latest)
        cursor.execute(
            f"""
            SELECT
                product_family,
                model_name,
                COUNT(*) AS tickets,
                SUM(repeat_flag) AS repeat_tickets,
                SUM(is_bot_resolved) AS bot_resolved_tickets,
                SUM(is_bot_transferred) AS bot_transferred_tickets,
                SUM(is_repair_visit) AS repair_visit_tickets,
                SUM(is_installation_visit) AS installation_visit_tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY product_family, model_name
            ORDER BY product_family, tickets DESC
            """,
            params,
        )
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        payload = []
        for row in rows:
            tickets = int(row.get("tickets", 0) or 0)
            payload.append(
                {
                    "product_family": row["product_family"],
                    "model_name": row["model_name"],
                    "tickets": tickets,
                    "repeat_rate": self._rate(row.get("repeat_tickets"), tickets),
                    "bot_deflection_rate": self._rate(row.get("bot_resolved_tickets"), tickets),
                    "bot_transfer_rate": self._rate(row.get("bot_transferred_tickets"), tickets),
                    "repair_field_visit_rate": self._rate(row.get("repair_visit_tickets"), tickets),
                    "installation_field_visit_rate": self._rate(row.get("installation_visit_tickets"), tickets),
                }
            )
        return {
            "meta": {"dataset_scope": "semantic_v2", "latest_date": latest.isoformat()},
            "filters": asdict(filters),
            "rows": payload,
        }

    def channel_mix(self, filters: DashboardFilters) -> dict[str, Any]:
        latest = self._latest_date()
        if latest is None:
            return {"meta": {"dataset_scope": "semantic_v2", "latest_date": None}, "filters": asdict(filters), "rows": []}
        start_date = self._window_start(latest, filters.date_preset)
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        where_sql, params = self._base_where(filters, start_date, latest)
        cursor.execute(
            f"""
            SELECT channel, COUNT(*) AS tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY channel
            ORDER BY tickets DESC
            """,
            params,
        )
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        total = sum(int(row.get("tickets", 0) or 0) for row in rows) or 1
        return {
            "meta": {"dataset_scope": "semantic_v2", "latest_date": latest.isoformat()},
            "filters": asdict(filters),
            "rows": [
                {
                    "label": row.get("channel") or "Unknown",
                    "count": int(row.get("tickets", 0) or 0),
                    "share": self._rate(row.get("tickets"), total),
                }
                for row in rows
            ],
        }

    def product_drill(self, filters: DashboardFilters, product_family: str) -> dict[str, Any]:
        scoped = DashboardFilters(
            date_preset=filters.date_preset,
            products=[product_family],
            models=list(filters.models),
            fault_codes=list(filters.fault_codes),
            channels=list(filters.channels),
            bot_actions=list(filters.bot_actions),
            quick_exclusions=list(filters.quick_exclusions),
        )
        latest = self._latest_date()
        if latest is None:
            return {"meta": {"dataset_scope": "semantic_v2", "latest_date": None}, "filters": asdict(scoped), "product_family": product_family}
        start_date = self._window_start(latest, scoped.date_preset)
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        where_sql, params = self._base_where(scoped, start_date, latest)

        counts = self._fetch_metric_counts(scoped, start_date, latest)
        cursor.execute(
            f"""
            SELECT model_name, COUNT(*) AS tickets, SUM(is_repair_visit) AS repair_visit_tickets,
                   SUM(is_bot_resolved) AS bot_resolved_tickets, SUM(repeat_flag) AS repeat_tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY model_name
            ORDER BY tickets DESC
            LIMIT 12
            """,
            params,
        )
        models = cursor.fetchall()
        cursor.execute(
            f"""
            SELECT fault_code, COUNT(*) AS tickets, SUM(is_repair_visit) AS repair_visit_tickets, SUM(repeat_flag) AS repeat_tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY fault_code
            ORDER BY tickets DESC
            LIMIT 8
            """,
            params,
        )
        categories = cursor.fetchall()
        cursor.execute(
            f"""
            SELECT fault_code_level_2, COUNT(*) AS tickets, SUM(is_repair_visit) AS repair_visit_tickets,
                   SUM(repeat_flag) AS repeat_tickets, SUM(is_bot_resolved) AS bot_resolved_tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY fault_code_level_2
            ORDER BY tickets DESC
            LIMIT 10
            """,
            params,
        )
        issues = cursor.fetchall()
        cursor.execute(
            f"""
            SELECT channel, COUNT(*) AS tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY channel
            ORDER BY tickets DESC
            LIMIT 6
            """,
            params,
        )
        channels = cursor.fetchall()
        cursor.close()
        connection.close()

        total = int(counts["tickets"])
        return {
            "meta": {"dataset_scope": "semantic_v2", "latest_date": latest.isoformat()},
            "filters": asdict(scoped),
            "product_family": product_family,
            "summary": self._kpi_payload(counts, None),
            "models": [self._model_row(row) for row in models],
            "categories": [self._mix_row(row, total, "fault_code") for row in categories],
            "issues": [self._issue_row(row, total) for row in issues],
            "channels": [self._mix_row(row, total, "channel") for row in channels],
        }

    def _base_where(self, filters: DashboardFilters, start_date: date, end_date: date) -> tuple[str, tuple[Any, ...]]:
        clauses = ["event_date BETWEEN %s AND %s"]
        params: list[Any] = [start_date, end_date]
        if filters.products:
            clauses.append(f"product_family IN ({', '.join(['%s'] * len(filters.products))})")
            params.extend(filters.products)
        if filters.models:
            clauses.append(f"model_name IN ({', '.join(['%s'] * len(filters.models))})")
            params.extend(filters.models)
        if filters.fault_codes:
            clauses.append(f"fault_code IN ({', '.join(['%s'] * len(filters.fault_codes))})")
            params.extend(filters.fault_codes)
        if filters.channels:
            clauses.append(f"channel IN ({', '.join(['%s'] * len(filters.channels))})")
            params.extend(filters.channels)
        if filters.bot_actions:
            clauses.append(f"bot_action_group IN ({', '.join(['%s'] * len(filters.bot_actions))})")
            params.extend(filters.bot_actions)
        if "installations" in filters.quick_exclusions:
            clauses.append("is_installation = 0")
        if "blank_chat" in filters.quick_exclusions:
            clauses.append("is_blank_chat = 0")
        if "duplicate_tickets" in filters.quick_exclusions:
            clauses.append("is_duplicate_ticket = 0")
        if "sales_marketing" in filters.quick_exclusions:
            clauses.append("is_sales_marketing = 0")
        return " AND ".join(clauses), tuple(params)

    def _latest_date(self) -> date | None:
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor()
        cursor.execute(f"SELECT MAX(event_date) FROM {settings.ticket_facts_table}")
        row = cursor.fetchone()
        cursor.close()
        connection.close()
        return row[0] if row and row[0] else None

    def _window_start(self, latest: date, preset: str) -> date:
        if preset == "14d":
            return latest - timedelta(days=13)
        if preset == "30d":
            return latest - timedelta(days=29)
        if preset == "history":
            return latest - timedelta(days=580)
        return latest - timedelta(days=59)

    def _fetch_metric_counts(self, filters: DashboardFilters, start_date: date, end_date: date) -> dict[str, int]:
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        where_sql, params = self._base_where(filters, start_date, end_date)
        cursor.execute(
            f"""
            SELECT
                COUNT(*) AS tickets,
                COALESCE(SUM(repeat_flag), 0) AS repeat_tickets,
                COALESCE(SUM(is_bot_resolved), 0) AS bot_resolved_tickets,
                COALESCE(SUM(is_bot_transferred), 0) AS bot_transferred_tickets,
                COALESCE(SUM(is_repair_visit), 0) AS repair_visit_tickets,
                COALESCE(SUM(is_installation_visit), 0) AS installation_visit_tickets,
                COALESCE(SUM(is_field_visit), 0) AS field_visit_tickets
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            """,
            params,
        )
        row = cursor.fetchone() or {}
        cursor.close()
        connection.close()
        return {key: int(row.get(key, 0) or 0) for key in row}

    def _kpi_payload(self, current: dict[str, int], previous: dict[str, int] | None) -> dict[str, dict[str, float]]:
        previous = previous or {}
        current_tickets = int(current.get("tickets", 0))
        previous_tickets = int(previous.get("tickets", 0))
        return {
            "total_tickets": {"value": current_tickets, "change": self._change(current_tickets, previous_tickets)},
            "repeat_rate": {"value": self._rate(current.get("repeat_tickets"), current_tickets), "change": self._change_rate(current.get("repeat_tickets"), current_tickets, previous.get("repeat_tickets"), previous_tickets)},
            "bot_deflection_rate": {"value": self._rate(current.get("bot_resolved_tickets"), current_tickets), "change": self._change_rate(current.get("bot_resolved_tickets"), current_tickets, previous.get("bot_resolved_tickets"), previous_tickets)},
            "bot_transfer_rate": {"value": self._rate(current.get("bot_transferred_tickets"), current_tickets), "change": self._change_rate(current.get("bot_transferred_tickets"), current_tickets, previous.get("bot_transferred_tickets"), previous_tickets)},
            "repair_field_visit_rate": {"value": self._rate(current.get("repair_visit_tickets"), current_tickets), "change": self._change_rate(current.get("repair_visit_tickets"), current_tickets, previous.get("repair_visit_tickets"), previous_tickets)},
            "installation_field_visit_rate": {"value": self._rate(current.get("installation_visit_tickets"), current_tickets), "change": self._change_rate(current.get("installation_visit_tickets"), current_tickets, previous.get("installation_visit_tickets"), previous_tickets)},
            "field_visit_rate": {"value": self._rate(current.get("field_visit_tickets"), current_tickets), "change": self._change_rate(current.get("field_visit_tickets"), current_tickets, previous.get("field_visit_tickets"), previous_tickets)},
        }

    def _mix_row(self, row: dict[str, Any], total: int, key: str) -> dict[str, Any]:
        count = int(row.get("tickets", 0) or 0)
        return {"label": row.get(key) or "Unknown", "count": count, "share": self._rate(count, total)}

    def _issue_row(self, row: dict[str, Any], total: int) -> dict[str, Any]:
        count = int(row.get("tickets", 0) or 0)
        return {
            "label": row.get("fault_code_level_2") or "Unknown",
            "count": count,
            "share": self._rate(count, total),
            "repair_rate": self._rate(row.get("repair_visit_tickets"), count),
            "repeat_rate": self._rate(row.get("repeat_tickets"), count),
            "bot_rate": self._rate(row.get("bot_resolved_tickets"), count),
        }

    def _model_row(self, row: dict[str, Any]) -> dict[str, Any]:
        tickets = int(row.get("tickets", 0) or 0)
        return {
            "model_name": row.get("model_name") or "Unknown",
            "tickets": tickets,
            "repair_rate": self._rate(row.get("repair_visit_tickets"), tickets),
            "repeat_rate": self._rate(row.get("repeat_tickets"), tickets),
            "bot_rate": self._rate(row.get("bot_resolved_tickets"), tickets),
        }

    def _rate(self, numerator: Any, denominator: Any) -> float:
        num = float(numerator or 0)
        den = float(denominator or 0)
        return num / den if den else 0.0

    def _change(self, current: float, previous: float) -> float:
        return 0.0 if not previous else (current - previous) / previous

    def _change_rate(self, current_num: Any, current_den: Any, previous_num: Any, previous_den: Any) -> float:
        current_rate = self._rate(current_num, current_den)
        previous_rate = self._rate(previous_num, previous_den)
        return 0.0 if not previous_rate else (current_rate - previous_rate) / previous_rate

    # ── Phase 2 methods ────────────────────────────────────────────────────────

    def period_breakdown(self, filters: DashboardFilters, start_date: date, end_date: date) -> dict[str, Any]:
        days = max((end_date - start_date).days + 1, 1)
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days - 1)
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        where_sql, params = self._base_where(filters, start_date, end_date)
        prev_where, prev_params = self._base_where(filters, prev_start, prev_end)

        kpi_select = f"""
            SELECT COUNT(*) AS tickets,
                   COALESCE(SUM(repeat_flag),0) AS repeat_tickets,
                   COALESCE(SUM(is_bot_resolved),0) AS bot_resolved_tickets,
                   COALESCE(SUM(is_bot_transferred),0) AS bot_transferred_tickets,
                   COALESCE(SUM(is_repair_visit),0) AS repair_visit_tickets,
                   COALESCE(SUM(is_installation_visit),0) AS installation_visit_tickets,
                   COALESCE(SUM(is_field_visit),0) AS field_visit_tickets
            FROM {settings.ticket_facts_table}"""
        cursor.execute(f"{kpi_select} WHERE {where_sql}", params)
        cur_counts = {k: int(v or 0) for k, v in (cursor.fetchone() or {}).items()}
        cursor.execute(f"{kpi_select} WHERE {prev_where}", prev_params)
        prev_counts = {k: int(v or 0) for k, v in (cursor.fetchone() or {}).items()}

        cursor.execute(
            f"""SELECT event_date, COUNT(*) AS tickets,
                       SUM(is_repair_visit) AS repair_field,
                       SUM(is_installation_visit) AS install_field,
                       SUM(is_bot_resolved) AS bot_resolved
                FROM {settings.ticket_facts_table} WHERE {where_sql}
                GROUP BY event_date ORDER BY event_date""",
            params,
        )
        timeline_rows = cursor.fetchall()

        cursor.execute(
            f"""SELECT product_family, COUNT(*) AS tickets,
                       SUM(is_repair_visit) AS repair_visits, SUM(is_installation_visit) AS install_visits,
                       SUM(repeat_flag) AS repeat_tickets, SUM(is_bot_resolved) AS bot_resolved
                FROM {settings.ticket_facts_table} WHERE {where_sql}
                GROUP BY product_family ORDER BY tickets DESC""",
            params,
        )
        product_rows = cursor.fetchall()

        cursor.execute(
            f"""SELECT fault_code, COUNT(*) AS tickets,
                       SUM(is_repair_visit) AS repair_visits, SUM(repeat_flag) AS repeat_tickets
                FROM {settings.ticket_facts_table} WHERE {where_sql}
                GROUP BY fault_code ORDER BY tickets DESC LIMIT 20""",
            params,
        )
        category_rows = cursor.fetchall()

        cursor.execute(
            f"""SELECT fault_code, fault_code_level_2, COUNT(*) AS tickets,
                       SUM(is_repair_visit) AS repair_visits, SUM(repeat_flag) AS repeat_tickets,
                       SUM(is_bot_resolved) AS bot_resolved
                FROM {settings.ticket_facts_table} WHERE {where_sql}
                GROUP BY fault_code, fault_code_level_2 ORDER BY tickets DESC""",
            params,
        )
        fc2_rows = cursor.fetchall()
        cursor.close()
        connection.close()

        cat_total = sum(int(r.get("tickets", 0) or 0) for r in category_rows) or 1
        fc2_cat_totals: dict[str, int] = {}
        for row in fc2_rows:
            cat = row.get("fault_code") or "Unclassified"
            fc2_cat_totals[cat] = fc2_cat_totals.get(cat, 0) + int(row.get("tickets", 0) or 0)
        fc2_by_cat: dict[str, list[dict[str, Any]]] = {}
        for row in fc2_rows:
            cat = row.get("fault_code") or "Unclassified"
            count = int(row.get("tickets", 0) or 0)
            cat_vol = fc2_cat_totals.get(cat, 1) or 1
            fc2_by_cat.setdefault(cat, []).append({
                "label": row.get("fault_code_level_2") or "Unknown",
                "count": count,
                "share": count / cat_vol,
                "repair_rate": self._rate(row.get("repair_visits"), count),
                "repeat_rate": self._rate(row.get("repeat_tickets"), count),
                "bot_rate": self._rate(row.get("bot_resolved"), count),
            })

        return {
            "meta": {"dataset_scope": "semantic_v2"},
            "filters": asdict(filters),
            "period": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "days": days},
            "kpis": self._kpi_payload(cur_counts, prev_counts),
            "timeline": [
                {
                    "date": str(row["event_date"]),
                    "tickets": int(row.get("tickets", 0) or 0),
                    "repair_field": int(row.get("repair_field", 0) or 0),
                    "install_field": int(row.get("install_field", 0) or 0),
                    "bot_resolved": int(row.get("bot_resolved", 0) or 0),
                }
                for row in timeline_rows
            ],
            "products": [
                {
                    "product_family": row["product_family"],
                    "ticket_volume": int(row.get("tickets", 0) or 0),
                    "repair_field_visit_rate": self._rate(row.get("repair_visits"), row.get("tickets")),
                    "installation_field_visit_rate": self._rate(row.get("install_visits"), row.get("tickets")),
                    "repeat_rate": self._rate(row.get("repeat_tickets"), row.get("tickets")),
                    "bot_deflection_rate": self._rate(row.get("bot_resolved"), row.get("tickets")),
                }
                for row in product_rows
            ],
            "categories": [
                {
                    "label": row.get("fault_code") or "Unclassified",
                    "count": int(row.get("tickets", 0) or 0),
                    "share": self._rate(row.get("tickets"), cat_total),
                    "repair_rate": self._rate(row.get("repair_visits"), row.get("tickets")),
                    "repeat_rate": self._rate(row.get("repeat_tickets"), row.get("tickets")),
                }
                for row in category_rows
            ],
            "fc2_by_category": fc2_by_cat,
        }

    def issues(self, filters: DashboardFilters) -> dict[str, Any]:
        latest = self._latest_date()
        if latest is None:
            return {"meta": {"dataset_scope": "semantic_v2", "latest_date": None}, "filters": asdict(filters), "issue_views": {}}
        start_date = self._window_start(latest, filters.date_preset)
        # Split window in half for "rising" detection (recent half vs earlier half).
        window_days = (latest - start_date).days + 1
        mid_date = start_date + timedelta(days=window_days // 2)
        connection = self._repository.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        where_sql, params = self._base_where(filters, start_date, latest)
        cursor.execute(
            f"""
            SELECT product_family, fault_code, fault_code_level_1, fault_code_level_2,
                   COUNT(*) AS tickets,
                   SUM(CASE WHEN event_date >= %s THEN 1 ELSE 0 END) AS recent_tickets,
                   SUM(CASE WHEN event_date < %s THEN 1 ELSE 0 END) AS previous_tickets,
                   SUM(is_repair_visit) AS repair_visits,
                   SUM(is_installation_visit) AS install_visits,
                   SUM(repeat_flag) AS repeat_tickets,
                   SUM(is_bot_resolved) AS bot_resolved,
                   SUM(is_bot_transferred) AS bot_transferred
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
            GROUP BY product_family, fault_code, fault_code_level_1, fault_code_level_2
            HAVING COUNT(*) >= 8
            ORDER BY tickets DESC
            LIMIT 300
            """,
            (mid_date, mid_date) + params,
        )
        rows = cursor.fetchall()
        cursor.close()

        # Second pass: resolution breakdown per (product, fault_code, fc2)
        cursor2 = connection.cursor(dictionary=True)
        cursor2.execute(
            f"""
            SELECT product_family, fault_code, fault_code_level_2,
                   resolution_code_level_1 AS resolution, COUNT(*) AS cnt
            FROM {settings.ticket_facts_table}
            WHERE {where_sql}
              AND resolution_code_level_1 IS NOT NULL AND resolution_code_level_1 != ''
            GROUP BY product_family, fault_code, fault_code_level_2, resolution_code_level_1
            ORDER BY cnt DESC
            """,
            params,
        )
        res_rows = cursor2.fetchall()
        cursor2.close()
        connection.close()

        res_map: dict[tuple, list] = {}
        for r in res_rows:
            key = (r.get("product_family") or "", r.get("fault_code") or "", r.get("fault_code_level_2") or "")
            res_map.setdefault(key, []).append({"label": r.get("resolution") or "", "count": int(r.get("cnt") or 0)})

        return {
            "meta": {"dataset_scope": "semantic_v2", "latest_date": latest.isoformat()},
            "filters": asdict(filters),
            "issue_views": self._build_issue_views(rows, res_map),
        }

    def _build_issue_views(self, rows: list[dict[str, Any]], res_map: dict | None = None) -> dict[str, list[dict[str, Any]]]:
        def make_issue(row: dict[str, Any]) -> dict[str, Any]:
            tickets = int(row.get("tickets", 0) or 0)
            recent = int(row.get("recent_tickets", 0) or 0)
            previous = int(row.get("previous_tickets", 0) or 0)
            product = row.get("product_family") or ""
            fault_code = row.get("fault_code") or ""
            fc1 = row.get("fault_code_level_1") or ""
            fc2_raw = row.get("fault_code_level_2") or ""
            # Fall back to FC1 then FC when FC2 is unclassified/blank
            fc2 = fc2_raw if fc2_raw and fc2_raw != "Unclassified" else (fc1 if fc1 and fc1 != "Unclassified" else fault_code)
            top_resolutions = (res_map or {}).get((product, fault_code, fc2_raw), [])[:4]
            repair_rate = self._rate(row.get("repair_visits"), tickets)
            repeat_rate = self._rate(row.get("repeat_tickets"), tickets)
            bot_rate = self._rate(row.get("bot_resolved"), tickets)
            transfer_rate = self._rate(row.get("bot_transferred"), tickets)
            labels: list[str] = []
            if previous >= 5 and recent > previous * 1.2:
                labels.append("rising")
            if repair_rate >= 0.12:
                labels.append("repair-heavy")
            if repeat_rate >= 0.1:
                labels.append("repeat-heavy")
            if bot_rate >= 0.25:
                labels.append("bot-friendly")
            if transfer_rate >= 0.25:
                labels.append("agent-leaking")
            if not labels:
                labels.append("material")
            composite_risk = (
                recent * 0.05
                + max((recent - previous) / max(previous, 1), 0.0) * 20
                + repair_rate * 160
                + repeat_rate * 90
                + transfer_rate * 40
            )
            return {
                "issue_id": f"{product}|{fault_code}|{fc2}|Unknown",
                "product_family": product,
                "fault_code": fault_code,
                "fault_code_level_2": fc2,
                "software_version": "Unknown",
                "volume": recent,
                "previous_volume": previous,
                "repair_field_visit_rate": repair_rate,
                "installation_field_visit_rate": 0.0,
                "repeat_rate": repeat_rate,
                "bot_deflection_rate": bot_rate,
                "bot_transfer_rate": transfer_rate,
                "blank_chat_rate": 0.0,
                "logistics_rate": 0.0,
                "top_symptom": None,
                "top_defect": None,
                "top_repair": None,
                "insight": f"{fc2 or fault_code} is {' and '.join(labels)}.",
                "top_resolutions": top_resolutions,
                "_composite_risk": composite_risk,
            }

        issues = [make_issue(row) for row in rows]

        def payload(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [{k: v for k, v in iss.items() if k != "_composite_risk"} for iss in items[:6]]

        rising = sorted(
            [iss for iss in issues if iss["previous_volume"] >= 5],
            key=lambda iss: (iss["volume"] - iss["previous_volume"]) / max(iss["previous_volume"], 1),
            reverse=True,
        )
        return {
            "biggest_burden": payload(sorted(issues, key=lambda iss: iss["volume"], reverse=True)),
            "rising": payload(rising),
            "repair_heavy": payload(sorted(issues, key=lambda iss: iss["volume"] * iss["repair_field_visit_rate"], reverse=True)),
            "repeat_heavy": payload(sorted(issues, key=lambda iss: iss["volume"] * iss["repeat_rate"], reverse=True)),
            "bot_friendly": payload(sorted([iss for iss in issues if iss["bot_deflection_rate"] >= 0.15], key=lambda iss: iss["volume"] * iss["bot_deflection_rate"], reverse=True)),
            "agent_leakage": payload(sorted([iss for iss in issues if iss["bot_transfer_rate"] >= 0.1], key=lambda iss: iss["volume"] * iss["bot_transfer_rate"], reverse=True)),
        }
