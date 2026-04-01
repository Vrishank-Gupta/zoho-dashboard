from __future__ import annotations

from contextlib import closing
from datetime import date, timedelta
from typing import Any

from ..config import settings
from ..models import DashboardFilters


VERSION_PLACEHOLDER = "Not available in source"


class ClickHouseAnalyticsRepository:
    def _client(self):
        import clickhouse_connect

        return clickhouse_connect.get_client(
            host=settings.clickhouse.host,
            port=settings.clickhouse.port,
            username=settings.clickhouse.user,
            password=settings.clickhouse.password,
            database=settings.clickhouse.database,
            secure=settings.clickhouse.secure,
        )

    def _query(self, sql: str) -> list[dict[str, Any]]:
        with closing(self._client()) as client:
            result = client.query(sql)
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def ping(self) -> bool:
        try:
            return bool(self._query("SELECT 1 AS ok")[0]["ok"])
        except Exception:
            return False

    def fetch_max_metric_date(self) -> date | None:
        rows = self._query(f"SELECT max(metric_date) AS max_date FROM {settings.clickhouse_daily_summary_table}")
        return rows[0]["max_date"] if rows else None

    def fetch_daily_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
            metric_date,
            product_family,
            fault_code,
            fault_code_level_1,
            fault_code_level_2,
            department_name,
            channel,
            {self._quote(VERSION_PLACEHOLDER)} AS software_version,
            sum(tickets) AS tickets,
            if(sum(tickets) = 0, 0.0, sum(field_visit_tickets) / sum(tickets)) AS field_visit_rate,
            if(sum(tickets) = 0, 0.0, sum(repair_field_tickets) / sum(tickets)) AS repair_field_visit_rate,
            if(sum(tickets) = 0, 0.0, sum(installation_field_tickets) / sum(tickets)) AS installation_field_visit_rate,
            if(sum(tickets) = 0, 0.0, sum(bot_resolved_tickets) / sum(tickets)) AS bot_deflection_rate,
            if(sum(tickets) = 0, 0.0, sum(bot_transferred_tickets) / sum(tickets)) AS bot_transfer_rate,
            if(sum(tickets) = 0, 0.0, sum(blank_chat_tickets) / sum(tickets)) AS blank_chat_rate,
            if(sum(tickets) = 0, 0.0, sum(fcr_tickets) / sum(tickets)) AS fcr_rate,
            if(sum(tickets) = 0, 0.0, sum(repeat_tickets) / sum(tickets)) AS repeat_rate,
            if(sum(tickets) = 0, 0.0, sum(logistics_tickets) / sum(tickets)) AS logistics_rate,
            if(sum(handle_time_ticket_count) = 0, 0.0, sum(total_handle_time_minutes) / sum(handle_time_ticket_count) / 60.0) AS handle_time_hours,
            if(sum(tickets) = 0, 0.0, sum(young_device_tickets) / sum(tickets)) AS young_device_rate
        FROM {settings.clickhouse_daily_summary_table}
        WHERE {self._summary_filters(filters, start_date, end_date)}
        GROUP BY metric_date, product_family, fault_code, fault_code_level_1, fault_code_level_2, department_name, channel
        ORDER BY metric_date, product_family, fault_code, fault_code_level_1, fault_code_level_2
        """
        return self._query(sql)

    def fetch_issue_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
            toDate(toMonday(metric_date)) AS week_start,
            product_family,
            fault_code,
            fault_code_level_1,
            fault_code_level_2,
            {self._quote(VERSION_PLACEHOLDER)} AS software_version,
            sum(tickets) AS tickets,
            if(sum(tickets) = 0, 0.0, sum(repair_field_tickets) / sum(tickets)) AS repair_field_visit_rate,
            if(sum(tickets) = 0, 0.0, sum(installation_field_tickets) / sum(tickets)) AS installation_field_visit_rate,
            if(sum(tickets) = 0, 0.0, sum(repeat_tickets) / sum(tickets)) AS repeat_rate,
            if(sum(tickets) = 0, 0.0, sum(bot_resolved_tickets) / sum(tickets)) AS bot_deflection_rate,
            if(sum(tickets) = 0, 0.0, sum(bot_transferred_tickets) / sum(tickets)) AS bot_transfer_rate,
            if(sum(tickets) = 0, 0.0, sum(blank_chat_tickets) / sum(tickets)) AS blank_chat_rate,
            if(sum(tickets) = 0, 0.0, sum(fcr_tickets) / sum(tickets)) AS fcr_rate,
            if(sum(tickets) = 0, 0.0, sum(logistics_tickets) / sum(tickets)) AS logistics_rate,
            anyLast(top_symptom) AS top_symptom,
            anyLast(top_defect) AS top_defect,
            anyLast(top_repair) AS top_repair
        FROM {settings.clickhouse_issues_summary_table}
        WHERE {self._issue_filters(filters, start_date, end_date)}
        GROUP BY week_start, product_family, fault_code, fault_code_level_1, fault_code_level_2
        ORDER BY week_start, tickets DESC
        """
        return self._query(sql)

    def fetch_version_rows(self, end_date: date, filters: DashboardFilters) -> list[dict[str, Any]]:
        return []

    def fetch_resolution_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
            toDate(toStartOfMonth(created_date)) AS month_start,
            canonical_product AS product_family,
            normalized_resolution AS resolution_code_level_1,
            count() AS tickets,
            if(count() = 0, 0.0, countIf(is_fcr_success = 1) / count()) AS fcr_rate,
            if(count() = 0, 0.0, countIf(is_bot_resolved = 1) / count()) AS bot_deflection_rate,
            if(count() = 0, 0.0, countIf(is_bot_transferred = 1) / count()) AS bot_transfer_rate,
            if(count() = 0, 0.0, countIf(dropped_in_bot = 1) / count()) AS blank_chat_rate,
            if(count() = 0, 0.0, countIf(field_visit_type = 'Repair') / count()) AS repair_field_rate
        FROM {settings.clickhouse_fact_table} FINAL
        WHERE {self._fact_filters(filters, start_date, end_date)}
        GROUP BY month_start, product_family, resolution_code_level_1
        ORDER BY month_start, tickets DESC
        """
        return self._query(sql)

    def fetch_channel_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
            toDate(toStartOfMonth(metric_date)) AS month_start,
            channel,
            department_name,
            sum(tickets) AS tickets,
            if(sum(tickets) = 0, 0.0, sum(fcr_tickets) / sum(tickets)) AS fcr_rate,
            if(sum(tickets) = 0, 0.0, sum(bot_resolved_tickets) / sum(tickets)) AS bot_deflection_rate,
            if(sum(tickets) = 0, 0.0, sum(bot_transferred_tickets) / sum(tickets)) AS bot_transfer_rate,
            if(sum(tickets) = 0, 0.0, sum(blank_chat_tickets) / sum(tickets)) AS blank_chat_rate,
            if(sum(tickets) = 0, 0.0, sum(repair_field_tickets) / sum(tickets)) AS repair_field_rate,
            if(sum(handle_time_ticket_count) = 0, 0.0, sum(total_handle_time_minutes) / sum(handle_time_ticket_count) / 60.0) AS handle_time_hours
        FROM {settings.clickhouse_daily_summary_table}
        WHERE {self._summary_filters(filters, start_date, end_date)}
        GROUP BY month_start, channel, department_name
        ORDER BY month_start, tickets DESC
        """
        return self._query(sql)

    def fetch_bot_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
            canonical_product AS product_family,
            count() AS chat_tickets,
            countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
            countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
            countIf(dropped_in_bot = 1) AS blank_chat_tickets,
            countIf(blank_chat_returned_7d = 1 AND dropped_in_bot = 1) AS blank_chat_returned_7d,
            countIf(blank_chat_resolved_7d = 1 AND dropped_in_bot = 1) AS blank_chat_resolved_7d,
            countIf(blank_chat_transferred_7d = 1 AND dropped_in_bot = 1) AS blank_chat_transferred_7d,
            countIf(blank_chat_blank_again_7d = 1 AND dropped_in_bot = 1) AS blank_chat_blank_again_7d,
            if(countIf(dropped_in_bot = 1) = 0, 0.0, countIf(blank_chat_returned_7d = 1 AND dropped_in_bot = 1) / countIf(dropped_in_bot = 1)) AS blank_chat_return_rate,
            if(countIf(dropped_in_bot = 1) = 0, 0.0, countIf(blank_chat_resolved_7d = 1 AND dropped_in_bot = 1) / countIf(dropped_in_bot = 1)) AS blank_chat_recovery_rate,
            if(countIf(dropped_in_bot = 1) = 0, 0.0, countIf(blank_chat_blank_again_7d = 1 AND dropped_in_bot = 1) / countIf(dropped_in_bot = 1)) AS blank_chat_repeat_rate,
            if(count() = 0, 0.0, countIf(is_bot_resolved = 1) / count()) AS bot_resolved_rate,
            if(count() = 0, 0.0, countIf(is_bot_transferred = 1) / count()) AS bot_transferred_rate,
            if(count() = 0, 0.0, countIf(dropped_in_bot = 1) / count()) AS blank_chat_rate
        FROM {settings.clickhouse_fact_table} FINAL
        WHERE {self._fact_filters(filters, start_date, end_date)} AND normalized_channel = 'Chat'
        GROUP BY product_family
        ORDER BY chat_tickets DESC
        """
        rows = self._query(sql)
        total_chat = sum(int(row["chat_tickets"]) for row in rows)
        if total_chat:
            total_resolved = sum(int(row["bot_resolved_tickets"]) for row in rows)
            total_transferred = sum(int(row["bot_transferred_tickets"]) for row in rows)
            total_blank = sum(int(row["blank_chat_tickets"]) for row in rows)
            total_returned = sum(int(row["blank_chat_returned_7d"]) for row in rows)
            total_resolved_after_return = sum(int(row["blank_chat_resolved_7d"]) for row in rows)
            total_transferred_after_return = sum(int(row["blank_chat_transferred_7d"]) for row in rows)
            total_blank_again = sum(int(row["blank_chat_blank_again_7d"]) for row in rows)
            rows.insert(
                0,
                {
                    "product_family": "All Chat",
                    "chat_tickets": total_chat,
                    "bot_resolved_tickets": total_resolved,
                    "bot_transferred_tickets": total_transferred,
                    "blank_chat_tickets": total_blank,
                    "blank_chat_returned_7d": total_returned,
                    "blank_chat_resolved_7d": total_resolved_after_return,
                    "blank_chat_transferred_7d": total_transferred_after_return,
                    "blank_chat_blank_again_7d": total_blank_again,
                    "blank_chat_return_rate": total_returned / total_blank if total_blank else 0.0,
                    "blank_chat_recovery_rate": total_resolved_after_return / total_blank if total_blank else 0.0,
                    "blank_chat_repeat_rate": total_blank_again / total_blank if total_blank else 0.0,
                    "bot_resolved_rate": total_resolved / total_chat,
                    "bot_transferred_rate": total_transferred / total_chat,
                    "blank_chat_rate": total_blank / total_chat,
                },
            )
        return rows

    def fetch_data_quality_row(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
            max(metric_date) AS as_of_date,
            sum(tickets) AS total_tickets,
            sum(usable_issue_tickets) AS usable_issue_tickets,
            sum(actionable_issue_tickets) AS actionable_issue_tickets,
            sumIf(tickets, fault_code = 'Unclassified') AS blank_fault_code_tickets,
            sumIf(tickets, fault_code_level_1 = 'Unclassified') AS blank_fault_code_l1_tickets,
            sumIf(tickets, fault_code_level_2 = 'Unclassified') AS blank_fault_code_l2_tickets,
            sum(other_product_tickets) AS other_product_tickets,
            sum(hero_internal_tickets) AS hero_internal_tickets,
            sum(blank_chat_tickets) AS dropped_in_bot_tickets,
            sum(missing_issue_outside_bot_tickets) AS missing_issue_outside_bot_tickets,
            sum(dirty_channel_tickets) AS dirty_channel_tickets,
            sum(email_department_reassigned_tickets) AS email_department_reassigned_tickets
        FROM {settings.clickhouse_daily_summary_table}
        WHERE {self._summary_filters(filters, start_date, end_date)}
        """
        return self._query(sql)

    def fetch_pipeline_rows(self, limit: int = 5) -> list[dict[str, Any]]:
        sql = f"""
        SELECT
            started_at AS run_started_at,
            finished_at AS run_finished_at,
            dateDiff('minute', started_at, finished_at) AS duration_minutes,
            status,
            job_name,
            rows_fetched AS source_rows,
            message
        FROM {settings.clickhouse_run_log_table}
        ORDER BY started_at DESC
        LIMIT {int(limit)}
        """
        return self._query(sql)

    def fetch_issue_tickets(self, filters: DashboardFilters, issue_id: str, limit: int = 24) -> list[dict[str, Any]]:
        product, fault_code, fault_code_l1, fault_code_l2 = self._parse_issue_id(issue_id)
        clauses = [
            self._fact_filters(filters, None, None),
            f"canonical_product = {self._quote(product)}",
            f"normalized_fault_code = {self._quote(fault_code)}",
            f"normalized_fault_code_l1 = {self._quote(fault_code_l1)}",
            f"normalized_fault_code_l2 = {self._quote(fault_code_l2)}",
        ]
        sql = f"""
        SELECT
            ticket_id,
            created_at,
            canonical_product AS product_family,
            product,
            normalized_department AS department,
            normalized_channel AS channel,
            normalized_fault_code AS fault_code,
            normalized_fault_code_l1 AS fault_code_level_1,
            normalized_fault_code_l2 AS fault_code_level_2,
            normalized_resolution AS resolution,
            bot_action,
            {self._quote(VERSION_PLACEHOLDER)} AS software_version,
            status,
            symptom,
            defect,
            repair,
            device_serial_number,
            device_age_days
        FROM {settings.clickhouse_fact_table} FINAL
        WHERE {' AND '.join([clause for clause in clauses if clause])}
        ORDER BY created_at DESC
        LIMIT {int(limit)}
        """
        return self._query(sql)

    def search_tickets(self, filters: DashboardFilters, query: str, limit: int = 50) -> list[dict[str, Any]]:
        clauses = [self._fact_filters(filters, None, None)]
        query_text = query.strip()
        if query_text:
            like_sql = self._quote(f"%{query_text.lower()}%")
            clauses.append(
                "("
                f"positionCaseInsensitive(ticket_id, {self._quote(query_text)}) > 0 OR "
                f"lowerUTF8(ifNull(symptom, '')) LIKE {like_sql} OR "
                f"lowerUTF8(ifNull(defect, '')) LIKE {like_sql} OR "
                f"lowerUTF8(ifNull(repair, '')) LIKE {like_sql}"
                ")"
            )
        sql = f"""
        SELECT
            ticket_id,
            created_at,
            canonical_product AS product_family,
            normalized_department AS department,
            normalized_channel AS channel,
            normalized_fault_code AS fault_code,
            normalized_fault_code_l1 AS fault_code_level_1,
            normalized_fault_code_l2 AS fault_code_level_2,
            normalized_resolution AS resolution,
            {self._quote(VERSION_PLACEHOLDER)} AS software_version,
            status,
            symptom,
            defect,
            repair
        FROM {settings.clickhouse_fact_table} FINAL
        WHERE {' AND '.join([clause for clause in clauses if clause])}
        ORDER BY created_at DESC
        LIMIT {int(limit)}
        """
        return self._query(sql)

    def _summary_filters(
        self,
        filters: DashboardFilters,
        start_date: date | None,
        end_date: date | None,
        channel_override: str | None = None,
    ) -> str:
        clauses: list[str] = []
        if start_date and end_date:
            clauses.append(f"metric_date BETWEEN toDate({self._quote_date(start_date)}) AND toDate({self._quote_date(end_date)})")
        if filters.product != "All":
            clauses.append(f"product_family = {self._quote(filters.product)}")
        if filters.department != "All":
            clauses.append(f"department_name = {self._quote(filters.department)}")
        if filters.issue != "All":
            clauses.append(f"fault_code = {self._quote(filters.issue)}")
        if channel_override:
            clauses.append(f"channel = {self._quote(channel_override)}")
        return " AND ".join(clauses) if clauses else "1 = 1"

    def _issue_filters(self, filters: DashboardFilters, start_date: date, end_date: date) -> str:
        clauses = [f"metric_date BETWEEN toDate({self._quote_date(start_date)}) AND toDate({self._quote_date(end_date)})"]
        if filters.product != "All":
            clauses.append(f"product_family = {self._quote(filters.product)}")
        if filters.department != "All":
            clauses.append(f"department_name = {self._quote(filters.department)}")
        if filters.issue != "All":
            clauses.append(f"fault_code = {self._quote(filters.issue)}")
        return " AND ".join(clauses)

    def _fact_filters(self, filters: DashboardFilters, start_date: date | None, end_date: date | None) -> str:
        clauses: list[str] = []
        if start_date and end_date:
            clauses.append(f"created_date BETWEEN toDate({self._quote_date(start_date)}) AND toDate({self._quote_date(end_date)})")
        if filters.product != "All":
            clauses.append(f"canonical_product = {self._quote(filters.product)}")
        if filters.department != "All":
            clauses.append(f"normalized_department = {self._quote(filters.department)}")
        if filters.issue != "All":
            clauses.append(f"normalized_fault_code = {self._quote(filters.issue)}")
        return " AND ".join(clauses) if clauses else "1 = 1"

    def _parse_issue_id(self, issue_id: str) -> tuple[str, str, str, str]:
        parts = issue_id.split("|")
        if len(parts) != 4:
            raise ValueError(f"Invalid issue id: {issue_id}")
        return tuple(parts)  # type: ignore[return-value]

    def _quote(self, value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    def _quote_date(self, value: date) -> str:
        return self._quote(value.isoformat())
