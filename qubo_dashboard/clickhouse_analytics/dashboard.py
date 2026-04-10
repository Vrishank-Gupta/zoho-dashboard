from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, timedelta

from ..config import settings
from ..models import DashboardFilters


class ClickHouseAnalyticsRepository:
    def __init__(self) -> None:
        self._metric_bounds_cache: tuple[datetime, tuple[date | None, date | None]] | None = None

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

    def _query(self, sql: str) -> list[dict]:
        with closing(self._client()) as client:
            result = client.query(sql)
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def ping(self) -> bool:
        try:
            return bool(self._query("SELECT 1 AS ok")[0]["ok"])
        except Exception:
            return False

    def _fetch_metric_bounds(self) -> tuple[date | None, date | None]:
        now = datetime.utcnow()
        if self._metric_bounds_cache and (now - self._metric_bounds_cache[0]).total_seconds() <= 120:
            return self._metric_bounds_cache[1]
        rows = self._query(
            f"""
            SELECT
                nullIf(min(metric_date), toDate('1970-01-01')) AS min_date,
                nullIf(max(metric_date), toDate('1970-01-01')) AS max_date
            FROM {settings.clickhouse_daily_summary_table}
            """
        )
        min_date = rows[0]["min_date"] if rows else None
        max_date = rows[0]["max_date"] if rows else None
        bounds = (min_date or None, max_date or None)
        self._metric_bounds_cache = (now, bounds)
        return bounds

    def fetch_max_metric_date(self) -> date | None:
        _, max_date = self._fetch_metric_bounds()
        return max_date

    def fetch_min_metric_date(self) -> date | None:
        min_date, _ = self._fetch_metric_bounds()
        return min_date

    def fetch_daily_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict]:
        sql = f"""
        SELECT
            metric_date,
            product_category,
            product_name,
            product_family,
            executive_fault_code,
            fault_code,
            fault_code_level_1,
            fault_code_level_2,
            department_name,
            channel,
            normalized_bot_action,
            bot_outcome,
            status,
            sum(tickets) AS tickets,
            sum(field_visit_tickets) AS field_visit_tickets,
            sum(repair_field_tickets) AS repair_field_tickets,
            sum(installation_field_tickets) AS installation_field_tickets,
            sum(bot_resolved_tickets) AS bot_resolved_tickets,
            sum(bot_transferred_tickets) AS bot_transferred_tickets,
            sum(blank_chat_tickets) AS blank_chat_tickets,
            sum(fcr_tickets) AS fcr_tickets,
            sum(repeat_tickets) AS repeat_tickets,
            sum(logistics_tickets) AS logistics_tickets,
            sum(young_device_tickets) AS young_device_tickets,
            sum(usable_issue_tickets) AS usable_issue_tickets,
            sum(actionable_issue_tickets) AS actionable_issue_tickets,
            sum(other_product_tickets) AS other_product_tickets,
            sum(hero_internal_tickets) AS hero_internal_tickets,
            sum(missing_issue_outside_bot_tickets) AS missing_issue_outside_bot_tickets,
            sum(dirty_channel_tickets) AS dirty_channel_tickets,
            sum(email_department_reassigned_tickets) AS email_department_reassigned_tickets,
            sum(total_handle_time_minutes) AS total_handle_time_minutes,
            sum(handle_time_ticket_count) AS handle_time_ticket_count
        FROM {settings.clickhouse_daily_summary_table}
        WHERE {self._summary_filters(filters, start_date, end_date)}
        GROUP BY
            metric_date,
            product_category,
            product_name,
            product_family,
            executive_fault_code,
            fault_code,
            fault_code_level_1,
            fault_code_level_2,
            department_name,
            channel,
            normalized_bot_action,
            bot_outcome,
            status
        ORDER BY metric_date, tickets DESC
        """
        return self._query(sql)

    def fetch_issue_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict]:
        sql = f"""
        SELECT
            metric_date,
            product_category,
            product_name,
            product_family,
            executive_fault_code,
            fault_code,
            fault_code_level_1,
            fault_code_level_2,
            department_name,
            channel,
            normalized_bot_action,
            sum(tickets) AS tickets,
            sum(repair_field_tickets) AS repair_field_tickets,
            sum(installation_field_tickets) AS installation_field_tickets,
            sum(repeat_tickets) AS repeat_tickets,
            sum(bot_resolved_tickets) AS bot_resolved_tickets,
            sum(bot_transferred_tickets) AS bot_transferred_tickets,
            sum(blank_chat_tickets) AS blank_chat_tickets,
            sum(fcr_tickets) AS fcr_tickets,
            sum(logistics_tickets) AS logistics_tickets,
            anyLast(top_symptom) AS top_symptom,
            anyLast(top_defect) AS top_defect,
            anyLast(top_repair) AS top_repair
        FROM {settings.clickhouse_issues_summary_table}
        WHERE {self._issue_filters(filters, start_date, end_date)}
        GROUP BY
            metric_date,
            product_category,
            product_name,
            product_family,
            executive_fault_code,
            fault_code,
            fault_code_level_1,
            fault_code_level_2,
            department_name,
            channel,
            normalized_bot_action
        ORDER BY metric_date, tickets DESC
        """
        return self._query(sql)

    def fetch_bot_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict]:
        sql = f"""
        SELECT
            product_category,
            product_name,
            canonical_product AS product_family,
            count() AS chat_tickets,
            countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
            countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
            countIf(dropped_in_bot = 1) AS blank_chat_tickets,
            countIf(blank_chat_returned_7d = 1 AND dropped_in_bot = 1) AS blank_chat_returned_7d,
            countIf(blank_chat_resolved_7d = 1 AND dropped_in_bot = 1) AS blank_chat_resolved_7d,
            countIf(blank_chat_transferred_7d = 1 AND dropped_in_bot = 1) AS blank_chat_transferred_7d,
            countIf(blank_chat_blank_again_7d = 1 AND dropped_in_bot = 1) AS blank_chat_blank_again_7d
        FROM {settings.clickhouse_fact_table} FINAL
        WHERE {self._fact_filters(filters, start_date, end_date)} AND normalized_channel = 'Chat'
        GROUP BY product_category, product_name, product_family
        ORDER BY chat_tickets DESC
        """
        return self._query(sql)

    def fetch_repeat_rows(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict]:
        sql = f"""
        SELECT
            return_created_date AS metric_date,
            product_category,
            product_name,
            product_family,
            aging_bucket,
            days_to_return,
            first_executive_fault_code,
            first_fault_code_level_1,
            first_fault_code_level_2,
            return_executive_fault_code,
            return_fault_code_level_1,
            return_fault_code_level_2,
            first_resolution,
            return_resolution,
            first_channel,
            return_channel,
            first_bot_action,
            return_bot_action,
            same_efc,
            same_fc2,
            count() AS repeat_returns
        FROM {settings.clickhouse_repeat_events_table}
        WHERE {self._repeat_filters(filters, start_date, end_date)}
        GROUP BY
            metric_date,
            product_category,
            product_name,
            product_family,
            aging_bucket,
            days_to_return,
            first_executive_fault_code,
            first_fault_code_level_1,
            first_fault_code_level_2,
            return_executive_fault_code,
            return_fault_code_level_1,
            return_fault_code_level_2,
            first_resolution,
            return_resolution,
            first_channel,
            return_channel,
            first_bot_action,
            return_bot_action,
            same_efc,
            same_fc2
        ORDER BY metric_date, repeat_returns DESC
        """
        return self._query(sql)

    def fetch_product_drilldown(self, filters: DashboardFilters, category: str, product_name: str) -> dict[str, list[dict]]:
        current_start, current_end = self._resolve_date_range(filters)
        previous_start, previous_end = self._previous_period(current_start, current_end)
        clauses = [
            self._fact_filters(filters, None, None),
            f"product_name = {self._quote(product_name)}",
        ]
        where_sql = " AND ".join([clause for clause in clauses if clause])
        return {
            "summary": self._query(
                f"""
                SELECT
                    count() AS tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(normalized_bot_action = 'Blank chat') AS blank_chat_tickets,
                    countIf(match(lower(ifNull(status, '')), 'open|escal|pending|progress|wip')) AS open_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                """
            ),
            "summary_previous": self._query(
                f"""
                SELECT
                    count() AS tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(normalized_bot_action = 'Blank chat') AS blank_chat_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {self._fact_filters(filters, previous_start, previous_end)} AND product_name = {self._quote(product_name)}
                """
            ),
            "timeline": self._query(
                f"""
                SELECT
                    created_date AS metric_date,
                    count() AS tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY metric_date
                ORDER BY metric_date
                """
            ),
            "bot_actions": self._query(
                f"""
                SELECT normalized_bot_action AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
              "resolutions": self._query(
                  f"""
                  SELECT normalized_resolution AS label, count() AS tickets
                  FROM {settings.clickhouse_fact_table} FINAL
                  WHERE {where_sql}
                  GROUP BY label
                  ORDER BY tickets DESC
                  """
              ),
            "statuses": self._query(
                f"""
                SELECT ifNull(status, 'Unknown') AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
              "efcs": self._query(
                  f"""
                  SELECT executive_fault_code AS label, count() AS tickets
                  FROM {settings.clickhouse_fact_table} FINAL
                  WHERE {where_sql}
                  GROUP BY label
                  ORDER BY tickets DESC
                  """
              ),
              "fc1": self._query(
                  f"""
                  SELECT normalized_fault_code_l1 AS label, count() AS tickets
                  FROM {settings.clickhouse_fact_table} FINAL
                  WHERE {where_sql}
                  GROUP BY label
                  ORDER BY tickets DESC
                  """
              ),
              "fc2": self._query(
                  f"""
                  SELECT normalized_fault_code_l2 AS label, count() AS tickets
                  FROM {settings.clickhouse_fact_table} FINAL
                  WHERE {where_sql}
                  GROUP BY label
                  ORDER BY tickets DESC
                  """
              ),
            "issue_matrix": self._query(
                f"""
                SELECT
                    executive_fault_code,
                    normalized_fault_code_l2 AS issue_detail,
                    count() AS tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY executive_fault_code, issue_detail
                ORDER BY tickets DESC
                LIMIT 15
                """
            ),
            "issue_daily": self._query(
                f"""
                SELECT
                    created_date AS metric_date,
                    executive_fault_code,
                    normalized_fault_code_l1 AS fault_code_level_1,
                    normalized_fault_code_l2 AS issue_detail,
                    normalized_resolution AS resolution,
                    count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY metric_date, executive_fault_code, fault_code_level_1, issue_detail, resolution
                ORDER BY metric_date, tickets DESC
                """
            ),
        }

    def fetch_category_drilldown(self, filters: DashboardFilters, category: str, product_names: list[str]) -> dict[str, list[dict]]:
        if not product_names:
            return {
                "summary": [],
                "summary_previous": [],
                "timeline": [],
                "products": [],
                "products_previous": [],
                "bot_actions": [],
                "resolutions": [],
                "statuses": [],
                "efcs": [],
                "fc1": [],
                "issues": [],
                "resolution_by_product": [],
            }
        current_start, current_end = self._resolve_date_range(filters)
        previous_start, previous_end = self._previous_period(current_start, current_end)
        clauses = [
            self._fact_filters(filters, None, None),
            f"product_name IN ({self._quote_join(product_names)})",
        ]
        where_sql = " AND ".join([clause for clause in clauses if clause])
        return {
            "summary": self._query(
                f"""
                SELECT
                    count() AS tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(normalized_bot_action = 'Blank chat') AS blank_chat_tickets,
                    countIf(match(lower(ifNull(status, '')), 'open|escal|pending|progress|wip')) AS open_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                """
            ),
            "summary_previous": self._query(
                f"""
                SELECT
                    count() AS tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(normalized_bot_action = 'Blank chat') AS blank_chat_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {self._fact_filters(filters, previous_start, previous_end)} AND product_name IN ({self._quote_join(product_names)})
                """
            ),
            "timeline": self._query(
                f"""
                SELECT
                    created_date AS metric_date,
                    count() AS tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY metric_date
                ORDER BY metric_date
                """
            ),
              "products": self._query(
                  f"""
                  SELECT
                      product_name AS label,
                    count() AS tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(match(lower(ifNull(status, '')), 'open|escal|pending|progress|wip')) AS open_tickets
                  FROM {settings.clickhouse_fact_table} FINAL
                  WHERE {where_sql}
                  GROUP BY label
                  ORDER BY tickets DESC
                  """
              ),
            "products_previous": self._query(
                f"""
                SELECT
                    product_name AS label,
                    count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {self._fact_filters(filters, previous_start, previous_end)} AND product_name IN ({self._quote_join(product_names)})
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
            "bot_actions": self._query(
                f"""
                SELECT normalized_bot_action AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
              "resolutions": self._query(
                  f"""
                  SELECT normalized_resolution AS label, count() AS tickets
                  FROM {settings.clickhouse_fact_table} FINAL
                  WHERE {where_sql}
                  GROUP BY label
                  ORDER BY tickets DESC
                  """
              ),
            "statuses": self._query(
                f"""
                SELECT ifNull(status, 'Unknown') AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
              "efcs": self._query(
                  f"""
                  SELECT executive_fault_code AS label, count() AS tickets
                  FROM {settings.clickhouse_fact_table} FINAL
                  WHERE {where_sql}
                  GROUP BY label
                  ORDER BY tickets DESC
                  """
              ),
            "fc1": self._query(
                f"""
                SELECT normalized_fault_code_l1 AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
            "issues": self._query(
                f"""
                SELECT
                    executive_fault_code,
                    count() AS tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY executive_fault_code
                ORDER BY tickets DESC
                LIMIT 15
                """
            ),
            "resolution_by_product": self._query(
                f"""
                SELECT
                    product_name,
                    normalized_resolution AS resolution,
                    count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY product_name, resolution
                ORDER BY tickets DESC
                LIMIT 18
                """
            ),
            "product_fault_daily": self._query(
                f"""
                SELECT
                    created_date AS metric_date,
                    product_name,
                    executive_fault_code,
                    normalized_fault_code_l1 AS fault_code_level_1,
                    normalized_fault_code_l2 AS fault_code_level_2,
                    normalized_resolution AS resolution,
                    count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY metric_date, product_name, executive_fault_code, fault_code_level_1, fault_code_level_2, resolution
                ORDER BY product_name, metric_date, tickets DESC
                """
            ),
        }

    def fetch_issue_drilldown(self, filters: DashboardFilters, issue_id: str) -> dict[str, list[dict]]:
        category, product_name, efc, issue_detail = self._parse_issue_id(issue_id)
        current_start, current_end = self._resolve_date_range(filters)
        previous_start, previous_end = self._previous_period(current_start, current_end)
        clauses = [
            self._fact_filters(filters, None, None),
            f"product_name = {self._quote(product_name)}",
            f"normalized_fault_code_l2 = {self._quote(issue_detail)}",
        ]
        where_sql = " AND ".join([clause for clause in clauses if clause])
        return {
            "summary": self._query(
                f"""
                SELECT
                    count() AS tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(normalized_bot_action = 'Blank chat') AS blank_chat_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                """
            ),
            "summary_previous": self._query(
                f"""
                SELECT
                    count() AS tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(normalized_bot_action = 'Blank chat') AS blank_chat_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {self._fact_filters(filters, previous_start, previous_end)} AND product_name = {self._quote(product_name)} AND normalized_fault_code_l2 = {self._quote(issue_detail)}
                """
            ),
            "timeline": self._query(
                f"""
                SELECT
                    created_date AS metric_date,
                    count() AS tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY metric_date
                ORDER BY metric_date
                """
            ),
            "bot_actions": self._query(
                f"""
                SELECT normalized_bot_action AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
            "resolutions": self._query(
                f"""
                SELECT normalized_resolution AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                LIMIT 12
                """
            ),
            "statuses": self._query(
                f"""
                SELECT ifNull(status, 'Unknown') AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
            "departments": self._query(
                f"""
                SELECT normalized_department AS label, count() AS tickets
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE {where_sql}
                GROUP BY label
                ORDER BY tickets DESC
                """
            ),
        }

    def fetch_data_quality_row(self, start_date: date, end_date: date, filters: DashboardFilters) -> list[dict]:
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

    def fetch_pipeline_rows(self, limit: int = 5) -> list[dict]:
        sql = f"""
        SELECT
            started_at AS run_started_at,
            finished_at AS run_finished_at,
            dateDiff('minute', started_at, finished_at) AS duration_minutes,
            status,
            job_name,
            rows_fetched AS source_rows,
            rows_inserted,
            message
        FROM {settings.clickhouse_run_log_table}
        ORDER BY started_at DESC
        LIMIT {int(limit)}
        """
        return self._query(sql)

    def fetch_issue_tickets(self, filters: DashboardFilters, issue_id: str, limit: int = 24) -> list[dict]:
        category, product, efc, issue_detail = self._parse_issue_id(issue_id)
        clauses = [
            self._fact_filters(filters, None, None),
            f"product_name = {self._quote(product)}",
            f"normalized_fault_code_l2 = {self._quote(issue_detail)}",
        ]
        sql = f"""
        SELECT
            ticket_id,
            created_at,
            product_category,
            product_name,
            canonical_product AS product_family,
            product,
            normalized_department AS department,
            normalized_channel AS channel,
            executive_fault_code,
            normalized_fault_code AS fault_code,
            normalized_fault_code_l1 AS fault_code_level_1,
            normalized_fault_code_l2 AS fault_code_level_2,
            normalized_resolution AS resolution,
            bot_action,
            status,
            symptom,
            defect,
            repair,
            device_serial_number
        FROM {settings.clickhouse_fact_table} FINAL
        WHERE {" AND ".join([clause for clause in clauses if clause])}
        ORDER BY created_at DESC
        LIMIT {int(limit)}
        """
        return self._query(sql)

    def search_tickets(self, filters: DashboardFilters, query: str, limit: int = 50) -> list[dict]:
        clauses = [self._fact_filters(filters, None, None)]
        query_text = query.strip()
        if query_text:
            like_sql = self._quote(f"%{query_text.lower()}%")
            clauses.append(
                "("
                f"positionCaseInsensitive(ticket_id, {self._quote(query_text)}) > 0 OR "
                f"lowerUTF8(ifNull(product, '')) LIKE {like_sql} OR "
                f"lowerUTF8(ifNull(symptom, '')) LIKE {like_sql} OR "
                f"lowerUTF8(ifNull(defect, '')) LIKE {like_sql} OR "
                f"lowerUTF8(ifNull(repair, '')) LIKE {like_sql}"
                ")"
            )
        sql = f"""
        SELECT
            ticket_id,
            created_at,
            product_category,
            canonical_product AS product_family,
            normalized_department AS department,
            normalized_channel AS channel,
            executive_fault_code,
            normalized_fault_code AS fault_code,
            normalized_fault_code_l1 AS fault_code_level_1,
            normalized_fault_code_l2 AS fault_code_level_2,
            normalized_resolution AS resolution,
            status,
            symptom,
            defect,
            repair
        FROM {settings.clickhouse_fact_table} FINAL
        WHERE {" AND ".join([clause for clause in clauses if clause])}
        ORDER BY created_at DESC
        LIMIT {int(limit)}
        """
        return self._query(sql)

    def _summary_filters(self, filters: DashboardFilters, start_date: date | None, end_date: date | None) -> str:
        clauses: list[str] = []
        date_start, date_end = (start_date, end_date) if start_date and end_date else self._resolve_date_range(filters)
        clauses.append(f"metric_date BETWEEN toDate({self._quote_date(date_start)}) AND toDate({self._quote_date(date_end)})")
        clauses.extend(self._in_filter("product_name", filters.products))
        clauses.extend(self._in_filter("department_name", filters.departments))
        clauses.extend(self._in_filter("channel", filters.channels))
        clauses.extend(self._in_filter("fault_code_level_2", filters.issue_details))
        clauses.extend(self._in_filter("status", filters.statuses))
        clauses.extend(self._in_filter("normalized_bot_action", filters.bot_actions))
        clauses.extend(self._multi_filters(
            "fault_code_level_1",
            filters.include_fc1,
            filters.exclude_fc1,
        ))
        clauses.extend(self._multi_filters(
            "fault_code_level_2",
            filters.include_fc2,
            filters.exclude_fc2,
        ))
        clauses.extend(self._multi_filters(
            "normalized_bot_action",
            filters.include_bot_action,
            filters.exclude_bot_action,
        ))
        if filters.exclude_installation:
            clauses.append(
                self._exclude_non_product_issue_clause(
                    fc1_col="fault_code_level_1",
                    fc2_col="fault_code_level_2",
                    efc_col="executive_fault_code",
                    product_category_col="product_category",
                )
            )
        if filters.exclude_blank_chat:
            clauses.append("normalized_bot_action != 'Blank chat'")
        if filters.exclude_unclassified_blank:
            clauses.append("lowerUTF8(trim(BOTH ' ' FROM product_name)) NOT IN ('blank product', 'blankproduct', '-')")
            clauses.append("lowerUTF8(trim(BOTH ' ' FROM executive_fault_code)) NOT IN ('blank', 'unclassified')")
        return " AND ".join(clauses) if clauses else "1 = 1"

    def _issue_filters(self, filters: DashboardFilters, start_date: date | None, end_date: date | None) -> str:
        return self._summary_filters(filters, start_date, end_date)

    def _fact_filters(self, filters: DashboardFilters, start_date: date | None, end_date: date | None) -> str:
        clauses: list[str] = []
        date_start, date_end = (start_date, end_date) if start_date and end_date else self._resolve_date_range(filters)
        clauses.append(f"created_date BETWEEN toDate({self._quote_date(date_start)}) AND toDate({self._quote_date(date_end)})")
        clauses.extend(self._in_filter("product_name", filters.products))
        clauses.extend(self._in_filter("normalized_department", filters.departments))
        clauses.extend(self._in_filter("normalized_channel", filters.channels))
        clauses.extend(self._in_filter("normalized_fault_code_l2", filters.issue_details))
        clauses.extend(self._in_filter("ifNull(status, 'Unknown')", filters.statuses))
        clauses.extend(self._in_filter("normalized_bot_action", filters.bot_actions))
        clauses.extend(self._multi_filters(
            "normalized_fault_code_l1",
            filters.include_fc1,
            filters.exclude_fc1,
        ))
        clauses.extend(self._multi_filters(
            "normalized_fault_code_l2",
            filters.include_fc2,
            filters.exclude_fc2,
        ))
        clauses.extend(self._multi_filters(
            "normalized_bot_action",
            filters.include_bot_action,
            filters.exclude_bot_action,
        ))
        if filters.exclude_installation:
            clauses.append(
                self._exclude_non_product_issue_clause(
                    fc1_col="normalized_fault_code_l1",
                    fc2_col="normalized_fault_code_l2",
                    efc_col="executive_fault_code",
                    product_category_col="product_category",
                )
            )
        if filters.exclude_blank_chat:
            clauses.append("normalized_bot_action != 'Blank chat'")
        if filters.exclude_unclassified_blank:
            clauses.append("lowerUTF8(trim(BOTH ' ' FROM product_name)) NOT IN ('blank product', 'blankproduct', '-')")
            clauses.append("lowerUTF8(trim(BOTH ' ' FROM executive_fault_code)) NOT IN ('blank', 'unclassified')")
        return " AND ".join(clauses) if clauses else "1 = 1"

    def _repeat_filters(self, filters: DashboardFilters, start_date: date | None, end_date: date | None) -> str:
        clauses: list[str] = []
        date_start, date_end = (start_date, end_date) if start_date and end_date else self._resolve_date_range(filters)
        clauses.append(f"return_created_date BETWEEN toDate({self._quote_date(date_start)}) AND toDate({self._quote_date(date_end)})")
        clauses.extend(self._in_filter("product_category", filters.categories))
        clauses.extend(self._in_filter("product_name", filters.products))
        clauses.extend(self._in_filter("return_executive_fault_code", filters.efcs))
        clauses.extend(self._in_filter("return_fault_code_level_2", filters.issue_details))
        clauses.extend(self._in_filter("return_channel", filters.channels))
        clauses.extend(self._in_filter("return_bot_action", filters.bot_actions))
        clauses.extend(self._multi_filters(
            "return_fault_code_level_1",
            filters.include_fc1,
            filters.exclude_fc1,
        ))
        clauses.extend(self._multi_filters(
            "return_fault_code_level_2",
            filters.include_fc2,
            filters.exclude_fc2,
        ))
        clauses.extend(self._multi_filters(
            "return_bot_action",
            filters.include_bot_action,
            filters.exclude_bot_action,
        ))
        if filters.exclude_installation:
            clauses.append(
                self._exclude_non_product_issue_clause(
                    fc1_col="return_fault_code_level_1",
                    fc2_col="return_fault_code_level_2",
                    efc_col="return_executive_fault_code",
                    product_category_col="product_category",
                )
            )
        if filters.exclude_blank_chat:
            clauses.append("return_bot_action != 'Blank chat'")
        if filters.exclude_unclassified_blank:
            clauses.append("lowerUTF8(trim(BOTH ' ' FROM product_name)) NOT IN ('blank product', 'blankproduct', '-')")
            clauses.append("lowerUTF8(trim(BOTH ' ' FROM return_executive_fault_code)) NOT IN ('blank', 'unclassified')")
        return " AND ".join(clauses) if clauses else "1 = 1"

    def _multi_filters(self, column: str, include_values: list[str], exclude_values: list[str]) -> list[str]:
        clauses: list[str] = []
        include_values = [value for value in include_values if value]
        exclude_values = [value for value in exclude_values if value]
        if include_values:
            clauses.append(f"{column} IN ({self._quote_join(include_values)})")
        if exclude_values:
            clauses.append(f"{column} NOT IN ({self._quote_join(exclude_values)})")
        return clauses

    def _in_filter(self, column: str, values: list[str]) -> list[str]:
        filtered = [value for value in values if value]
        if not filtered:
            return []
        return [f"{column} IN ({self._quote_join(filtered)})"]

    def _exclude_non_product_issue_clause(
        self,
        *,
        fc1_col: str,
        fc2_col: str,
        efc_col: str,
        product_category_col: str,
    ) -> str:
        combined = (
            f"lowerUTF8(concat(ifNull({fc1_col}, ''), ' ', ifNull({fc2_col}, ''), ' ', ifNull({efc_col}, '')))"
        )
        business_noise_terms = [
            "instal",
            "sales",
            "marketing",
            "logistic",
            "order",
            "fulfilment",
            "fulfillment",
            "delivery",
            "shipment",
            "subscription",
            "billing",
            "monetisation",
            "monetization",
            "enquiry",
            "inquiry",
            "pre-purchase",
            "pre purchase",
            "prepurchase",
            "non product",
            "non-product",
        ]
        keyword_checks = " AND ".join(
            [f"positionCaseInsensitive({combined}, {self._quote(term)}) = 0" for term in business_noise_terms]
        )
        return (
            "("
            f"lowerUTF8(trim(BOTH ' ' FROM ifNull({product_category_col}, ''))) NOT IN ('others', 'other', 'blank product', 'blankproduct', '-')"
            f" AND {keyword_checks}"
            ")"
        )

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

    def _quote_join(self, values: list[str]) -> str:
        return ", ".join(self._quote(value) for value in values)

    def _resolve_date_range(self, filters: DashboardFilters) -> tuple[date, date]:
        max_date = self.fetch_max_metric_date()
        min_date = self.fetch_min_metric_date()
        if not max_date or not min_date:
            today = datetime.utcnow().date()
            return today - timedelta(days=59), today
        start_date = self._parse_date(filters.date_start) or max(min_date, max_date - timedelta(days=59))
        end_date = self._parse_date(filters.date_end) or max_date
        if start_date < min_date:
            start_date = min_date
        if end_date > max_date:
            end_date = max_date
        if start_date > end_date:
            start_date = end_date
        return start_date, end_date

    def _previous_period(self, start_date: date, end_date: date) -> tuple[date, date]:
        span_days = max(0, (end_date - start_date).days)
        previous_end = start_date - timedelta(days=1)
        previous_start = previous_end - timedelta(days=span_days)
        return previous_start, previous_end

    def _parse_date(self, value: str | None) -> date | None:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
