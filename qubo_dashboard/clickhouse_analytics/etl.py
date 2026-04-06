from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
import traceback
from typing import Any
from zoneinfo import ZoneInfo

import mysql.connector

from ..config import settings
from ..models import TicketRecord
from ..pipeline.transforms import build_ticket_facts
from ..repository import clean_text, parse_datetime
from .schema import bootstrap_statements


EXTRACT_COLUMNS = [
    "Ticket_Id",
    "Created_Time",
    "Modified_Time",
    "Ticket_Closed_Time",
    "Department_Name",
    "Channel",
    "Email",
    "Mobile",
    "Phone",
    "Name",
    "Product",
    "Device_Model",
    "Fault_Code",
    "Fault_Code_Level_1",
    "Fault_Code_Level_2",
    "Resolution_Code_Level_1",
    "Bot_Action",
    "Status",
    "Device_Serial_Number",
    "Number_of_Reopen",
    "Symptom",
    "Defect",
    "Repair",
    "First_Commissioning_Date",
]


@dataclass(slots=True)
class ExtractedTicket:
    ticket: TicketRecord
    modified_at: datetime

    @property
    def bot_outcome(self) -> str:
        if self.ticket.is_bot_resolved:
            return "Bot resolved"
        if self.ticket.is_bot_transferred:
            return "Bot transferred"
        if self.ticket.is_blank_chat:
            return "Blank chat"
        return "Non-bot / Other"


@dataclass(slots=True)
class ETLResult:
    started_at: datetime
    finished_at: datetime
    rows_fetched: int
    rows_inserted: int
    affected_dates: set[str]
    last_sync_time: datetime | None
    message: str


class ClickHouseETLJob:
    def __init__(self) -> None:
        self._available_columns_cache: set[str] | None = None
        self._local_tz = ZoneInfo(settings.normalized_etl_timezone)

    def run(self) -> ETLResult:
        self.ensure_bootstrap()
        started_at = datetime.now(UTC)
        previous_sync = self.get_last_successful_sync()
        target_dates = self.compute_target_dates(previous_sync, started_at)

        rows_fetched = 0
        rows_inserted = 0
        affected_dates = {day.isoformat() for day in target_dates}
        last_sync_time: datetime | None = None

        try:
            if target_dates:
                self.clear_dates(target_dates)

            for target_date in target_dates:
                day_start, day_end = self.day_window(target_date)
                cursor_time = day_start - timedelta(seconds=1)
                cursor_ticket_id = ""
                while True:
                    batch = self.fetch_created_batch(
                        day_start=day_start,
                        day_end=day_end,
                        cursor_time=cursor_time,
                        cursor_ticket_id=cursor_ticket_id,
                        limit=settings.etl_batch_size,
                    )
                    if not batch:
                        break
                    rows_fetched += len(batch)
                    transformed = self.transform_rows(batch)
                    if transformed:
                        self.insert_fact_rows(transformed)
                        rows_inserted += len(transformed)
                        last_sync_time = max(item.modified_at for item in transformed)
                    cursor_time = parse_datetime(batch[-1].get(settings.zoho_created_column)) or day_end
                    cursor_ticket_id = str(batch[-1].get(settings.zoho_primary_key) or "")

            if target_dates:
                self.rebuild_summaries(sorted(affected_dates))

            finished_at = datetime.now(UTC)
            result = ETLResult(
                started_at=started_at,
                finished_at=finished_at,
                rows_fetched=rows_fetched,
                rows_inserted=rows_inserted,
                affected_dates=affected_dates,
                last_sync_time=last_sync_time or started_at,
                message=f"ETL complete. fetched={rows_fetched}, inserted={rows_inserted}, dates={len(affected_dates)}",
            )
            self.write_state("Success", result.last_sync_time, started_at, result.message)
            self.log_run(result, "Success", "")
            return result
        except Exception as exc:
            finished_at = datetime.now(UTC)
            result = ETLResult(
                started_at=started_at,
                finished_at=finished_at,
                rows_fetched=rows_fetched,
                rows_inserted=rows_inserted,
                affected_dates=affected_dates,
                last_sync_time=last_sync_time,
                message=str(exc),
            )
            self.write_state("Failed", previous_sync, started_at, str(exc))
            self.log_run(result, "Failed", traceback.format_exc())
            raise

    def ensure_bootstrap(self) -> None:
        with closing(self._clickhouse_client()) as client:
            for statement in bootstrap_statements():
                client.command(statement)

    def get_last_successful_sync(self) -> datetime | None:
        sql = f"""
        SELECT last_successful_sync
        FROM {settings.clickhouse_sync_state_table} FINAL
        WHERE pipeline_name = {self._quote(settings.etl_job_name)}
        ORDER BY updated_at DESC
        LIMIT 1
        """
        rows = self._query(sql)
        value = rows[0]["last_successful_sync"] if rows else None
        return self._ensure_utc(value) if value else None

    def compute_target_dates(self, previous_sync: datetime | None, now: datetime) -> list[date]:
        local_today = now.astimezone(self._local_tz).date()
        yesterday = local_today - timedelta(days=1)
        start_date = self._source_start_date()
        if yesterday < start_date:
            return []
        if previous_sync is None:
            days = (yesterday - start_date).days + 1
            return [start_date + timedelta(days=index) for index in range(days)]
        return [yesterday]

    def day_window(self, target_date: date) -> tuple[datetime, datetime]:
        start = datetime.combine(target_date, time.min)
        end = start + timedelta(days=1)
        return start, end

    def clear_dates(self, target_dates: list[date]) -> None:
        dates_sql = ", ".join(self._quote(day.isoformat()) for day in target_dates)
        with closing(self._clickhouse_client()) as client:
            client.command(
                f"ALTER TABLE {settings.clickhouse_fact_table} DELETE WHERE created_date IN ({dates_sql})",
                settings={"mutations_sync": 1},
            )
            client.command(
                f"ALTER TABLE {settings.clickhouse_daily_summary_table} DELETE WHERE metric_date IN ({dates_sql})",
                settings={"mutations_sync": 1},
            )
            client.command(
                f"ALTER TABLE {settings.clickhouse_issues_summary_table} DELETE WHERE metric_date IN ({dates_sql})",
                settings={"mutations_sync": 1},
            )

    def fetch_created_batch(
        self,
        day_start: datetime,
        day_end: datetime,
        cursor_time: datetime,
        cursor_ticket_id: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        connection = mysql.connector.connect(
            host=settings.zoho_db.host,
            port=settings.zoho_db.port,
            user=settings.zoho_db.user,
            password=settings.zoho_db.password,
            database=settings.zoho_db.database,
        )
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SET SESSION sql_mode = ''")
            available_columns = self.get_available_columns(connection)
            created_column = settings.zoho_created_column if settings.zoho_created_column in available_columns else "Created_Time"
            primary_key = settings.zoho_primary_key if settings.zoho_primary_key in available_columns else "Ticket_Id"
            select_parts = [
                column if column in available_columns else f"NULL AS {column}"
                for column in EXTRACT_COLUMNS
            ]
            query = f"""
            SELECT {", ".join(select_parts)}
            FROM {settings.zoho_ticket_table}
            WHERE {created_column} >= %s
              AND {created_column} < %s
              AND (
                ({created_column} > %s)
                OR ({created_column} = %s AND COALESCE({primary_key}, '') > %s)
              )
            ORDER BY {created_column}, {primary_key}
            LIMIT %s
            """
            cursor.execute(
                query,
                (
                    day_start.strftime("%Y-%m-%d %H:%M:%S"),
                    day_end.strftime("%Y-%m-%d %H:%M:%S"),
                    cursor_time.strftime("%Y-%m-%d %H:%M:%S"),
                    cursor_time.strftime("%Y-%m-%d %H:%M:%S"),
                    cursor_ticket_id,
                    int(limit),
                ),
            )
            return list(cursor.fetchall())
        finally:
            connection.close()

    def get_available_columns(self, connection) -> set[str]:
        if self._available_columns_cache is not None:
            return self._available_columns_cache
        cursor = connection.cursor()
        cursor.execute(f"SHOW COLUMNS FROM {settings.zoho_ticket_table}")
        self._available_columns_cache = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return self._available_columns_cache

    def transform_rows(self, rows: list[dict[str, Any]]) -> list[ExtractedTicket]:
        extracted: list[ExtractedTicket] = []
        for row in rows:
            created_at = parse_datetime(row.get("Created_Time"))
            if not created_at:
                continue
            ticket = TicketRecord(
                ticket_id=str(row.get("Ticket_Id") or ""),
                created_at=created_at,
                closed_at=parse_datetime(row.get("Ticket_Closed_Time")),
                department_name=clean_text(row.get("Department_Name")),
                channel=clean_text(row.get("Channel")),
                email=clean_text(row.get("Email")),
                mobile=clean_text(row.get("Mobile")),
                phone=clean_text(row.get("Phone")),
                name=clean_text(row.get("Name")),
                product=clean_text(row.get("Product")),
                device_model=clean_text(row.get("Device_Model")),
                fault_code=clean_text(row.get("Fault_Code")),
                fault_code_level_1=clean_text(row.get("Fault_Code_Level_1")),
                fault_code_level_2=clean_text(row.get("Fault_Code_Level_2")),
                resolution_code_level_1=clean_text(row.get("Resolution_Code_Level_1")),
                bot_action=clean_text(row.get("Bot_Action")),
                software_version=None,
                status=clean_text(row.get("Status")),
                device_serial_number=clean_text(row.get("Device_Serial_Number")),
                number_of_reopen=clean_text(row.get("Number_of_Reopen")),
                symptom=clean_text(row.get("Symptom")),
                defect=clean_text(row.get("Defect")),
                repair=clean_text(row.get("Repair")),
                first_commissioning_date=parse_datetime(row.get("First_Commissioning_Date")),
                raw={str(key): value for key, value in row.items()},
            )
            modified_at = self._ensure_utc(parse_datetime(row.get(settings.zoho_modified_column)) or created_at)
            extracted.append(ExtractedTicket(ticket=ticket, modified_at=modified_at))
        return extracted

    def insert_fact_rows(self, tickets: list[ExtractedTicket]) -> None:
        facts = {fact.ticket.ticket_id: fact for fact in build_ticket_facts([item.ticket for item in tickets])}
        now = datetime.now(UTC)
        columns = [
            "ticket_id", "source_updated_at", "ingest_version", "ingested_at", "created_at", "created_date",
            "closed_at", "department_name", "normalized_department", "channel", "normalized_channel",
            "customer_name", "email", "mobile", "phone", "product", "product_name", "device_model", "canonical_product",
            "product_category",
            "fault_code", "normalized_fault_code", "fault_code_level_1", "normalized_fault_code_l1",
            "fault_code_level_2", "normalized_fault_code_l2", "executive_fault_code",
            "resolution_code_level_1", "normalized_resolution",
            "bot_action", "normalized_bot_action", "bot_outcome", "status", "device_serial_number", "number_of_reopen",
            "symptom", "defect", "repair", "first_commissioning_date", "customer_key", "field_visit_type",
            "handle_time_minutes", "device_age_days", "is_core_product", "is_internal_hero", "is_field_service",
            "is_logistics", "is_bot_resolved", "is_bot_transferred", "is_blank_chat", "is_fcr_success",
            "repeat_flag", "usable_issue", "actionable_issue", "dropped_in_bot",
            "missing_issue_outside_bot", "dirty_channel", "reassigned_email_department", "blank_chat_returned_7d",
            "blank_chat_resolved_7d", "blank_chat_transferred_7d", "blank_chat_blank_again_7d",
        ]
        rows: list[list[Any]] = []
        for item in tickets:
            ticket = item.ticket
            fact = facts[ticket.ticket_id]
            rows.append(
                [
                    ticket.ticket_id,
                    item.modified_at,
                    int(item.modified_at.timestamp() * 1000),
                    now,
                    self._ensure_utc(ticket.created_at),
                    ticket.created_at.date(),
                    self._ensure_utc(ticket.closed_at) if ticket.closed_at else None,
                    ticket.department_name,
                    ticket.normalized_department,
                    ticket.channel,
                    ticket.normalized_channel,
                    ticket.name,
                    ticket.email,
                    ticket.mobile,
                    ticket.phone,
                    ticket.product,
                    ticket.product_name,
                    ticket.device_model,
                    ticket.canonical_product,
                    ticket.product_category,
                    ticket.fault_code,
                    ticket.normalized_fault_code,
                    ticket.fault_code_level_1,
                    ticket.normalized_fault_code_l1,
                    ticket.fault_code_level_2,
                    ticket.normalized_fault_code_l2,
                    ticket.executive_fault_code,
                    ticket.resolution_code_level_1,
                    ticket.normalized_resolution,
                    ticket.bot_action,
                    ticket.normalized_bot_action,
                    item.bot_outcome,
                    ticket.status,
                    ticket.device_serial_number,
                    ticket.number_of_reopen,
                    ticket.symptom,
                    ticket.defect,
                    ticket.repair,
                    self._ensure_utc(ticket.first_commissioning_date) if ticket.first_commissioning_date else None,
                    ticket.customer_key,
                    ticket.field_visit_type or "None",
                    float(ticket.handle_time_minutes) if ticket.handle_time_minutes is not None else None,
                    int(ticket.device_age_days) if ticket.device_age_days is not None else None,
                    int(ticket.is_core_product),
                    int(ticket.is_internal_hero),
                    int(ticket.is_field_service),
                    int(ticket.is_logistics),
                    int(ticket.is_bot_resolved),
                    int(ticket.is_bot_transferred),
                    int(ticket.is_blank_chat),
                    int(ticket.is_fcr_success),
                    int(fact.repeat_flag),
                    int(fact.usable_issue),
                    int(fact.actionable_issue),
                    int(fact.dropped_in_bot),
                    int(fact.missing_issue_outside_bot),
                    int(fact.dirty_channel),
                    int(fact.reassigned_email_department),
                    int(fact.blank_chat_returned_7d),
                    int(fact.blank_chat_resolved_7d),
                    int(fact.blank_chat_transferred_7d),
                    int(fact.blank_chat_blank_again_7d),
                ]
            )
        with closing(self._clickhouse_client()) as client:
            client.insert(settings.clickhouse_fact_table, rows, column_names=columns)

    def rebuild_summaries(self, affected_dates: list[str]) -> None:
        dates_sql = ", ".join(self._quote(day) for day in affected_dates)
        with closing(self._clickhouse_client()) as client:
            client.command(
                f"""
                INSERT INTO {settings.clickhouse_daily_summary_table}
                SELECT
                    created_date AS metric_date,
                    product_category,
                    product_name,
                    canonical_product AS product_family,
                    executive_fault_code,
                    normalized_fault_code AS fault_code,
                    normalized_fault_code_l1 AS fault_code_level_1,
                    normalized_fault_code_l2 AS fault_code_level_2,
                    normalized_department AS department_name,
                    normalized_channel AS channel,
                    normalized_bot_action,
                    bot_outcome,
                    ifNull(status, 'Unknown') AS status,
                    count() AS tickets,
                    countIf(is_field_service = 1) AS field_visit_tickets,
                    countIf(field_visit_type = 'Repair') AS repair_field_tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_field_tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(dropped_in_bot = 1) AS blank_chat_tickets,
                    countIf(is_fcr_success = 1) AS fcr_tickets,
                    countIf(repeat_flag = 1) AS repeat_tickets,
                    countIf(is_logistics = 1) AS logistics_tickets,
                    countIf(device_age_days <= 30) AS young_device_tickets,
                    countIf(usable_issue = 1) AS usable_issue_tickets,
                    countIf(actionable_issue = 1) AS actionable_issue_tickets,
                    countIf(canonical_product = 'Others') AS other_product_tickets,
                    countIf(is_internal_hero = 1) AS hero_internal_tickets,
                    countIf(missing_issue_outside_bot = 1) AS missing_issue_outside_bot_tickets,
                    countIf(dirty_channel = 1) AS dirty_channel_tickets,
                    countIf(reassigned_email_department = 1 AND normalized_channel = 'Email') AS email_department_reassigned_tickets,
                    sumIf(handle_time_minutes, handle_time_minutes IS NOT NULL) AS total_handle_time_minutes,
                    countIf(handle_time_minutes IS NOT NULL) AS handle_time_ticket_count
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE created_date IN ({dates_sql})
                GROUP BY metric_date, product_category, product_name, product_family, executive_fault_code, fault_code, fault_code_level_1, fault_code_level_2, department_name, channel, normalized_bot_action, bot_outcome, status
                """
            )
            client.command(
                f"""
                INSERT INTO {settings.clickhouse_issues_summary_table}
                SELECT
                    created_date AS metric_date,
                    product_category,
                    product_name,
                    canonical_product AS product_family,
                    executive_fault_code,
                    normalized_fault_code AS fault_code,
                    normalized_fault_code_l1 AS fault_code_level_1,
                    normalized_fault_code_l2 AS fault_code_level_2,
                    normalized_department AS department_name,
                    normalized_channel AS channel,
                    normalized_bot_action,
                    count() AS tickets,
                    countIf(field_visit_type = 'Repair') AS repair_field_tickets,
                    countIf(positionCaseInsensitive(normalized_fault_code_l1, 'instal') > 0 OR positionCaseInsensitive(normalized_fault_code_l2, 'instal') > 0) AS installation_field_tickets,
                    countIf(repeat_flag = 1) AS repeat_tickets,
                    countIf(is_bot_resolved = 1) AS bot_resolved_tickets,
                    countIf(is_bot_transferred = 1) AS bot_transferred_tickets,
                    countIf(dropped_in_bot = 1) AS blank_chat_tickets,
                    countIf(is_fcr_success = 1) AS fcr_tickets,
                    countIf(is_logistics = 1) AS logistics_tickets,
                    topK(1)(ifNull(symptom, 'Unknown'))[1] AS top_symptom,
                    topK(1)(ifNull(defect, 'Unknown'))[1] AS top_defect,
                    topK(1)(ifNull(repair, 'Unknown'))[1] AS top_repair
                FROM {settings.clickhouse_fact_table} FINAL
                WHERE created_date IN ({dates_sql}) AND usable_issue = 1
                GROUP BY metric_date, product_category, product_name, product_family, executive_fault_code, fault_code, fault_code_level_1, fault_code_level_2, department_name, channel, normalized_bot_action
                """
            )

    def write_state(self, status: str, last_sync: datetime | None, attempted_at: datetime, notes: str) -> None:
        with closing(self._clickhouse_client()) as client:
            client.insert(
                settings.clickhouse_sync_state_table,
                [[settings.etl_job_name, last_sync, attempted_at, datetime.now(UTC), status, notes]],
                column_names=[
                    "pipeline_name",
                    "last_successful_sync",
                    "last_attempted_sync",
                    "updated_at",
                    "status",
                    "notes",
                ],
            )

    def log_run(self, result: ETLResult, status: str, stacktrace: str) -> None:
        with closing(self._clickhouse_client()) as client:
            client.insert(
                settings.clickhouse_run_log_table,
                [[
                    settings.etl_job_name,
                    result.started_at,
                    result.finished_at,
                    status,
                    int(result.rows_fetched),
                    int(result.rows_inserted),
                    int(len(result.affected_dates)),
                    result.last_sync_time,
                    result.message,
                    stacktrace,
                ]],
                column_names=[
                    "job_name",
                    "started_at",
                    "finished_at",
                    "status",
                    "rows_fetched",
                    "rows_inserted",
                    "affected_dates",
                    "last_sync_time",
                    "message",
                    "stacktrace",
                ],
            )

    def _clickhouse_client(self):
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
        with closing(self._clickhouse_client()) as client:
            result = client.query(sql)
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def _quote(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def _source_start_date(self) -> date:
        return datetime.strptime(settings.source_start_date, "%Y-%m-%d").date()

    def _ensure_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=self._local_tz).astimezone(UTC)
        return value.astimezone(UTC)
