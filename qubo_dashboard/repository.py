from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .config import settings
from .models import TicketRecord


class TicketRepository:
    def __init__(self) -> None:
        self._zoho_columns_cache: set[str] | None = None

    def fetch_tickets(self, since: datetime | None = None) -> list[TicketRecord]:
        if not settings.has_zoho_database:
            raise RuntimeError("Remote Zoho DB is not configured.")
        return self._fetch_zoho_mysql(since=since)

    def fetch_tickets_strict(self, since: datetime | None = None) -> list[TicketRecord]:
        return self.fetch_tickets(since=since)

    def fetch_issue_tickets(
        self,
        product_family: str,
        fault_code: str,
        fault_code_level_2: str,
        software_version: str,
        limit: int = 24,
    ) -> list[TicketRecord]:
        if not settings.has_zoho_database:
            return []
        try:
            return self._fetch_issue_tickets_from_zoho(
                product_family=product_family,
                fault_code=fault_code,
                fault_code_level_2=fault_code_level_2,
                software_version=software_version,
                limit=limit,
            )
        except Exception:
            return []

    def get_connection_status(self) -> dict[str, Any]:
        return {
            "zoho_configured": settings.has_zoho_database,
            "agg_configured": settings.has_agg_database,
            "zoho_ticket_table": settings.zoho_ticket_table,
            "raw_ticket_cache_table": settings.raw_ticket_cache_table,
            "api_snapshot_cache_table": settings.api_snapshot_cache_table,
            "agg_tables": {
                "agg_daily_tickets": settings.agg_daily_tickets_table,
                "agg_fc_weekly": settings.agg_fc_weekly_table,
                "agg_sw_version": settings.agg_sw_version_table,
                "agg_resolution": settings.agg_resolution_table,
                "agg_channel": settings.agg_channel_table,
                "agg_bot": settings.agg_bot_table,
                "agg_hourly_heatmap": settings.agg_hourly_heatmap_table,
                "agg_replacements": settings.agg_replacements_table,
                "agg_voc_mismatch": settings.agg_voc_mismatch_table,
                "agg_anomalies": settings.agg_anomalies_table,
                "agg_health_score": settings.agg_health_score_table,
                "agg_data_quality": settings.agg_data_quality_table,
                "api_snapshot_cache": settings.api_snapshot_cache_table,
                "ticket_facts": settings.ticket_facts_table,
                "fact_daily_overview": settings.fact_daily_overview_table,
                "fact_daily_product": settings.fact_daily_product_table,
                "fact_daily_model": settings.fact_daily_model_table,
                "fact_daily_issue": settings.fact_daily_issue_table,
                "fact_daily_channel": settings.fact_daily_channel_table,
                "fact_daily_bot": settings.fact_daily_bot_table,
                "pipeline_log": settings.pipeline_log_table,
            },
        }

    def get_latest_cached_ticket_created_at(self) -> datetime | None:
        connection = self.open_agg_connection()
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT MAX(created_at) FROM {settings.raw_ticket_cache_table}"
        )
        row = cursor.fetchone()
        cursor.close()
        connection.close()
        latest = row[0] if row else None
        return parse_datetime(latest)

    def fetch_cached_tickets(self, since: datetime | None = None) -> list[TicketRecord]:
        connection = self.open_agg_connection()
        cursor = connection.cursor(dictionary=True)
        query = self._build_cached_ticket_select_query(since=since)
        if since:
            cursor.execute(query, (since.strftime("%Y-%m-%d %H:%M:%S"),))
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        tickets: list[TicketRecord] = []
        for row in rows:
            ticket = self._cached_row_to_ticket(row)
            if ticket:
                tickets.append(ticket)
        return tickets

    def upsert_cached_tickets(self, tickets: list[TicketRecord]) -> int:
        if not tickets:
            return 0
        connection = self.open_agg_connection()
        cursor = connection.cursor()
        columns = [
            "ticket_id",
            "created_at",
            "closed_at",
            "department_name",
            "channel",
            "email",
            "mobile",
            "phone",
            "name",
            "product",
            "device_model",
            "fault_code",
            "fault_code_level_1",
            "fault_code_level_2",
            "resolution_code_level_1",
            "bot_action",
            "software_version",
            "device_serial_number",
            "number_of_reopen",
            "symptom",
            "defect",
            "repair",
        ]
        placeholders = ", ".join(["%s"] * len(columns))
        updates = ", ".join(
            f"{column}=VALUES({column})" for column in columns if column != "ticket_id"
        )
        sql = (
            f"INSERT INTO {settings.raw_ticket_cache_table} ({', '.join(columns)}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )
        values = [
            (
                ticket.ticket_id,
                ticket.created_at,
                ticket.closed_at,
                ticket.department_name,
                ticket.channel,
                ticket.email,
                ticket.mobile,
                ticket.phone,
                ticket.name,
                ticket.product,
                ticket.device_model,
                ticket.fault_code,
                ticket.fault_code_level_1,
                ticket.fault_code_level_2,
                ticket.resolution_code_level_1,
                ticket.bot_action,
                ticket.software_version,
                ticket.device_serial_number,
                ticket.number_of_reopen,
                ticket.symptom,
                ticket.defect,
                ticket.repair,
            )
            for ticket in tickets
        ]
        cursor.executemany(sql, values)
        affected = cursor.rowcount
        connection.commit()
        cursor.close()
        connection.close()
        return affected

    def fetch_snapshot_payload(self, cache_type: str, cache_key: str) -> dict[str, Any] | None:
        connection = self.open_agg_connection()
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT payload_json FROM {settings.api_snapshot_cache_table} WHERE cache_type = %s AND cache_key = %s",
            (cache_type, cache_key),
        )
        row = cursor.fetchone()
        cursor.close()
        connection.close()
        if not row or not row[0]:
            return None
        try:
            payload = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def upsert_snapshot_payload(self, cache_type: str, cache_key: str, payload: dict[str, Any], source_mode: str) -> None:
        connection = self.open_agg_connection()
        cursor = connection.cursor()
        cursor.execute(
            (
                f"INSERT INTO {settings.api_snapshot_cache_table} "
                "(cache_type, cache_key, payload_json, source_mode, generated_at) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE payload_json = VALUES(payload_json), source_mode = VALUES(source_mode), generated_at = VALUES(generated_at)"
            ),
            (
                cache_type,
                cache_key,
                json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
                source_mode,
                datetime.now(),
            ),
        )
        connection.commit()
        cursor.close()
        connection.close()

    def _fetch_zoho_mysql(self, since: datetime | None = None) -> list[TicketRecord]:
        import mysql.connector

        connection = mysql.connector.connect(
            host=settings.zoho_db.host,
            port=settings.zoho_db.port,
            user=settings.zoho_db.user,
            password=settings.zoho_db.password,
            database=settings.zoho_db.database,
        )
        self._set_zoho_session(connection)
        available_columns = self._get_zoho_columns(connection)
        cursor = connection.cursor(dictionary=True)
        where_clause = "WHERE Created_Time >= %s" if since else ""
        query = self._build_zoho_select_query(available_columns, where_clause=where_clause)
        if since:
            cursor.execute(query, (since.strftime("%Y-%m-%d %H:%M:%S"),))
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        tickets: list[TicketRecord] = []
        for row in rows:
            ticket = self._row_to_ticket(row)
            if ticket:
                tickets.append(ticket)
        return tickets

    def _build_cached_ticket_select_query(self, since: datetime | None = None) -> str:
        where_clause = "WHERE created_at >= %s" if since else ""
        return (
            "SELECT "
            "ticket_id, created_at, closed_at, department_name, channel, email, mobile, phone, "
            "name, product, device_model, fault_code, fault_code_level_1, fault_code_level_2, "
            "resolution_code_level_1, bot_action, software_version, device_serial_number, "
            "number_of_reopen, symptom, defect, repair "
            f"FROM {settings.raw_ticket_cache_table} {where_clause} ORDER BY created_at"
        ).strip()

    def _fetch_issue_tickets_from_zoho(
        self,
        product_family: str,
        fault_code: str,
        fault_code_level_2: str,
        software_version: str,
        limit: int,
    ) -> list[TicketRecord]:
        import mysql.connector

        connection = mysql.connector.connect(
            host=settings.zoho_db.host,
            port=settings.zoho_db.port,
            user=settings.zoho_db.user,
            password=settings.zoho_db.password,
            database=settings.zoho_db.database,
        )
        self._set_zoho_session(connection)
        available_columns = self._get_zoho_columns(connection)
        cursor = connection.cursor(dictionary=True)
        query = self._build_zoho_select_query(
            available_columns,
            where_clause=(
                "WHERE Fault_Code = %s "
                "AND Fault_Code_Level_2 = %s "
                "ORDER BY Created_Time DESC "
                "LIMIT %s"
            ),
        )
        cursor.execute(query, (fault_code, fault_code_level_2, limit * 8))
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        tickets: list[TicketRecord] = []
        for row in rows:
            ticket = self._row_to_ticket(row)
            if ticket and ticket.canonical_product == product_family:
                tickets.append(ticket)
            if len(tickets) >= limit:
                break
        return tickets

    def open_agg_connection(self):
        import mysql.connector

        connection = mysql.connector.connect(
            host=settings.agg_db.host,
            port=settings.agg_db.port,
            user=settings.agg_db.user,
            password=settings.agg_db.password,
            database=settings.agg_db.database,
        )
        return connection

    def _set_zoho_session(self, connection) -> None:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SET SESSION sql_mode = ''")
        cursor.close()

    def _get_zoho_columns(self, connection) -> set[str]:
        if self._zoho_columns_cache is not None:
            return self._zoho_columns_cache
        cursor = connection.cursor()
        cursor.execute(f"SHOW COLUMNS FROM {settings.zoho_ticket_table}")
        self._zoho_columns_cache = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return self._zoho_columns_cache

    def _build_zoho_select_query(self, available_columns: set[str], where_clause: str = "") -> str:
        desired_columns = [
            "Ticket_Id",
            "Created_Time",
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
            "Software_Version",
            "Device_Serial_Number",
            "Number_of_Reopen",
            "Symptom",
            "Defect",
            "Repair",
        ]
        select_parts = []
        for column in desired_columns:
            if column in available_columns:
                select_parts.append(column)
            else:
                select_parts.append(f"NULL AS {column}")
        return f"SELECT {', '.join(select_parts)} FROM {settings.zoho_ticket_table} {where_clause}".strip()

    def _row_to_ticket(self, row: dict[str, object]) -> TicketRecord | None:
        created_at = parse_datetime(row.get("Created_Time"))
        if not created_at:
            return None
        return TicketRecord(
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
            software_version=clean_text(row.get("Software_Version")),
            device_serial_number=clean_text(row.get("Device_Serial_Number")),
            number_of_reopen=clean_text(row.get("Number_of_Reopen")),
            symptom=clean_text(row.get("Symptom")),
            defect=clean_text(row.get("Defect")),
            repair=clean_text(row.get("Repair")),
            raw={str(key): value for key, value in row.items()},
        )

    def _cached_row_to_ticket(self, row: dict[str, object]) -> TicketRecord | None:
        created_at = parse_datetime(row.get("created_at"))
        if not created_at:
            return None
        return TicketRecord(
            ticket_id=str(row.get("ticket_id") or ""),
            created_at=created_at,
            closed_at=parse_datetime(row.get("closed_at")),
            department_name=clean_text(row.get("department_name")),
            channel=clean_text(row.get("channel")),
            email=clean_text(row.get("email")),
            mobile=clean_text(row.get("mobile")),
            phone=clean_text(row.get("phone")),
            name=clean_text(row.get("name")),
            product=clean_text(row.get("product")),
            device_model=clean_text(row.get("device_model")),
            fault_code=clean_text(row.get("fault_code")),
            fault_code_level_1=clean_text(row.get("fault_code_level_1")),
            fault_code_level_2=clean_text(row.get("fault_code_level_2")),
            resolution_code_level_1=clean_text(row.get("resolution_code_level_1")),
            bot_action=clean_text(row.get("bot_action")),
            software_version=clean_text(row.get("software_version")),
            device_serial_number=clean_text(row.get("device_serial_number")),
            number_of_reopen=clean_text(row.get("number_of_reopen")),
            symptom=clean_text(row.get("symptom")),
            defect=clean_text(row.get("defect")),
            repair=clean_text(row.get("repair")),
            raw={str(key): value for key, value in row.items()},
        )


def clean_text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def parse_datetime(value: object) -> datetime | None:
    if value in {None, "", "0000-00-00 00:00:00", "0000-00-00"}:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None
