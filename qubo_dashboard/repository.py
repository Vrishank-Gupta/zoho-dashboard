from __future__ import annotations

from datetime import datetime
from typing import Any

from .config import settings
from .models import TicketRecord
from .sample_data import generate_sample_tickets


class TicketRepository:
    def __init__(self) -> None:
        self._zoho_columns_cache: set[str] | None = None

    def fetch_tickets(self) -> list[TicketRecord]:
        if not settings.has_zoho_database:
            return generate_sample_tickets()
        try:
            return self._fetch_zoho_mysql()
        except Exception:
            return generate_sample_tickets()

    def fetch_tickets_strict(self) -> list[TicketRecord]:
        if not settings.has_zoho_database:
            raise RuntimeError("Remote Zoho DB is not configured.")
        return self._fetch_zoho_mysql()

    def fetch_issue_tickets(
        self,
        product_family: str,
        fault_code: str,
        fault_code_level_1: str,
        fault_code_level_2: str,
        limit: int = 24,
    ) -> list[TicketRecord]:
        if not settings.has_zoho_database:
            tickets = generate_sample_tickets()
            return [
                ticket
                for ticket in tickets
                if ticket.canonical_product == product_family
                and ticket.normalized_fault_code == fault_code
                and ticket.normalized_fault_code_l1 == fault_code_level_1
                and ticket.normalized_fault_code_l2 == fault_code_level_2
            ][:limit]
        try:
            return self._fetch_issue_tickets_from_zoho(
                product_family=product_family,
                fault_code=fault_code,
                fault_code_level_1=fault_code_level_1,
                fault_code_level_2=fault_code_level_2,
                limit=limit,
            )
        except Exception:
            return []

    def get_connection_status(self) -> dict[str, Any]:
        return {
            "use_sample_data": settings.use_sample_data,
            "zoho_configured": settings.has_zoho_database,
            "agg_configured": settings.has_agg_database,
            "zoho_ticket_table": settings.zoho_ticket_table,
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
                "pipeline_log": settings.pipeline_log_table,
            },
        }

    def _fetch_zoho_mysql(self) -> list[TicketRecord]:
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
            where_clause="WHERE Created_Time >= %s ORDER BY Created_Time DESC",
        )
        cursor.execute(query, (settings.source_start_date,))
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        tickets: list[TicketRecord] = []
        for row in rows:
            ticket = self._row_to_ticket(row)
            if ticket:
                tickets.append(ticket)
        return tickets

    def _fetch_issue_tickets_from_zoho(
        self,
        product_family: str,
        fault_code: str,
        fault_code_level_1: str,
        fault_code_level_2: str,
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
                "WHERE Created_Time >= %s "
                "AND Fault_Code = %s "
                "AND Fault_Code_Level_1 = %s "
                "AND Fault_Code_Level_2 = %s "
                "ORDER BY Created_Time DESC "
                "LIMIT %s"
            ),
        )
        cursor.execute(query, (settings.source_start_date, fault_code, fault_code_level_1, fault_code_level_2, limit * 8))
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
            "Status",
            "Device_Serial_Number",
            "Number_of_Reopen",
            "Symptom",
            "Defect",
            "Repair",
            "First_Commissioning_Date",
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
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None
