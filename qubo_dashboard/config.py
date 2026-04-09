from __future__ import annotations

from dataclasses import dataclass
import os

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> None:
        return None


load_dotenv()

TIMEZONE_ALIASES = {
    "Asia/Calcutta": "Asia/Kolkata",
}

@dataclass(slots=True)
class DatabaseConfig:
    host: str | None
    port: int
    user: str | None
    password: str | None
    database: str | None

    @property
    def is_configured(self) -> bool:
        return bool(self.host and self.user and self.password and self.database)


@dataclass(slots=True)
class ClickHouseConfig:
    host: str | None
    port: int
    user: str | None
    password: str | None
    database: str | None
    secure: bool

    @property
    def is_configured(self) -> bool:
        return bool(self.host and self.user and self.database)


@dataclass(slots=True)
class Settings:
    app_host: str = os.getenv("QUBO_APP_HOST", "127.0.0.1")
    app_port: int = int(os.getenv("QUBO_APP_PORT", "8000"))
    app_reload: bool = os.getenv("QUBO_APP_RELOAD", "true").lower() == "true"
    serve_frontend: bool = os.getenv("QUBO_SERVE_FRONTEND", "false").lower() == "true"
    use_sample_data: bool = os.getenv("QUBO_USE_SAMPLE_DATA", "false").lower() == "true"
    analytics_backend: str = os.getenv("QUBO_ANALYTICS_BACKEND", "clickhouse").lower()
    pipeline_recreate_tables: bool = os.getenv("QUBO_PIPELINE_RECREATE_TABLES", "true").lower() == "true"
    cors_allowed_origins_raw: str = os.getenv("QUBO_CORS_ALLOWED_ORIGINS", "*")
    zoho_ticket_table: str = os.getenv("QUBO_ZOHO_TICKET_TABLE", "Call_Driver_Data_Zoho_FromAug2024")
    zoho_primary_key: str = os.getenv("QUBO_ZOHO_PRIMARY_KEY", "Ticket_Id")
    zoho_created_column: str = os.getenv("QUBO_ZOHO_CREATED_COLUMN", "Created_Time")
    zoho_modified_column: str = os.getenv("QUBO_ZOHO_MODIFIED_COLUMN", "Modified_Time")
    source_start_date: str = os.getenv("QUBO_SOURCE_START_DATE", "2026-01-01")
    mapping_workbook_path: str | None = os.getenv("QUBO_MAPPING_WORKBOOK")
    clickhouse_fact_table: str = os.getenv("QUBO_CLICKHOUSE_FACT_TABLE", "tickets_fact_recent")
    clickhouse_daily_summary_table: str = os.getenv("QUBO_CLICKHOUSE_DAILY_SUMMARY_TABLE", "tickets_daily_summary")
    clickhouse_issues_summary_table: str = os.getenv("QUBO_CLICKHOUSE_ISSUES_SUMMARY_TABLE", "issues_daily_summary")
    clickhouse_repeat_events_table: str = os.getenv("QUBO_CLICKHOUSE_REPEAT_EVENTS_TABLE", "repeat_ticket_events")
    clickhouse_sync_state_table: str = os.getenv("QUBO_CLICKHOUSE_SYNC_STATE_TABLE", "etl_sync_state")
    clickhouse_run_log_table: str = os.getenv("QUBO_CLICKHOUSE_RUN_LOG_TABLE", "etl_run_log")
    etl_schedule: str = os.getenv("QUBO_ETL_SCHEDULE", "0 2 * * *")
    etl_timezone: str = os.getenv("QUBO_ETL_TIMEZONE", "Asia/Calcutta")
    etl_batch_size: int = int(os.getenv("QUBO_ETL_BATCH_SIZE", "5000"))
    etl_backfill_days: int = int(os.getenv("QUBO_ETL_BACKFILL_DAYS", "90"))
    etl_overlap_hours: int = int(os.getenv("QUBO_ETL_OVERLAP_HOURS", "24"))
    etl_job_name: str = os.getenv("QUBO_ETL_JOB_NAME", "mysql_to_clickhouse")
    agg_daily_tickets_table: str = os.getenv("QUBO_AGG_DAILY_TICKETS_TABLE", "agg_daily_tickets")
    agg_fc_weekly_table: str = os.getenv("QUBO_AGG_FC_WEEKLY_TABLE", "agg_fc_weekly")
    agg_sw_version_table: str = os.getenv("QUBO_AGG_SW_VERSION_TABLE", "agg_sw_version")
    agg_resolution_table: str = os.getenv("QUBO_AGG_RESOLUTION_TABLE", "agg_resolution")
    agg_channel_table: str = os.getenv("QUBO_AGG_CHANNEL_TABLE", "agg_channel")
    agg_bot_table: str = os.getenv("QUBO_AGG_BOT_TABLE", "agg_bot")
    agg_hourly_heatmap_table: str = os.getenv("QUBO_AGG_HOURLY_HEATMAP_TABLE", "agg_hourly_heatmap")
    agg_replacements_table: str = os.getenv("QUBO_AGG_REPLACEMENTS_TABLE", "agg_replacements")
    agg_voc_mismatch_table: str = os.getenv("QUBO_AGG_VOC_MISMATCH_TABLE", "agg_voc_mismatch")
    agg_anomalies_table: str = os.getenv("QUBO_AGG_ANOMALIES_TABLE", "agg_anomalies")
    agg_health_score_table: str = os.getenv("QUBO_AGG_HEALTH_SCORE_TABLE", "agg_health_score")
    agg_data_quality_table: str = os.getenv("QUBO_AGG_DATA_QUALITY_TABLE", "agg_data_quality")
    pipeline_log_table: str = os.getenv("QUBO_PIPELINE_LOG_TABLE", "pipeline_log")

    @property
    def zoho_db(self) -> DatabaseConfig:
        return DatabaseConfig(
            host=os.getenv("QUBO_ZOHO_DB_HOST"),
            port=int(os.getenv("QUBO_ZOHO_DB_PORT", "3306")),
            user=os.getenv("QUBO_ZOHO_DB_USER"),
            password=os.getenv("QUBO_ZOHO_DB_PASSWORD"),
            database=os.getenv("QUBO_ZOHO_DB_NAME"),
        )

    @property
    def agg_db(self) -> DatabaseConfig:
        return DatabaseConfig(
            host=os.getenv("QUBO_AGG_DB_HOST"),
            port=int(os.getenv("QUBO_AGG_DB_PORT", "3306")),
            user=os.getenv("QUBO_AGG_DB_USER"),
            password=os.getenv("QUBO_AGG_DB_PASSWORD"),
            database=os.getenv("QUBO_AGG_DB_NAME"),
        )

    @property
    def clickhouse(self) -> ClickHouseConfig:
        return ClickHouseConfig(
            host=os.getenv("QUBO_CLICKHOUSE_HOST"),
            port=int(os.getenv("QUBO_CLICKHOUSE_PORT", "8123")),
            user=os.getenv("QUBO_CLICKHOUSE_USER", "default"),
            password=os.getenv("QUBO_CLICKHOUSE_PASSWORD"),
            database=os.getenv("QUBO_CLICKHOUSE_DATABASE"),
            secure=os.getenv("QUBO_CLICKHOUSE_SECURE", "false").lower() == "true",
        )

    @property
    def has_zoho_database(self) -> bool:
        return bool(not self.use_sample_data and self.zoho_db.is_configured)

    @property
    def has_agg_database(self) -> bool:
        return bool(not self.use_sample_data and self.agg_db.is_configured)

    @property
    def has_clickhouse(self) -> bool:
        return bool(not self.use_sample_data and self.clickhouse.is_configured)

    @property
    def cors_allowed_origins(self) -> list[str]:
        raw = self.cors_allowed_origins_raw.strip()
        if not raw:
            return []
        if raw == "*":
            return ["*"]
        return [item.strip() for item in raw.split(",") if item.strip()]

    @property
    def normalized_etl_timezone(self) -> str:
        return TIMEZONE_ALIASES.get(self.etl_timezone, self.etl_timezone)


settings = Settings()
