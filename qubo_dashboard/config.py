from __future__ import annotations

from dataclasses import dataclass
import os

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> None:
        return None


load_dotenv()

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
class Settings:
    app_host: str = os.getenv("QUBO_APP_HOST", "127.0.0.1")
    app_port: int = int(os.getenv("QUBO_APP_PORT", "8000"))
    app_reload: bool = os.getenv("QUBO_APP_RELOAD", "true").lower() == "true"
    serve_frontend: bool = os.getenv("QUBO_SERVE_FRONTEND", "false").lower() == "true"
    use_sample_data: bool = os.getenv("QUBO_USE_SAMPLE_DATA", "false").lower() == "true"
    pipeline_recreate_tables: bool = os.getenv("QUBO_PIPELINE_RECREATE_TABLES", "true").lower() == "true"
    cors_allowed_origins_raw: str = os.getenv("QUBO_CORS_ALLOWED_ORIGINS", "*")
    zoho_ticket_table: str = os.getenv("QUBO_ZOHO_TICKET_TABLE", "Call_Driver_Data_Zoho_FromAug2024")
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
    def has_zoho_database(self) -> bool:
        return bool(not self.use_sample_data and self.zoho_db.is_configured)

    @property
    def has_agg_database(self) -> bool:
        return bool(not self.use_sample_data and self.agg_db.is_configured)

    @property
    def cors_allowed_origins(self) -> list[str]:
        raw = self.cors_allowed_origins_raw.strip()
        if not raw:
            return []
        if raw == "*":
            return ["*"]
        return [item.strip() for item in raw.split(",") if item.strip()]


settings = Settings()
