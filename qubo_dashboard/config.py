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
    pipeline_recreate_tables: bool = os.getenv("QUBO_PIPELINE_RECREATE_TABLES", "true").lower() == "true"
    pipeline_force_rebuild: bool = os.getenv("QUBO_PIPELINE_FORCE_REBUILD", "false").lower() == "true"
    pipeline_source_cutoff: str = os.getenv("QUBO_PIPELINE_SOURCE_CUTOFF", "2026-01-01")
    raw_ticket_cache_table: str = os.getenv("QUBO_RAW_TICKET_CACHE_TABLE", "raw_ticket_cache")
    api_snapshot_cache_table: str = os.getenv("QUBO_API_SNAPSHOT_CACHE_TABLE", "api_snapshot_cache")
    snapshot_prewarm_enabled: bool = os.getenv("QUBO_SNAPSHOT_PREWARM_ENABLED", "false").lower() == "true"
    snapshot_prewarm_presets_raw: str = os.getenv("QUBO_SNAPSHOT_PREWARM_PRESETS", "60d")
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
    agg_model_breakdown_table: str = os.getenv("QUBO_AGG_MODEL_BREAKDOWN_TABLE", "agg_model_breakdown")
    ticket_facts_table: str = os.getenv("QUBO_TICKET_FACTS_TABLE", "ticket_facts")
    fact_daily_overview_table: str = os.getenv("QUBO_FACT_DAILY_OVERVIEW_TABLE", "fact_daily_overview")
    fact_daily_product_table: str = os.getenv("QUBO_FACT_DAILY_PRODUCT_TABLE", "fact_daily_product")
    fact_daily_model_table: str = os.getenv("QUBO_FACT_DAILY_MODEL_TABLE", "fact_daily_model")
    fact_daily_issue_table: str = os.getenv("QUBO_FACT_DAILY_ISSUE_TABLE", "fact_daily_issue")
    fact_daily_channel_table: str = os.getenv("QUBO_FACT_DAILY_CHANNEL_TABLE", "fact_daily_channel")
    fact_daily_bot_table: str = os.getenv("QUBO_FACT_DAILY_BOT_TABLE", "fact_daily_bot")
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
        return self.zoho_db.is_configured

    @property
    def has_agg_database(self) -> bool:
        return self.agg_db.is_configured

    @property
    def cors_allowed_origins(self) -> list[str]:
        raw = self.cors_allowed_origins_raw.strip()
        if not raw:
            return []
        if raw == "*":
            return ["*"]
        return [item.strip() for item in raw.split(",") if item.strip()]

    @property
    def snapshot_prewarm_presets(self) -> list[str]:
        raw = self.snapshot_prewarm_presets_raw.strip()
        if not raw:
            return ["60d"]
        return [item.strip() for item in raw.split(",") if item.strip()]


settings = Settings()
