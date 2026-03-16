from __future__ import annotations

from datetime import datetime

from ..config import settings
from ..repository import TicketRepository
from .sql import CREATE_TABLE_STATEMENTS
from .transforms import build_aggregates, build_ticket_facts


def reset_table(cursor, table_name: str, create_sql: str) -> None:
    if settings.pipeline_recreate_tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(create_sql)


def bulk_insert(cursor, table_name: str, rows: list[dict]) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"
    values = [tuple(row[column] for column in columns) for row in rows]
    cursor.executemany(sql, values)


def ensure_tables(cursor) -> None:
    table_map = {
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
    }
    for key, table_name in table_map.items():
        reset_table(cursor, table_name, CREATE_TABLE_STATEMENTS[key].format(table_name=table_name))


def run_pipeline() -> None:
    if not settings.has_agg_database:
        raise RuntimeError("Local aggregate DB is not configured.")
    started_at = datetime.now()
    repository = TicketRepository()
    tickets = repository.fetch_tickets_strict()
    facts = build_ticket_facts(tickets)
    aggregates = build_aggregates(facts)

    connection = repository.open_agg_connection()
    cursor = connection.cursor()
    ensure_tables(cursor)

    table_targets = {
        settings.agg_daily_tickets_table: aggregates["agg_daily_tickets"],
        settings.agg_fc_weekly_table: aggregates["agg_fc_weekly"],
        settings.agg_sw_version_table: aggregates["agg_sw_version"],
        settings.agg_resolution_table: aggregates["agg_resolution"],
        settings.agg_channel_table: aggregates["agg_channel"],
        settings.agg_bot_table: aggregates["agg_bot"],
        settings.agg_hourly_heatmap_table: aggregates["agg_hourly_heatmap"],
        settings.agg_replacements_table: aggregates["agg_replacements"],
        settings.agg_voc_mismatch_table: aggregates["agg_voc_mismatch"],
        settings.agg_anomalies_table: aggregates["agg_anomalies"],
        settings.agg_health_score_table: aggregates["agg_health_score"],
        settings.agg_data_quality_table: aggregates["agg_data_quality"],
    }

    for table_name, rows in table_targets.items():
        bulk_insert(cursor, table_name, rows)

    finished_at = datetime.now()
    duration_minutes = int((finished_at - started_at).total_seconds() / 60)
    log_row = {
        "run_started_at": started_at,
        "run_finished_at": finished_at,
        "duration_minutes": duration_minutes,
        "status": "Success",
        "job_name": "nightly_aggregate_refresh",
        "source_rows": len(tickets),
        "message": "Full refresh completed",
    }
    bulk_insert(cursor, settings.pipeline_log_table, [log_row])
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Pipeline complete. Source rows: {len(tickets)}. Duration: {duration_minutes} minutes.")


if __name__ == "__main__":
    run_pipeline()
