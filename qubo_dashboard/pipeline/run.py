from __future__ import annotations

from datetime import datetime
from time import perf_counter

from ..config import settings
from ..repository import TicketRepository
from .sql import CREATE_TABLE_STATEMENTS
from .transforms import build_aggregates, build_ticket_facts


def reset_table(cursor, table_name: str, create_sql: str) -> None:
    if settings.pipeline_recreate_tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(create_sql)
        return
    cursor.execute(create_sql)
    cursor.execute(f"TRUNCATE TABLE {table_name}")


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
        "agg_model_breakdown": settings.agg_model_breakdown_table,
        "pipeline_log": settings.pipeline_log_table,
    }
    for key, table_name in table_map.items():
        reset_table(cursor, table_name, CREATE_TABLE_STATEMENTS[key].format(table_name=table_name))


def ensure_raw_ticket_cache_table(cursor) -> None:
    cursor.execute(
        CREATE_TABLE_STATEMENTS["raw_ticket_cache"].format(
            table_name=settings.raw_ticket_cache_table
        )
    )


def run_pipeline() -> None:
    if not settings.has_agg_database:
        raise RuntimeError("Local aggregate DB is not configured.")
    started_at = datetime.now()
    repository = TicketRepository()
    cutoff = datetime.fromisoformat(settings.pipeline_source_cutoff)
    connection = repository.open_agg_connection()
    cursor = connection.cursor()
    ensure_raw_ticket_cache_table(cursor)
    connection.commit()
    cursor.close()
    connection.close()

    latest_cached = repository.get_latest_cached_ticket_created_at()
    fetch_since = max(cutoff, latest_cached) if latest_cached else cutoff

    fetch_started = perf_counter()
    new_tickets = repository.fetch_tickets_strict(since=fetch_since)
    fetch_elapsed = perf_counter() - fetch_started
    print(f"Fetched {len(new_tickets)} tickets from Zoho since {fetch_since} in {fetch_elapsed:.1f}s")

    cache_started = perf_counter()
    cached_rows_written = repository.upsert_cached_tickets(new_tickets)
    cache_elapsed = perf_counter() - cache_started
    print(f"Upserted {cached_rows_written} rows into {settings.raw_ticket_cache_table} in {cache_elapsed:.1f}s")

    load_started = perf_counter()
    tickets = repository.fetch_cached_tickets(since=cutoff)
    load_elapsed = perf_counter() - load_started
    print(f"Loaded {len(tickets)} cached tickets from {settings.raw_ticket_cache_table} since {cutoff.date()} in {load_elapsed:.1f}s")

    build_started = perf_counter()
    facts = build_ticket_facts(tickets)
    aggregates = build_aggregates(facts)
    build_elapsed = perf_counter() - build_started
    print(f"Built ticket facts and aggregates in {build_elapsed:.1f}s")

    connection = repository.open_agg_connection()
    cursor = connection.cursor()
    ddl_started = perf_counter()
    ensure_tables(cursor)
    ddl_elapsed = perf_counter() - ddl_started
    print(f"Prepared aggregate tables in {ddl_elapsed:.1f}s")

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
        settings.agg_model_breakdown_table: aggregates["agg_model_breakdown"],
    }

    write_started = perf_counter()
    for table_name, rows in table_targets.items():
        bulk_insert(cursor, table_name, rows)
        print(f"Wrote {len(rows)} rows to {table_name}")

    finished_at = datetime.now()
    duration_minutes = int((finished_at - started_at).total_seconds() / 60)
    log_row = {
        "run_started_at": started_at,
        "run_finished_at": finished_at,
        "duration_minutes": duration_minutes,
        "status": "Success",
        "job_name": "nightly_aggregate_refresh",
        "source_rows": len(tickets),
        "message": f"Aggregate refresh completed from cache; fetched {len(new_tickets)} incremental Zoho rows",
    }
    bulk_insert(cursor, settings.pipeline_log_table, [log_row])
    connection.commit()
    write_elapsed = perf_counter() - write_started
    cursor.close()
    connection.close()
    print(f"Pipeline complete. Source rows: {len(tickets)}. Write phase: {write_elapsed:.1f}s. Duration: {duration_minutes} minutes.")


if __name__ == "__main__":
    run_pipeline()
