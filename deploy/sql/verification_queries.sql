-- Backend / pipeline verification queries for DevOps handoff

SELECT COUNT(*) AS raw_ticket_cache_rows FROM raw_ticket_cache;
SELECT MAX(created_at) AS latest_cached_ticket_created_at FROM raw_ticket_cache;

SELECT COUNT(*) AS agg_daily_tickets_rows FROM agg_daily_tickets;
SELECT MAX(metric_date) AS latest_metric_date FROM agg_daily_tickets;

SELECT COUNT(*) AS agg_fc_weekly_rows FROM agg_fc_weekly;
SELECT COUNT(*) AS agg_bot_rows FROM agg_bot;
SELECT COUNT(*) AS agg_model_breakdown_rows FROM agg_model_breakdown;
SELECT COUNT(*) AS agg_anomalies_rows FROM agg_anomalies;

SELECT run_started_at, run_finished_at, duration_minutes, status, source_rows, message
FROM pipeline_log
ORDER BY run_started_at DESC
LIMIT 10;
