-- Qubo Support Health Command Center
-- Bootstrap script for the aggregate analytics schema.
-- Run this against the aggregate MySQL database configured via QUBO_AGG_*.

CREATE TABLE IF NOT EXISTS agg_daily_tickets (
    metric_date DATE NOT NULL,
    product_family VARCHAR(100) NOT NULL,
    fault_code VARCHAR(100) NOT NULL,
    fault_code_level_2 VARCHAR(150) NOT NULL,
    department_name VARCHAR(100) NOT NULL,
    channel VARCHAR(100) NOT NULL,
    software_version VARCHAR(100) NOT NULL,
    tickets INT NOT NULL,
    field_visit_rate DOUBLE NOT NULL,
    repair_field_visit_rate DOUBLE NOT NULL,
    installation_field_visit_rate DOUBLE NOT NULL,
    bot_deflection_rate DOUBLE NOT NULL,
    bot_transfer_rate DOUBLE NOT NULL,
    blank_chat_rate DOUBLE NOT NULL,
    fcr_rate DOUBLE NOT NULL,
    repeat_rate DOUBLE NOT NULL,
    logistics_rate DOUBLE NOT NULL,
    handle_time_hours DOUBLE NOT NULL,
    young_device_rate DOUBLE NOT NULL,
    KEY idx_agg_daily_metric_date (metric_date),
    KEY idx_agg_daily_product_issue (product_family, fault_code, fault_code_level_2),
    KEY idx_agg_daily_department_channel (department_name, channel)
);

CREATE TABLE IF NOT EXISTS agg_fc_weekly (
    week_start DATE NOT NULL,
    product_family VARCHAR(100) NOT NULL,
    fault_code VARCHAR(100) NOT NULL,
    fault_code_level_2 VARCHAR(150) NOT NULL,
    software_version VARCHAR(100) NOT NULL,
    tickets INT NOT NULL,
    repair_field_visit_rate DOUBLE NOT NULL,
    installation_field_visit_rate DOUBLE NOT NULL,
    repeat_rate DOUBLE NOT NULL,
    bot_deflection_rate DOUBLE NOT NULL,
    bot_transfer_rate DOUBLE NOT NULL,
    blank_chat_rate DOUBLE NOT NULL,
    fcr_rate DOUBLE NOT NULL,
    logistics_rate DOUBLE NOT NULL,
    top_symptom VARCHAR(255) NOT NULL,
    top_defect VARCHAR(255) NOT NULL,
    top_repair VARCHAR(255) NOT NULL,
    KEY idx_agg_fc_weekly_week (week_start),
    KEY idx_agg_fc_weekly_issue (product_family, fault_code, fault_code_level_2),
    KEY idx_agg_fc_weekly_version (software_version)
);

CREATE TABLE IF NOT EXISTS agg_sw_version (
    as_of_date DATE NOT NULL,
    product_family VARCHAR(100) NOT NULL,
    software_version VARCHAR(100) NOT NULL,
    fault_code_level_2 VARCHAR(150) NOT NULL,
    tickets_60d INT NOT NULL,
    tickets_prev_60d INT NOT NULL,
    repair_field_visit_rate DOUBLE NOT NULL,
    repeat_rate DOUBLE NOT NULL,
    severity_index DOUBLE NOT NULL,
    coverage_rate DOUBLE NOT NULL,
    KEY idx_agg_sw_version_date (as_of_date),
    KEY idx_agg_sw_version_product_version (product_family, software_version),
    KEY idx_agg_sw_version_issue (fault_code_level_2)
);

CREATE TABLE IF NOT EXISTS agg_resolution (
    month_start DATE NOT NULL,
    product_family VARCHAR(100) NOT NULL,
    resolution_code_level_1 VARCHAR(150) NOT NULL,
    tickets INT NOT NULL,
    fcr_rate DOUBLE NOT NULL,
    bot_deflection_rate DOUBLE NOT NULL,
    bot_transfer_rate DOUBLE NOT NULL,
    blank_chat_rate DOUBLE NOT NULL,
    repair_field_rate DOUBLE NOT NULL,
    KEY idx_agg_resolution_month (month_start),
    KEY idx_agg_resolution_product (product_family),
    KEY idx_agg_resolution_code (resolution_code_level_1)
);

CREATE TABLE IF NOT EXISTS agg_channel (
    month_start DATE NOT NULL,
    channel VARCHAR(100) NOT NULL,
    department_name VARCHAR(100) NOT NULL,
    tickets INT NOT NULL,
    fcr_rate DOUBLE NOT NULL,
    bot_deflection_rate DOUBLE NOT NULL,
    bot_transfer_rate DOUBLE NOT NULL,
    blank_chat_rate DOUBLE NOT NULL,
    repair_field_rate DOUBLE NOT NULL,
    handle_time_hours DOUBLE NOT NULL,
    KEY idx_agg_channel_month (month_start),
    KEY idx_agg_channel_channel (channel),
    KEY idx_agg_channel_department (department_name)
);

CREATE TABLE IF NOT EXISTS agg_hourly_heatmap (
    weekday_name VARCHAR(20) NOT NULL,
    hour_slot_4h VARCHAR(20) NOT NULL,
    tickets INT NOT NULL,
    KEY idx_agg_hourly_heatmap_weekday_slot (weekday_name, hour_slot_4h)
);

CREATE TABLE IF NOT EXISTS agg_replacements (
    month_start DATE NOT NULL,
    product_family VARCHAR(100) NOT NULL,
    resolution_reason VARCHAR(150) NOT NULL,
    tickets INT NOT NULL,
    estimated_cost DOUBLE NOT NULL,
    KEY idx_agg_replacements_month (month_start),
    KEY idx_agg_replacements_product (product_family)
);

CREATE TABLE IF NOT EXISTS agg_bot (
    product_family VARCHAR(100) NOT NULL,
    chat_tickets INT NOT NULL,
    bot_resolved_tickets INT NOT NULL,
    bot_transferred_tickets INT NOT NULL,
    blank_chat_tickets INT NOT NULL,
    blank_chat_returned_7d INT NOT NULL,
    blank_chat_resolved_7d INT NOT NULL,
    blank_chat_transferred_7d INT NOT NULL,
    blank_chat_blank_again_7d INT NOT NULL,
    blank_chat_return_rate DOUBLE NOT NULL,
    blank_chat_recovery_rate DOUBLE NOT NULL,
    blank_chat_repeat_rate DOUBLE NOT NULL,
    bot_resolved_rate DOUBLE NOT NULL,
    bot_transferred_rate DOUBLE NOT NULL,
    blank_chat_rate DOUBLE NOT NULL,
    KEY idx_agg_bot_product (product_family)
);

CREATE TABLE IF NOT EXISTS agg_voc_mismatch (
    product_family VARCHAR(100) NOT NULL,
    fault_code_level_2 VARCHAR(150) NOT NULL,
    diagnosed_defect VARCHAR(255) NOT NULL,
    tickets INT NOT NULL,
    mismatch_rate DOUBLE NOT NULL,
    KEY idx_agg_voc_mismatch_product_issue (product_family, fault_code_level_2)
);

CREATE TABLE IF NOT EXISTS agg_anomalies (
    detected_at DATE NOT NULL,
    product_family VARCHAR(100) NOT NULL,
    fault_code VARCHAR(100) NOT NULL,
    department_name VARCHAR(100) NOT NULL,
    current_14d INT NOT NULL,
    baseline_60d DOUBLE NOT NULL,
    anomaly_score DOUBLE NOT NULL,
    KEY idx_agg_anomalies_date (detected_at),
    KEY idx_agg_anomalies_product_issue (product_family, fault_code),
    KEY idx_agg_anomalies_department (department_name)
);

CREATE TABLE IF NOT EXISTS agg_health_score (
    metric_date DATE NOT NULL,
    health_score DOUBLE NOT NULL,
    repair_field_rate DOUBLE NOT NULL,
    repeat_rate DOUBLE NOT NULL,
    bot_deflection_rate DOUBLE NOT NULL,
    fcr_rate DOUBLE NOT NULL,
    KEY idx_agg_health_score_date (metric_date)
);

CREATE TABLE IF NOT EXISTS agg_data_quality (
    as_of_date DATE NOT NULL,
    total_tickets INT NOT NULL,
    usable_issue_tickets INT NOT NULL,
    actionable_issue_tickets INT NOT NULL,
    blank_fault_code_tickets INT NOT NULL,
    blank_fault_code_l2_tickets INT NOT NULL,
    unknown_product_tickets INT NOT NULL,
    hero_internal_tickets INT NOT NULL,
    version_coverage_tickets INT NOT NULL,
    dropped_in_bot_tickets INT NOT NULL,
    missing_issue_outside_bot_tickets INT NOT NULL,
    dirty_channel_tickets INT NOT NULL,
    email_department_reassigned_tickets INT NOT NULL,
    KEY idx_agg_data_quality_date (as_of_date)
);

CREATE TABLE IF NOT EXISTS pipeline_log (
    run_started_at DATETIME NOT NULL,
    run_finished_at DATETIME NOT NULL,
    duration_minutes INT NOT NULL,
    status VARCHAR(30) NOT NULL,
    job_name VARCHAR(100) NOT NULL,
    source_rows INT NOT NULL,
    message VARCHAR(255) NOT NULL,
    KEY idx_pipeline_log_started (run_started_at),
    KEY idx_pipeline_log_status (status)
);
