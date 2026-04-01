CREATE DATABASE IF NOT EXISTS qubo_analytics;

CREATE TABLE IF NOT EXISTS qubo_analytics.tickets_fact_recent (
    ticket_id String,
    source_updated_at DateTime64(3, 'UTC'),
    ingest_version UInt64,
    ingested_at DateTime64(3, 'UTC'),
    created_at DateTime64(3, 'UTC'),
    created_date Date,
    closed_at Nullable(DateTime64(3, 'UTC')),
    department_name Nullable(String),
    normalized_department LowCardinality(String),
    channel Nullable(String),
    normalized_channel LowCardinality(String),
    customer_name Nullable(String),
    email Nullable(String),
    mobile Nullable(String),
    phone Nullable(String),
    product Nullable(String),
    device_model Nullable(String),
    canonical_product LowCardinality(String),
    fault_code Nullable(String),
    normalized_fault_code LowCardinality(String),
    fault_code_level_1 Nullable(String),
    normalized_fault_code_l1 LowCardinality(String),
    fault_code_level_2 Nullable(String),
    normalized_fault_code_l2 LowCardinality(String),
    resolution_code_level_1 Nullable(String),
    normalized_resolution LowCardinality(String),
    bot_action Nullable(String),
    bot_outcome LowCardinality(String),
    status Nullable(String),
    device_serial_number Nullable(String),
    number_of_reopen Nullable(String),
    symptom Nullable(String),
    defect Nullable(String),
    repair Nullable(String),
    first_commissioning_date Nullable(DateTime64(3, 'UTC')),
    customer_key Nullable(String),
    field_visit_type LowCardinality(String),
    handle_time_minutes Nullable(Float64),
    device_age_days Nullable(Int32),
    is_core_product UInt8,
    is_internal_hero UInt8,
    is_field_service UInt8,
    is_logistics UInt8,
    is_bot_resolved UInt8,
    is_bot_transferred UInt8,
    is_blank_chat UInt8,
    is_fcr_success UInt8,
    repeat_flag UInt8,
    usable_issue UInt8,
    actionable_issue UInt8,
    dropped_in_bot UInt8,
    missing_issue_outside_bot UInt8,
    dirty_channel UInt8,
    reassigned_email_department UInt8,
    blank_chat_returned_7d UInt8,
    blank_chat_resolved_7d UInt8,
    blank_chat_transferred_7d UInt8,
    blank_chat_blank_again_7d UInt8
)
ENGINE = ReplacingMergeTree(ingest_version)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, canonical_product, normalized_fault_code, ticket_id);

CREATE TABLE IF NOT EXISTS qubo_analytics.tickets_daily_summary (
    metric_date Date,
    product_family LowCardinality(String),
    fault_code LowCardinality(String),
    fault_code_level_1 LowCardinality(String),
    fault_code_level_2 LowCardinality(String),
    department_name LowCardinality(String),
    channel LowCardinality(String),
    bot_outcome LowCardinality(String),
    tickets UInt64,
    field_visit_tickets UInt64,
    repair_field_tickets UInt64,
    installation_field_tickets UInt64,
    bot_resolved_tickets UInt64,
    bot_transferred_tickets UInt64,
    blank_chat_tickets UInt64,
    fcr_tickets UInt64,
    repeat_tickets UInt64,
    logistics_tickets UInt64,
    young_device_tickets UInt64,
    usable_issue_tickets UInt64,
    actionable_issue_tickets UInt64,
    other_product_tickets UInt64,
    hero_internal_tickets UInt64,
    missing_issue_outside_bot_tickets UInt64,
    dirty_channel_tickets UInt64,
    email_department_reassigned_tickets UInt64,
    total_handle_time_minutes Float64,
    handle_time_ticket_count UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, product_family, fault_code, fault_code_level_1, fault_code_level_2, department_name, channel, bot_outcome);

CREATE TABLE IF NOT EXISTS qubo_analytics.issues_daily_summary (
    metric_date Date,
    product_family LowCardinality(String),
    fault_code LowCardinality(String),
    fault_code_level_1 LowCardinality(String),
    fault_code_level_2 LowCardinality(String),
    department_name LowCardinality(String),
    channel LowCardinality(String),
    tickets UInt64,
    repair_field_tickets UInt64,
    installation_field_tickets UInt64,
    repeat_tickets UInt64,
    bot_resolved_tickets UInt64,
    bot_transferred_tickets UInt64,
    blank_chat_tickets UInt64,
    fcr_tickets UInt64,
    logistics_tickets UInt64,
    top_symptom String,
    top_defect String,
    top_repair String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, product_family, fault_code, fault_code_level_1, fault_code_level_2, department_name, channel);

CREATE TABLE IF NOT EXISTS qubo_analytics.etl_sync_state (
    pipeline_name LowCardinality(String),
    last_successful_sync Nullable(DateTime64(3, 'UTC')),
    last_attempted_sync Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC'),
    status LowCardinality(String),
    notes String
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (pipeline_name);

CREATE TABLE IF NOT EXISTS qubo_analytics.etl_run_log (
    job_name LowCardinality(String),
    started_at DateTime64(3, 'UTC'),
    finished_at DateTime64(3, 'UTC'),
    status LowCardinality(String),
    rows_fetched UInt64,
    rows_inserted UInt64,
    affected_dates UInt32,
    last_sync_time Nullable(DateTime64(3, 'UTC')),
    message String,
    stacktrace String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (job_name, started_at);
