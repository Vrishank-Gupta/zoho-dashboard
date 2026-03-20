from __future__ import annotations


CREATE_TABLE_STATEMENTS = {
    "raw_ticket_cache": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticket_id VARCHAR(100) NOT NULL PRIMARY KEY,
            created_at DATETIME NOT NULL,
            closed_at DATETIME NULL,
            department_name VARCHAR(255) NULL,
            channel VARCHAR(255) NULL,
            email VARCHAR(255) NULL,
            mobile VARCHAR(255) NULL,
            phone VARCHAR(255) NULL,
            name VARCHAR(255) NULL,
            product VARCHAR(255) NULL,
            device_model VARCHAR(255) NULL,
            fault_code VARCHAR(255) NULL,
            fault_code_level_1 VARCHAR(255) NULL,
            fault_code_level_2 VARCHAR(255) NULL,
            resolution_code_level_1 VARCHAR(255) NULL,
            bot_action VARCHAR(255) NULL,
            software_version VARCHAR(255) NULL,
            device_serial_number VARCHAR(255) NULL,
            number_of_reopen VARCHAR(100) NULL,
            symptom TEXT NULL,
            defect TEXT NULL,
            repair TEXT NULL,
            INDEX idx_created_at (created_at)
        )
    """,
    "agg_daily_tickets": """
        CREATE TABLE IF NOT EXISTS {table_name} (
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
            repeat_rate DOUBLE NOT NULL,
            logistics_rate DOUBLE NOT NULL,
            handle_time_hours DOUBLE NOT NULL,
            cancelled_existing_ticket_rate DOUBLE NOT NULL
        )
    """,
    "agg_fc_weekly": """
        CREATE TABLE IF NOT EXISTS {table_name} (
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
            logistics_rate DOUBLE NOT NULL,
            top_symptom VARCHAR(255) NOT NULL,
            top_defect VARCHAR(255) NOT NULL,
            top_repair VARCHAR(255) NOT NULL
        )
    """,
    "agg_sw_version": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            as_of_date DATE NOT NULL,
            product_family VARCHAR(100) NOT NULL,
            software_version VARCHAR(100) NOT NULL,
            fault_code_level_2 VARCHAR(150) NOT NULL,
            tickets_60d INT NOT NULL,
            tickets_prev_60d INT NOT NULL,
            repair_field_visit_rate DOUBLE NOT NULL,
            repeat_rate DOUBLE NOT NULL,
            severity_index DOUBLE NOT NULL,
            coverage_rate DOUBLE NOT NULL
        )
    """,
    "agg_resolution": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            month_start DATE NOT NULL,
            product_family VARCHAR(100) NOT NULL,
            resolution_code_level_1 VARCHAR(150) NOT NULL,
            tickets INT NOT NULL,
            bot_deflection_rate DOUBLE NOT NULL,
            bot_transfer_rate DOUBLE NOT NULL,
            blank_chat_rate DOUBLE NOT NULL,
            repair_field_rate DOUBLE NOT NULL
        )
    """,
    "agg_channel": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            month_start DATE NOT NULL,
            channel VARCHAR(100) NOT NULL,
            department_name VARCHAR(100) NOT NULL,
            tickets INT NOT NULL,
            bot_deflection_rate DOUBLE NOT NULL,
            bot_transfer_rate DOUBLE NOT NULL,
            blank_chat_rate DOUBLE NOT NULL,
            repair_field_rate DOUBLE NOT NULL,
            handle_time_hours DOUBLE NOT NULL
        )
    """,
    "agg_hourly_heatmap": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            weekday_name VARCHAR(20) NOT NULL,
            hour_slot_4h VARCHAR(20) NOT NULL,
            tickets INT NOT NULL
        )
    """,
    "agg_replacements": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            month_start DATE NOT NULL,
            product_family VARCHAR(100) NOT NULL,
            resolution_reason VARCHAR(150) NOT NULL,
            tickets INT NOT NULL,
            estimated_cost DOUBLE NOT NULL
        )
    """,
    "agg_bot": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            product_family VARCHAR(100) NOT NULL,
            chat_tickets INT NOT NULL,
            bot_resolved_tickets INT NOT NULL,
            bot_transferred_tickets INT NOT NULL,
            blank_chat_tickets INT NOT NULL,
            cancelled_existing_ticket_tickets INT NOT NULL,
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
            cancelled_existing_ticket_rate DOUBLE NOT NULL
        )
    """,
    "agg_voc_mismatch": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            product_family VARCHAR(100) NOT NULL,
            fault_code_level_2 VARCHAR(150) NOT NULL,
            diagnosed_defect VARCHAR(255) NOT NULL,
            tickets INT NOT NULL,
            mismatch_rate DOUBLE NOT NULL
        )
    """,
    "agg_anomalies": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            detected_at DATE NOT NULL,
            product_family VARCHAR(100) NOT NULL,
            fault_code VARCHAR(100) NOT NULL,
            department_name VARCHAR(100) NOT NULL,
            current_14d INT NOT NULL,
            baseline_60d DOUBLE NOT NULL,
            anomaly_score DOUBLE NOT NULL
        )
    """,
    "agg_health_score": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            metric_date DATE NOT NULL,
            health_score DOUBLE NOT NULL,
            repair_field_rate DOUBLE NOT NULL,
            repeat_rate DOUBLE NOT NULL,
            bot_deflection_rate DOUBLE NOT NULL
        )
    """,
    "agg_data_quality": """
        CREATE TABLE IF NOT EXISTS {table_name} (
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
            email_department_reassigned_tickets INT NOT NULL
        )
    """,
    "agg_model_breakdown": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            product_family VARCHAR(100) NOT NULL,
            canonical_model VARCHAR(150) NOT NULL,
            tickets INT NOT NULL,
            repair_field_visit_rate DOUBLE NOT NULL,
            repeat_rate DOUBLE NOT NULL,
            bot_deflection_rate DOUBLE NOT NULL,
            bot_transfer_rate DOUBLE NOT NULL,
            blank_chat_rate DOUBLE NOT NULL
        )
    """,
    "pipeline_log": """
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_started_at DATETIME NOT NULL,
            run_finished_at DATETIME NOT NULL,
            duration_minutes INT NOT NULL,
            status VARCHAR(30) NOT NULL,
            job_name VARCHAR(100) NOT NULL,
            source_rows INT NOT NULL,
            message VARCHAR(255) NOT NULL
        )
    """,
}
