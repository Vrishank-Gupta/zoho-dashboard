-- Tickets over time
SELECT
    metric_date,
    sum(tickets) AS tickets,
    sum(repair_field_tickets) AS repair_field_visits,
    sum(bot_resolved_tickets) AS bot_resolved
FROM qubo_analytics.tickets_daily_summary
WHERE metric_date BETWEEN toDate('2026-02-01') AND toDate('2026-03-31')
  AND product_family = 'Dash Cam'
GROUP BY metric_date
ORDER BY metric_date;

-- Bot outcome split
SELECT
    bot_outcome,
    sum(tickets) AS tickets
FROM qubo_analytics.tickets_daily_summary
WHERE metric_date BETWEEN toDate('2026-02-01') AND toDate('2026-03-31')
  AND channel = 'Chat'
GROUP BY bot_outcome
ORDER BY tickets DESC;

-- Top issues
SELECT
    product_family,
    fault_code,
    fault_code_level_1,
    fault_code_level_2,
    sum(tickets) AS tickets,
    sum(repair_field_tickets) / nullIf(sum(tickets), 0) AS repair_rate,
    sum(repeat_tickets) / nullIf(sum(tickets), 0) AS repeat_rate
FROM qubo_analytics.issues_daily_summary
WHERE metric_date BETWEEN toDate('2026-02-01') AND toDate('2026-03-31')
GROUP BY product_family, fault_code, fault_code_level_1, fault_code_level_2
ORDER BY tickets DESC
LIMIT 20;

-- Category and product breakdown
SELECT
    fault_code,
    product_family,
    sum(tickets) AS tickets,
    sum(bot_resolved_tickets) / nullIf(sum(tickets), 0) AS bot_resolved_rate
FROM qubo_analytics.tickets_daily_summary
WHERE metric_date BETWEEN toDate('2026-02-01') AND toDate('2026-03-31')
GROUP BY fault_code, product_family
ORDER BY tickets DESC
LIMIT 50;

-- Drilldown table
SELECT
    ticket_id,
    created_at,
    canonical_product AS product_family,
    normalized_department AS department,
    normalized_channel AS channel,
    normalized_fault_code AS fault_code,
    normalized_fault_code_l1 AS fault_code_level_1,
    normalized_fault_code_l2 AS fault_code_level_2,
    normalized_resolution AS resolution,
    status,
    symptom,
    defect,
    repair
FROM qubo_analytics.tickets_fact_recent FINAL
WHERE created_date BETWEEN toDate('2026-02-01') AND toDate('2026-03-31')
  AND canonical_product = 'Dash Cam'
  AND normalized_fault_code = 'Product issue'
ORDER BY created_at DESC
LIMIT 100;
