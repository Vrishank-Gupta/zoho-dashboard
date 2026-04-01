# ClickHouse Analytics Blueprint

## Architecture

Single-VM deployment:

1. MySQL stays the source of truth.
2. `qubo-clickhouse-etl` runs nightly inside Docker with APScheduler.
3. ETL reads changed rows from MySQL using `Modified_Time`.
4. ETL upserts raw versions into `tickets_fact_recent` in ClickHouse.
5. ETL rebuilds only affected dates in:
   - `tickets_daily_summary`
   - `issues_daily_summary`
6. FastAPI reads ClickHouse only for dashboard, drilldown, and ticket search.
7. Frontend keeps calling the same API container.

Flow:

`MySQL -> incremental ETL -> ClickHouse fact -> ClickHouse summaries -> FastAPI -> frontend`

## Docker Setup

Services in `docker-compose.yml`:

- `qubo-clickhouse`
- `qubo-dashboard-api`
- `qubo-clickhouse-etl`

Recommended for one VM:

- Keep ClickHouse data on a named Docker volume.
- Keep ETL and API as separate containers so scheduler failures do not restart the API.
- Let both API and ETL talk to ClickHouse over the Compose network.

## ClickHouse Tables

Implemented in:

- `deploy/sql/clickhouse_bootstrap.sql`
- `qubo_dashboard/clickhouse_analytics/schema.py`

Tables:

- `tickets_fact_recent`
  - Engine: `ReplacingMergeTree(ingest_version)`
  - Partition: `toYYYYMM(created_date)`
  - Order key: `(created_date, canonical_product, normalized_fault_code, ticket_id)`
  - Purpose: append-only upsert target for changed source rows

- `tickets_daily_summary`
  - Engine: `MergeTree`
  - Partition: `toYYYYMM(metric_date)`
  - Order key: `(metric_date, product_family, fault_code, fault_code_level_1, fault_code_level_2, department_name, channel, bot_outcome)`
  - Purpose: date/category/product/channel/bot-outcome level dashboard scans

- `issues_daily_summary`
  - Engine: `MergeTree`
  - Partition: `toYYYYMM(metric_date)`
  - Order key: `(metric_date, product_family, fault_code, fault_code_level_1, fault_code_level_2, department_name, channel)`
  - Purpose: top-issues and issue-trend queries

Low cardinality is applied to product, category, department, channel, bot outcome, and version-like string dimensions to reduce storage and speed filters.

## Incremental ETL Design

Implemented in `qubo_dashboard/clickhouse_analytics/etl.py`.

Rules:

- First successful run:
  - backfill last `QUBO_ETL_BACKFILL_DAYS` days
- Next runs:
  - start from `last_successful_sync - overlap_buffer`
- Watermark:
  - stored in `etl_sync_state.last_successful_sync`
- Overlap buffer:
  - `QUBO_ETL_OVERLAP_HOURS`, default `24`
- Change detection:
  - uses MySQL `Modified_Time`
- Duplicate safety:
  - raw fact writes are idempotent because `ReplacingMergeTree` keeps the latest `ingest_version`
- Aggregate rebuild:
  - detect affected `created_date` values from changed rows
  - delete only those dates from summary tables
  - reinsert summaries from `tickets_fact_recent FINAL`

Why this works:

- reruns can safely re-read the same overlap window
- partial failures do not corrupt the source of truth
- summaries are deterministic from the deduplicated fact table

## Scheduler

Chosen approach: APScheduler inside the ETL container.

Why:

- simpler on one VM than host cron plus container exec
- config lives with the ETL code
- easy manual replay with the same container image

Default schedule:

- `QUBO_ETL_SCHEDULE=0 2 * * *`
- `QUBO_ETL_TIMEZONE=Asia/Calcutta`

Entrypoint:

- `python -m qubo_dashboard.clickhouse_analytics.scheduler`

Manual trigger:

```bash
docker compose run --rm qubo-clickhouse-etl python -m qubo_dashboard.clickhouse_analytics.run_once
```

Debug failures:

```bash
docker compose logs -f qubo-clickhouse-etl
docker compose exec qubo-clickhouse bash
docker compose exec qubo-dashboard-api curl -s http://127.0.0.1:8000/api/health
```

State and logs:

- `etl_sync_state`
- `etl_run_log`

## Dashboard Query Path

FastAPI now prefers ClickHouse when:

- `QUBO_ANALYTICS_BACKEND=clickhouse`
- ClickHouse config is present

Code:

- `qubo_dashboard/clickhouse_analytics/dashboard.py`
- `qubo_dashboard/analytics.py`

The API no longer needs live MySQL dashboard reads in ClickHouse mode. MySQL remains extract-only.

## Key Design Decisions

- ClickHouse over MySQL:
  - columnar scans, fast group-bys, compression, and much better filter performance at millions of rows
- `ReplacingMergeTree` for fact:
  - simplest safe upsert model for incremental loads
- `MergeTree` for summaries:
  - summaries are rebuilt per affected date, so they do not need row-version replacement
- APScheduler over host cron:
  - fewer moving pieces on a single VM, and manual runs use the same code path
- daily summaries instead of full raw scans:
  - most dashboard filters hit pre-aggregated tables, with the fact table reserved for drilldown and ticket search
