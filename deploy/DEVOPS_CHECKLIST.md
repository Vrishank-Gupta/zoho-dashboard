# Qubo Dashboard Deployment Checklist

## 1. Backend VM

- Linux VM with Docker Engine and Docker Compose plugin
- 2 vCPU minimum, 4 vCPU preferred
- 4 GB RAM minimum, 8 GB preferred
- Static outbound access to:
  - remote Zoho MySQL host and port
  - local or managed aggregate MySQL host and port
- Inbound HTTPS only through reverse proxy or load balancer

## 2. Domains and TLS

- One backend API hostname, for example `qubo-support-api.yourdomain.com`
- TLS certificate for the API hostname
- Optional internal-only routing or network restriction for `/api/pipeline/run`

## 3. Aggregate MySQL

- Dedicated database for aggregate tables
- MySQL user with:
  - `CREATE`
  - `ALTER`
  - `DROP`
  - `INDEX`
  - `INSERT`
  - `UPDATE`
  - `DELETE`
  - `SELECT`
- Run [bootstrap_analytics.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/sql/bootstrap_analytics.sql)

## 4. Zoho Source Access

- Host, port, username, password, database, and table name for the remote Zoho MySQL source
- Firewall rule allowing the backend VM to connect outbound to that source
- Read-only MySQL user is sufficient for the source

## 5. Environment Variables

Backend needs these secrets and settings:

- `QUBO_APP_HOST=0.0.0.0`
- `QUBO_APP_PORT=8000`
- `QUBO_APP_RELOAD=false`
- `QUBO_SERVE_FRONTEND=false`
- `QUBO_USE_SAMPLE_DATA=false`
- `QUBO_PIPELINE_RECREATE_TABLES=false`
- `QUBO_CORS_ALLOWED_ORIGINS=<frontend-domain>`
- `QUBO_ZOHO_DB_HOST`
- `QUBO_ZOHO_DB_PORT`
- `QUBO_ZOHO_DB_USER`
- `QUBO_ZOHO_DB_PASSWORD`
- `QUBO_ZOHO_DB_NAME`
- `QUBO_ZOHO_TICKET_TABLE`
- `QUBO_AGG_DB_HOST`
- `QUBO_AGG_DB_PORT`
- `QUBO_AGG_DB_USER`
- `QUBO_AGG_DB_PASSWORD`
- `QUBO_AGG_DB_NAME`
- `QUBO_AGG_DAILY_TICKETS_TABLE`
- `QUBO_AGG_FC_WEEKLY_TABLE`
- `QUBO_AGG_SW_VERSION_TABLE`
- `QUBO_AGG_RESOLUTION_TABLE`
- `QUBO_AGG_CHANNEL_TABLE`
- `QUBO_AGG_BOT_TABLE`
- `QUBO_AGG_HOURLY_HEATMAP_TABLE`
- `QUBO_AGG_REPLACEMENTS_TABLE`
- `QUBO_AGG_VOC_MISMATCH_TABLE`
- `QUBO_AGG_ANOMALIES_TABLE`
- `QUBO_AGG_HEALTH_SCORE_TABLE`
- `QUBO_AGG_DATA_QUALITY_TABLE`
- `QUBO_PIPELINE_LOG_TABLE`

## 6. Backend Runtime

- Deploy the repo as a Docker container using [Dockerfile](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/Dockerfile)
- Reverse proxy `/` and `/api/*` to the container if you want a single backend domain
- Health endpoint for checks:
  - `/api/health`

## 7. Frontend Hosting

- Deploy the `frontend/` directory to Netlify, Vercel, S3, or any static host
- Before deploy, set [config.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/frontend/config.js) to the production backend API URL
- Production-ready templates are available here:
  - [deploy/.env.backend.production.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/.env.backend.production.example)
  - [frontend.config.production.example.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/frontend.config.production.example.js)
- Example:
  - `apiBaseUrl: "https://qubo-support-api.yourdomain.com"`

## 8. Pipeline Execution

- Either:
  - schedule `python -m qubo_dashboard.pipeline.run` on the backend host
- Or:
  - let internal users trigger the pipeline from the dashboard using `/api/pipeline/run`

Preferred production setup:

- scheduled refresh via cron or systemd timer every 15 to 60 minutes
- dashboard button kept as manual fallback

## 9. Monitoring

- Capture container stdout/stderr
- Alert if `/api/health` fails
- Alert if `pipeline_log` shows repeated failures
- Track average pipeline duration and source row count drift
