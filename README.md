# Qubo Support Health Command Center

This repo is now split into two deployable parts:

- `qubo_dashboard/` - Python API backend and pipeline
- `frontend/` - static dashboard frontend for Netlify, Vercel, S3, or any static host

## Architecture

- Backend:
  - FastAPI API
  - reads raw Zoho tickets from the remote MySQL source
  - writes aggregates into the local analytics MySQL database
  - serves dashboard APIs and ticket drilldown
- Frontend:
  - plain HTML, CSS, and JS
  - talks to the backend through `frontend/config.js`
  - can be deployed independently from the backend

## Backend deployment

Backend is intended to run on a VM or container host.

Recommended:

- Docker on the VM
- reverse proxy with HTTPS
- frontend hosted separately

### Backend environment

Copy `.env.example` to `.env` and fill in values.

For production handoff, DevOps should start from:

- [deploy/.env.backend.production.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/.env.backend.production.example)

Important backend settings:

```env
QUBO_APP_HOST=0.0.0.0
QUBO_APP_PORT=8000
QUBO_APP_RELOAD=false
QUBO_SERVE_FRONTEND=false
QUBO_USE_SAMPLE_DATA=false
QUBO_PIPELINE_RECREATE_TABLES=false
QUBO_CORS_ALLOWED_ORIGINS=https://your-frontend-domain.netlify.app
```

Database mappings:

- `QUBO_ZOHO_*` = remote Zoho source table
- `QUBO_AGG_*` = local aggregate analytics database
- aggregate table names are also env-driven for:
  - hourly heatmap
  - VOC mismatch

### Run backend locally

```bash
pip install -r requirements.txt
python -m uvicorn qubo_dashboard.main:app --host 127.0.0.1 --port 8010 --reload
```

API health:

- `http://127.0.0.1:8010/api/health`

### Run backend with Docker

```bash
docker compose up --build
```

The backend container exposes port `8000`.

## Frontend deployment

The frontend is fully static.

Files:

- `frontend/index.html`
- `frontend/styles.css`
- `frontend/app.js`
- `frontend/config.js`

### Configure the frontend API URL

Edit `frontend/config.js` before deployment:

```js
window.QUBO_APP_CONFIG = {
  apiBaseUrl: "https://api-support.example.com",
};
```

`frontend/config.example.js` is included as a template.

For production handoff, DevOps should start from:

- [frontend.config.production.example.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/frontend.config.production.example.js)

### Static hosting

You can deploy the `frontend/` directory directly to:

- Netlify
- Vercel
- S3 + CloudFront
- Nginx static hosting

`netlify.toml` is included for Netlify and publishes `frontend/`.

## Pipeline

The pipeline is part of the backend codebase.

Manual CLI run:

```bash
python -m qubo_dashboard.pipeline.run
```

Manual dashboard-triggered run:

- the frontend has a `Run pipeline` button
- it calls `POST /api/pipeline/run`
- it is open in the current internal deployment model

Production recommendation:

- schedule the CLI pipeline every 15 to 60 minutes
- keep the dashboard button as an admin fallback

Pipeline status endpoint:

- `GET /api/pipeline/status`

## SQL bootstrap

Run the aggregate schema bootstrap against the aggregate MySQL database:

- [bootstrap_analytics.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/sql/bootstrap_analytics.sql)

This creates:

- `agg_daily_tickets`
- `agg_fc_weekly`
- `agg_sw_version`
- `agg_resolution`
- `agg_channel`
- `agg_hourly_heatmap`
- `agg_replacements`
- `agg_bot`
- `agg_voc_mismatch`
- `agg_anomalies`
- `agg_health_score`
- `agg_data_quality`
- `pipeline_log`

## DevOps handoff

Use:

- [DEVOPS_CHECKLIST.md](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/DEVOPS_CHECKLIST.md)
- [DEVOPS_DEPLOYMENT_GUIDE.md](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/DEVOPS_DEPLOYMENT_GUIDE.md)

That file lists:

- VM requirements
- Docker expectations
- MySQL access requirements
- firewall and CORS requirements
- secrets needed
- frontend hosting requirements
- monitoring expectations

## Current production-facing endpoints

- `GET /api/health`
- `GET /api/dashboard`
- `GET /api/issues/{issue_id}`
- `GET /api/tickets`
- `GET /api/pipeline/status`
- `POST /api/pipeline/run`

## Notes

- The backend defaults to API-only mode. Set `QUBO_SERVE_FRONTEND=true` only if you intentionally want FastAPI to serve the static dashboard files.
- The pipeline and dashboard are generalized against incoming ticket data. The product rollups and issue cleaning are rule-based, not hardcoded to current row counts.
- Version analysis remains limited by source coverage if software version is missing upstream.
