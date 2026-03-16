# Qubo Support Health Command Center
# DevOps Deployment Guide

This document is the complete handoff for deploying the Qubo Support Health dashboard.

The application is split into:

- Backend API and pipeline:
  - Python FastAPI application
  - reads raw Zoho ticket data from a remote MySQL source
  - writes aggregate analytics tables into a MySQL analytics database
- Frontend:
  - static HTML/CSS/JS app
  - hosted separately on a static hosting platform such as Netlify
  - calls the backend API over HTTPS

This guide is written so DevOps can deploy the full stack end to end.

## 0. Production deployment model

This application is intended to run in production as two separate deployments:

- Frontend:
  - static site
  - hosted on Netlify or equivalent
  - example URL: `https://qubo-support.companydomain.com`
- Backend:
  - FastAPI API and pipeline service
  - hosted on a VM or container host
  - example URL: `https://qubo-support-api.companydomain.com`

The frontend does not need to be served by the backend in production.

The frontend calls the backend API using `frontend/config.js`.

Example:

```js
window.QUBO_APP_CONFIG = {
  apiBaseUrl: "https://qubo-support-api.companydomain.com",
};
```

The backend must allow that frontend origin through:

```env
QUBO_CORS_ALLOWED_ORIGINS=https://qubo-support.companydomain.com
```

## 1. Current architecture

### Backend responsibilities

- Expose dashboard APIs
- Expose ticket drilldown APIs
- Run the analytics pipeline
- Read source data from the remote Zoho MySQL database
- Write aggregate tables into the analytics MySQL database

### Frontend responsibilities

- Load the dashboard UI
- Call backend APIs
- Trigger the pipeline manually from the UI

### Deployment split

- Backend should run on a VM or container host
- Frontend should run on a static hosting service
- Frontend and backend communicate over HTTPS
- This is the expected production architecture, not just a local development convenience

## 2. What DevOps needs from the business / engineering team

DevOps will need the following before deployment starts.

### A. Infrastructure / account access

- AWS account access or the target cloud account access
- The correct AWS account ID and region
- IAM user or role with permission to:
  - create or manage EC2
  - create or manage security groups
  - create or manage Route53 records if DNS is in AWS
  - create or manage ACM certificates if TLS is handled in AWS
  - create or manage load balancer or reverse proxy infrastructure if needed
- SSH or SSM access model for the VM

If AWS is not being used, DevOps needs equivalent access on the chosen cloud or hosting environment.

### B. Domain and DNS

- Backend API subdomain
  - example: `qubo-support-api.companydomain.com`
- Frontend dashboard subdomain
  - example: `qubo-support.companydomain.com`
- DNS management access or a request path to the DNS owner

### C. Database access

#### Remote Zoho MySQL source

- Host
- Port
- Database name
- Username
- Password
- Table name
  - expected: `Call_Driver_Data_Zoho_FromAug2024`
- Firewall rule / allowlist support so the backend VM can connect

#### Analytics MySQL database

- Host
- Port
- Database name
- Username
- Password
- Permission to create and manage tables in that database

### D. Application env values

- `QUBO_CORS_ALLOWED_ORIGINS`
  - frontend production domain
- all `QUBO_ZOHO_*` values
- all `QUBO_AGG_*` values

### E. OpenAI API key

Current state:

- this dashboard does not require an OpenAI API key to run today
- there is no production dependency in the current code that calls OpenAI

Ask DevOps for an OpenAI API key only if you want to add:

- AI-generated summaries
- issue narrative generation
- action recommendation generation
- natural language search or copilots

If AI features are planned, request:

- one OpenAI API key stored in secret management
- usage/budget guardrails
- approved outbound internet access from backend to OpenAI APIs

For current deployment, this is optional and not required.

## 3. What DevOps should provision

## Backend host

Recommended:

- 1 Linux VM
- Ubuntu 22.04 or similar
- Docker Engine installed
- Docker Compose plugin installed

Recommended size:

- minimum: 2 vCPU, 4 GB RAM
- preferred: 4 vCPU, 8 GB RAM

Why Docker is suitable here:

- backend is a single Python service
- dependencies are simple
- easier repeatable deployment
- easier rollback
- easier environment separation

## Frontend host

Recommended:

- Netlify for the static frontend

Alternatives:

- Vercel
- S3 + CloudFront
- static Nginx hosting

## Databases

- Remote Zoho MySQL source remains where it is
- Aggregate analytics MySQL should be reachable from the backend VM

## TLS / reverse proxy

One of these is needed:

- Nginx on the VM with TLS
- managed load balancer with TLS in front of the container

## Monitoring

- container logs
- API health check
- pipeline failure alerts
- disk usage checks
- CPU/RAM monitoring

## 4. Files DevOps should use from this repo

- Backend container:
  - [Dockerfile](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/Dockerfile)
  - [docker-compose.yml](C:/Users/User/OneDrive%20-%20Hero%20Ltd/Desktop/Bot%20Analytics/docker-compose.yml)
- Backend env template:
  - [.env.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/.env.example)
  - [.env.backend.production.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/.env.backend.production.example)
- Aggregate DB bootstrap SQL:
  - [bootstrap_analytics.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/sql/bootstrap_analytics.sql)
- Frontend static files:
  - [index.html](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/frontend/index.html)
  - [styles.css](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/frontend/styles.css)
  - [app.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/frontend/app.js)
  - [config.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/frontend/config.js)
  - [frontend.config.production.example.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/frontend.config.production.example.js)
- Netlify config:
  - [netlify.toml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/netlify.toml)

## 4A. Production URL example

Use a setup like this:

- Frontend:
  - `https://qubo-support.companydomain.com`
- Backend API:
  - `https://qubo-support-api.companydomain.com`

Frontend config:

```js
window.QUBO_APP_CONFIG = {
  apiBaseUrl: "https://qubo-support-api.companydomain.com",
};
```

Backend CORS:

```env
QUBO_CORS_ALLOWED_ORIGINS=https://qubo-support.companydomain.com
```

This is the key contract between the two deployments.

## 5. Environment variables DevOps must configure

These go into the backend environment.

```env
QUBO_APP_HOST=0.0.0.0
QUBO_APP_PORT=8000
QUBO_APP_RELOAD=false
QUBO_SERVE_FRONTEND=false
QUBO_USE_SAMPLE_DATA=false
QUBO_PIPELINE_RECREATE_TABLES=false
QUBO_CORS_ALLOWED_ORIGINS=https://your-frontend-domain.netlify.app

QUBO_ZOHO_DB_HOST=<remote-source-host>
QUBO_ZOHO_DB_PORT=3306
QUBO_ZOHO_DB_USER=<remote-source-user>
QUBO_ZOHO_DB_PASSWORD=<remote-source-password>
QUBO_ZOHO_DB_NAME=<remote-source-db>
QUBO_ZOHO_TICKET_TABLE=Call_Driver_Data_Zoho_FromAug2024

QUBO_AGG_DB_HOST=<analytics-db-host>
QUBO_AGG_DB_PORT=3306
QUBO_AGG_DB_USER=<analytics-db-user>
QUBO_AGG_DB_PASSWORD=<analytics-db-password>
QUBO_AGG_DB_NAME=<analytics-db-name>

QUBO_AGG_DAILY_TICKETS_TABLE=agg_daily_tickets
QUBO_AGG_FC_WEEKLY_TABLE=agg_fc_weekly
QUBO_AGG_SW_VERSION_TABLE=agg_sw_version
QUBO_AGG_RESOLUTION_TABLE=agg_resolution
QUBO_AGG_CHANNEL_TABLE=agg_channel
QUBO_AGG_BOT_TABLE=agg_bot
QUBO_AGG_HOURLY_HEATMAP_TABLE=agg_hourly_heatmap
QUBO_AGG_REPLACEMENTS_TABLE=agg_replacements
QUBO_AGG_VOC_MISMATCH_TABLE=agg_voc_mismatch
QUBO_AGG_ANOMALIES_TABLE=agg_anomalies
QUBO_AGG_HEALTH_SCORE_TABLE=agg_health_score
QUBO_AGG_DATA_QUALITY_TABLE=agg_data_quality
QUBO_PIPELINE_LOG_TABLE=pipeline_log
```

## 6. Step-by-step deployment

### Step 1. Provision the backend VM

- Create Linux VM
- Open inbound HTTPS or reverse proxy port only
- Allow outbound connections to:
  - remote Zoho MySQL host
  - analytics MySQL host

### Step 2. Install base software on VM

Install:

- Docker Engine
- Docker Compose plugin

Optional but useful:

- Nginx
- certbot if TLS is terminated on the VM

### Step 3. Put the application on the VM

Options:

- clone the repo
- copy the deployment package onto the VM

Place the application in a stable directory, for example:

```bash
/opt/qubo-support-dashboard
```

### Step 4. Create the backend env file

Create:

```bash
/opt/qubo-support-dashboard/.env
```

Populate it with the backend values listed earlier.

DevOps can start from:

- [deploy/.env.backend.production.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/.env.backend.production.example)

### Step 5. Create the analytics tables

Run [bootstrap_analytics.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/sql/bootstrap_analytics.sql) against the analytics MySQL database.

Example:

```bash
mysql -h <agg-host> -u <agg-user> -p <agg-db> < deploy/sql/bootstrap_analytics.sql
```

### Step 6. Start the backend container

From the project root on the VM:

```bash
docker compose up --build -d
```

This starts the backend API container.

### Step 7. Validate backend health

Check:

```bash
curl http://127.0.0.1:8000/api/health
```

Expected:

- API responds successfully
- Zoho DB shows configured
- aggregate DB shows configured

### Step 8. Set up reverse proxy and HTTPS

Expose the backend at the API domain, for example:

```text
https://qubo-support-api.companydomain.com
```

Reverse proxy should forward to:

```text
http://127.0.0.1:8000
```

Do not expose MySQL publicly as part of this deployment.

Only the backend API should be internet-facing.

### Step 9. Run the first pipeline

Run:

```bash
docker compose exec qubo-dashboard-api python -m qubo_dashboard.pipeline.run
```

or if running outside compose:

```bash
python -m qubo_dashboard.pipeline.run
```

Validate:

- aggregate tables are populated
- `pipeline_log` has a success row

### Step 10. Schedule the pipeline

Preferred approach:

- schedule pipeline every 15 to 60 minutes

Recommended options:

- cron
- systemd timer
- CI/CD scheduled job that hits the backend

Best production pattern:

- regular scheduled CLI run
- manual dashboard button remains available as backup

Example cron:

```cron
*/30 * * * * cd /opt/qubo-support-dashboard && docker compose exec -T qubo-dashboard-api python -m qubo_dashboard.pipeline.run >> /var/log/qubo-pipeline.log 2>&1
```

### Step 11. Prepare the frontend for deployment

Edit [config.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/frontend/config.js):

```js
window.QUBO_APP_CONFIG = {
  apiBaseUrl: "https://qubo-support-api.companydomain.com",
};
```

DevOps can start from:

- [frontend.config.production.example.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/frontend.config.production.example.js)

### Step 12. Deploy the frontend

For Netlify:

- point Netlify to this repo or to the `frontend/` directory artifact
- publish directory:
  - `frontend`
- `netlify.toml` is already included

Frontend production URL example:

```text
https://qubo-support.companydomain.com
```

Important:

- frontend must be deployed separately from the backend
- frontend must be configured to call the backend API domain
- backend does not need to host frontend files in production

### Step 13. Set backend CORS

Backend env must include:

```env
QUBO_CORS_ALLOWED_ORIGINS=https://qubo-support.companydomain.com
```

If there are multiple frontend domains, provide them comma-separated.

### Step 14. Validate end-to-end

Check:

- frontend loads
- dashboard API loads successfully
- issue drawer loads ticket evidence
- pipeline status endpoint works
- pipeline can be run manually from UI

## 7. What to ask DevOps explicitly

This is the concise request list you can send them.

### Please provision

- one Linux VM for the backend
- Docker Engine and Docker Compose on that VM
- one production backend API domain with HTTPS
- one production frontend domain on Netlify or equivalent
- access from the VM to:
  - remote Zoho MySQL
  - analytics MySQL

### Please obtain / configure

- AWS or target cloud account access
- DNS access or DNS request support
- backend env secrets
- analytics DB credentials
- remote Zoho source DB credentials

### Please deploy

- backend container using this repo
- aggregate DB schema using [bootstrap_analytics.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop/Bot%20Analytics/deploy/sql/bootstrap_analytics.sql)
- frontend static files from `frontend/`
- pipeline scheduler
- frontend configured to point to the production backend API domain
- backend configured with CORS allowing only the production frontend domain

### Please monitor

- `/api/health`
- container logs
- pipeline failures
- pipeline runtime

## 8. Security notes

- Do not store secrets in the repo
- Store backend env values in a secret manager or secure VM env file
- Consider internal-only exposure or network restriction for `/api/pipeline/run` if the system moves beyond the internal audience
- Use least-privilege DB users:
  - source DB user should be read-only
  - analytics DB user needs table management and write access
- MySQL ports should remain private and reachable only from approved hosts

## 9. Optional future additions

These are not required for the current deployment, but DevOps may ask.

### OpenAI integration

Only needed if AI features are added later.

Would require:

- OpenAI API key
- outbound internet access to OpenAI
- budget controls
- secret storage

### Object storage / backups

Optional for:

- exporting snapshots
- retaining pipeline outputs
- long-term archival

### CI/CD pipeline

Optional but recommended:

- build backend container on merge
- deploy container to VM
- deploy frontend to Netlify automatically

## 10. Deployment sign-off checklist

Deployment is complete only when all of the below are true:

- backend API is reachable over HTTPS
- frontend is reachable over HTTPS
- frontend can call backend successfully
- frontend is configured to the production backend API URL
- backend CORS matches the frontend production domain
- aggregate DB schema exists
- first pipeline run succeeds
- dashboard shows live warehouse data
- issue drilldown works
- manual pipeline trigger works
- scheduled pipeline refresh is active
- monitoring is enabled
