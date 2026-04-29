# Zoho Dashboard Handoff

## What This Project Is

This repo powers the Qubo support dashboard:

- Frontend: dark-theme executive dashboard UI in `frontend/`
- Backend API: FastAPI app in `qubo_dashboard/`
- Analytics engine: ClickHouse-backed dashboard + drilldowns
- ETL: MySQL source table -> ClickHouse fact/summaries

The project now includes:

- Device model as a first-class analytical dimension
- Firmware/software version plumbing in code
- Workspace-style drilldowns for product/category/issue/repeat
- Structured logging support in app code

## Repo To Copy

Copy the full repo folder:

- `C:\Users\vrish\Downloads\scraper_refactored\zoho-dashboard`

That gives the new laptop:

- frontend code
- backend code
- ETL code
- Dockerfiles
- requirements
- deploy artifacts/templates
- handoff context

## Files/Folders To Copy Manually

These are the important non-git things to move manually.

### 1. SSH key used for prod

Copy the PEM/key file used for prod SSH from the old laptop to the new laptop.

Current key path used in this project:

- `C:\Users\vrish\.ssh\heroiot-prod-voc-qubo-support-mkt-.pem`

Also preserve permissions and keep it outside the repo.

### 2. Any `.env` / production env files

If the project depends on env files not committed to git, copy them manually.

Examples to check for on old laptop or prod bundle workflow:

- `.env`
- `.env.backend.production`
- any local override env file used for deploy

These are required for:

- MySQL source DB connectivity
- ClickHouse connectivity
- CORS / serve mode
- mapping workbook path

### 3. Mapping workbook, if external

If `QUBO_MAPPING_WORKBOOK` points to a local workbook path, copy that workbook too.

### 4. Any private notes / screenshots / external HTML references

If you still need them for UI reference, move:

- `C:\Users\vrish\Downloads\qubo_cxo_dashboard_enhanced.html`

and any other design references.

## Prod Architecture

Production app path:

- `/opt/zoho-dashboard`

Important: operate only on `/opt/zoho-dashboard`

Do not touch:

- `/home/ec2-user/zoho-dashboard_prev_*`
- any unrelated projects on the VM

Main prod containers:

- `qubo-dashboard-api`
- `qubo-frontend-final`
- `qubo-clickhouse-etl`
- `qubo-clickhouse`

Ports:

- frontend exposed on host port `81`
- API exposed on host port `8020`
- app inside container listens on `8001`

## Important Prod Gotchas

### 1. Container internal port is 8001

When manually running frontend/API containers, map host -> container as:

- `81:8001`
- `8020:8001`

Not `8000`.

### 2. ClickHouse credentials are not fully present in `.env.backend.production`

During earlier deploy work, ClickHouse auth had to be sourced from the running ClickHouse container env rather than relying only on `.env.backend.production`.

So if API/ETL starts but ClickHouse shows as not configured, check:

- `QUBO_CLICKHOUSE_HOST`
- `QUBO_CLICKHOUSE_PORT`
- `QUBO_CLICKHOUSE_DATABASE`
- `QUBO_CLICKHOUSE_USER`
- `QUBO_CLICKHOUSE_PASSWORD`

### 3. SSH to prod may intermittently time out

This happened during prior deploy attempts. If deployment blocks, first verify SSH reachability before assuming code issues.

## Current Code State

### Already implemented locally

- firmware/software version support in source ingestion
- ClickHouse schema updates for software version
- firmware filtering/query plumbing
- firmware drilldown panels replacing placeholder copy
- structured app logging:
  - `logs/access.log`
  - `logs/audit.log`
  - `logs/runtime.log`

### Logging behavior added

- request ID added to responses
- request access logs with duration/status/IP/header subset
- audit logs for:
  - pipeline run
  - pipeline status view
  - mapping save
  - mapping CSV upload/download

## Local Validation Commands

Run these on the new laptop after setup:

```powershell
node --check frontend\app.js
python -m py_compile qubo_dashboard\config.py qubo_dashboard\repository.py qubo_dashboard\clickhouse_analytics\etl.py qubo_dashboard\clickhouse_analytics\schema.py qubo_dashboard\clickhouse_analytics\dashboard.py qubo_dashboard\analytics.py qubo_dashboard\main.py qubo_dashboard\models.py qubo_dashboard\logging_utils.py qubo_dashboard\run.py
```

## Deploy Flow

Typical deploy pattern used:

1. Build a tar bundle locally
2. Upload bundle to prod VM
3. Extract into `/opt/zoho-dashboard`
4. Rebuild Docker images
5. Restart only:
   - `qubo-dashboard-api`
   - `qubo-frontend-final`
   - `qubo-clickhouse-etl`
6. Verify:
   - `/api/health`
   - frontend asset version
   - dashboard payload
7. Run ETL sync/backfill if schema/dimensions changed

## Firmware Backfill Note

Because older ClickHouse fact rows were loaded before `software_version` was stored, code deploy alone is not enough. A backfill/reload is needed so historical firmware data appears in summaries and drilldowns.

There is a temporary local helper script created during work:

- `.tmp_run_full_firmware_backfill.py`

It is a one-off helper, not part of normal app runtime.

## Recommended Setup On New Laptop

1. Install:
   - Python
   - Node.js
   - Git
   - Docker Desktop (if needed locally)
   - OpenSSH client

2. Copy:
   - full repo
   - PEM key
   - env files
   - mapping workbook

3. Open repo and install Python dependencies:

```powershell
pip install -r requirements.txt
```

4. Verify local syntax with the validation commands above

5. Test SSH manually before any prod deploy

## What To Tell Codex On The New Laptop

Use this kind of bootstrapping prompt first:

> This is the Qubo Zoho support dashboard project. Read `PROJECT_HANDOFF.md` first, then inspect the repo. Only operate on `/opt/zoho-dashboard` in prod. Do not touch unrelated VM folders or projects. The dashboard uses FastAPI + ClickHouse + ETL from MySQL. Continue from current local repo state.

That will save a lot of re-explaining.

## Best Practice For Context Transfer

Do not rely only on chat history.

Transfer context using:

1. this repo
2. `PROJECT_HANDOFF.md`
3. env files / keys / workbook
4. a short current-status note like:

- what is deployed
- what is pending
- what broke last
- what SSH/prod issue existed

## Suggested Final Manual Checklist

- Copy repo folder
- Copy prod PEM key
- Copy env files
- Copy mapping workbook
- Install Python/Node/Docker/Git
- Run local validation
- Verify SSH to prod
- Tell Codex to read `PROJECT_HANDOFF.md` first

