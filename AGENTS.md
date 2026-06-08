# Agent Notes

Read these before making or deploying changes.

## Start Here

1. Read `PROJECT_HANDOFF.md` for project context.
2. Read `DEPLOYMENT_REFERENCE.md` for current production access and deploy commands.
3. Inspect the repo before editing. Prefer existing patterns.

## Current Production Access

Use:

```powershell
ssh -i "$HOME\.ssh\analytics-report-key.pem" ec2-user@ec2-15-207-57-132.ap-south-1.compute.amazonaws.com
```

The active Docker Compose working directory is:

```text
/home/ec2-user/zoho-dashboard
```

Do not assume `/opt/zoho-dashboard` is active. It exists, but the running containers currently point to `/home/ec2-user/zoho-dashboard`.

## Running Services

Production containers:

```text
qubo-frontend-final
qubo-dashboard-api
qubo-clickhouse-etl
qubo-clickhouse
```

Host ports:

```text
frontend: 81 -> 8000
api:      8020 -> 8000
```

## Deploy Discipline

- Preserve server `.env.production`.
- For frontend-only changes, deploy only `frontend/index.html`, `frontend/styles.css`, and `frontend/app.js`.
- Rebuild/restart only `qubo-frontend` for frontend-only changes.
- Before overwriting server frontend files, create a timestamped backup under `~/zoho-dashboard/deploy_backups/`.
- Do not touch old folders such as `/home/ec2-user/zoho-dashboard_prev_*`.

## Local Validation

Run at minimum:

```powershell
node --check frontend\app.js
git diff --check
```

For backend changes, also run relevant Python compile/tests from `PROJECT_HANDOFF.md`.

## Recent Verified Deploy

On 2026-06-08, frontend tooltip changes were deployed and verified publicly:

```text
https://qubo-support.stage.platform.quboweb.com/
restyle32=True
function infoTip present in app.js
.info-tip present in styles.css
```
