# Qubo Dashboard Deployment Reference

This is the current local reference for deploying the Qubo dashboard frontend/backend VM.

## Production SSH

Use the analytics report key:

```powershell
ssh -i "$HOME\.ssh\analytics-report-key.pem" ec2-user@ec2-15-207-57-132.ap-south-1.compute.amazonaws.com
```

Equivalent key path on this machine:

```text
C:\Users\Vrishank Gupta\.ssh\analytics-report-key.pem
```

Do not use the older handoff host `43.204.252.106` for the current stage frontend. The current stage frontend resolves to the active AWS hosts behind:

```text
qubo-support.stage.platform.quboweb.com
```

## Active Server Paths

The active Docker Compose project is:

```text
/home/ec2-user/zoho-dashboard
```

`docker inspect qubo-frontend-final` shows:

```text
com.docker.compose.project.working_dir=/home/ec2-user/zoho-dashboard
com.docker.compose.project.config_files=/home/ec2-user/zoho-dashboard/docker-compose.production.yml
```

There may also be an `/opt/zoho-dashboard` folder, but the currently running containers are not using it.

## Running Containers

Expected production containers:

```text
qubo-frontend-final
qubo-dashboard-api
qubo-clickhouse-etl
qubo-clickhouse
```

Expected exposed ports:

```text
frontend: host 81 -> container 8000
api:      host 8020 -> container 8000
```

## Frontend-Only Deploy

Use this when changing only:

```text
frontend/index.html
frontend/styles.css
frontend/app.js
```

From the local repo root:

```powershell
tar -czf .deploy_frontend.tgz frontend/index.html frontend/styles.css frontend/app.js
scp -i "$HOME\.ssh\analytics-report-key.pem" .deploy_frontend.tgz ec2-user@ec2-15-207-57-132.ap-south-1.compute.amazonaws.com:~/frontend_deploy.tgz
ssh -i "$HOME\.ssh\analytics-report-key.pem" ec2-user@ec2-15-207-57-132.ap-south-1.compute.amazonaws.com
```

On the server:

```bash
cd ~/zoho-dashboard
stamp=$(date +%Y%m%d-%H%M%S)
mkdir -p "deploy_backups/frontend-$stamp"
cp frontend/index.html frontend/styles.css frontend/app.js "deploy_backups/frontend-$stamp/"
tar -xzf ~/frontend_deploy.tgz
rm -f ~/frontend_deploy.tgz
docker compose -f docker-compose.production.yml build --no-cache qubo-frontend
docker compose -f docker-compose.production.yml up -d --no-deps qubo-frontend
```

## Verify Frontend Deploy

From the server:

```bash
curl -sf http://localhost:81/ | grep -o 'restyle[0-9][0-9]' | head
curl -sf 'http://localhost:81/app.js?v=restyle32' | grep -c 'function infoTip'
```

From local PowerShell:

```powershell
$h = Invoke-WebRequest -Uri "https://qubo-support.stage.platform.quboweb.com/?_ts=$(Get-Date -Format yyyyMMddHHmmss)" -UseBasicParsing
$h.Content.Contains("restyle32")

$j = Invoke-WebRequest -Uri "https://qubo-support.stage.platform.quboweb.com/app.js?v=restyle32&_ts=$(Get-Date -Format yyyyMMddHHmmss)" -UseBasicParsing
$j.Content.Contains("function infoTip")
```

## Notes

- Preserve `.env.production` on the server.
- For frontend-only changes, do not rebuild API or ETL containers.
- Keep a timestamped backup of frontend files before extracting new assets.
- `frontend/config.js` currently chooses local API for localhost and same-origin API in production.
