# DevOps Deployment Guide

This guide is the full production deployment path for Qubo CS Dashboard.

## Production topology

- Backend API on VM/container host
- Frontend on static hosting
- Remote Zoho MySQL as source
- Analytics MySQL as target/cache/aggregate store

## Backend deployment

### 1. Prepare VM

```bash
sudo mkdir -p /opt/qubo-support-dashboard
cd /opt
git clone <repo-url> qubo-support-dashboard
cd qubo-support-dashboard
```

### 2. Create backend env

```bash
cp deploy/.env.backend.production.example .env
```

Fill in:

- `QUBO_ZOHO_*`
- `QUBO_AGG_*`
- `QUBO_CORS_ALLOWED_ORIGINS`

### 3. Bootstrap analytics schema

```bash
bash deploy/scripts/bootstrap_analytics.sh
```

### 4. Start backend

```bash
docker compose up -d --build
```

### 5. Verify API

```bash
curl http://127.0.0.1:8000/api/health
```

### 6. Run first pipeline

```bash
bash deploy/scripts/run_pipeline.sh
```

### 7. Verify data state

```bash
mysql -h <agg-host> -u <agg-user> -p <agg-db> < deploy/sql/verification_queries.sql
```

## Frontend deployment

Deploy the [frontend](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/frontend) directory to the static host.

Set production API config to:

```js
window.QUBO_APP_CONFIG = {
  apiBaseUrl: "https://qubo-support-api.companydomain.com",
};
```

Use template:

- [frontend.config.production.example.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/frontend.config.production.example.js)

## Reverse proxy

Use:

- [qubo-dashboard-api.conf.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/nginx/qubo-dashboard-api.conf.example)

Point public HTTPS traffic to:

- `http://127.0.0.1:8000`

## Pipeline scheduling

### Cron

```cron
*/30 * * * * cd /opt/qubo-support-dashboard && bash deploy/scripts/run_pipeline.sh >> /var/log/qubo-pipeline.log 2>&1
```

### systemd timer

```bash
sudo cp deploy/systemd/qubo-dashboard-pipeline.service /etc/systemd/system/
sudo cp deploy/systemd/qubo-dashboard-pipeline.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now qubo-dashboard-pipeline.timer
```

## CI/CD

- backend deploy workflow: [deploy-backend.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/.github/workflows/deploy-backend.yml)
- frontend deploy workflow: [deploy-frontend.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/.github/workflows/deploy-frontend.yml)
- CI validation: [ci.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/.github/workflows/ci.yml)

## Notes

- `raw_ticket_cache` must exist in analytics DB
- `docker-compose.yml` exposes `${QUBO_PUBLIC_PORT:-8000}` so DevOps can change public port without editing code
- local development still uses [run_local.py](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/run_local.py)
