# DevOps Handoff

This is the shortest path for production deployment.

## Deployment shape

- Backend: VM or container host
- Frontend: static hosting service
- Databases:
  - remote Zoho MySQL source
  - analytics MySQL target

Recommended URLs:

- Frontend: `https://qubo-support.companydomain.com`
- Backend: `https://qubo-support-api.companydomain.com`

## Files DevOps should use

- Backend env template:
  - [deploy/.env.backend.production.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/.env.backend.production.example)
- Backend runtime:
  - [Dockerfile](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/Dockerfile)
  - [docker-compose.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/docker-compose.yml)
- SQL:
  - [bootstrap_analytics.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/sql/bootstrap_analytics.sql)
  - [verification_queries.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/sql/verification_queries.sql)
- Scripts:
  - [bootstrap_analytics.sh](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/scripts/bootstrap_analytics.sh)
  - [run_pipeline.sh](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/scripts/run_pipeline.sh)
  - [deploy_backend.sh](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/scripts/deploy_backend.sh)
- Frontend config:
  - [frontend.config.production.example.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/frontend.config.production.example.js)
- Reverse proxy example:
  - [qubo-dashboard-api.conf.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/nginx/qubo-dashboard-api.conf.example)

## VM steps

1. Clone repo to VM

```bash
git clone <repo-url> /opt/qubo-support-dashboard
cd /opt/qubo-support-dashboard
```

2. Create `.env`

```bash
cp deploy/.env.backend.production.example .env
```

3. Fill real secrets in `.env`

4. Bootstrap analytics DB

```bash
bash deploy/scripts/bootstrap_analytics.sh
```

5. Start backend

```bash
docker compose up -d --build
```

6. Verify health

```bash
curl http://127.0.0.1:8000/api/health
```

7. Run initial pipeline

```bash
bash deploy/scripts/run_pipeline.sh
```

8. Verify data

```bash
mysql -h <agg-host> -u <agg-user> -p <agg-db> < deploy/sql/verification_queries.sql
```

## Frontend hosting steps

1. Publish the `frontend/` directory
2. Replace `frontend/config.js` during deploy with the production backend API URL
3. Confirm backend CORS includes the frontend hostname

Example:

```js
window.QUBO_APP_CONFIG = {
  apiBaseUrl: "https://qubo-support-api.companydomain.com",
};
```

## Pipeline scheduling

Option 1: cron

```cron
*/30 * * * * cd /opt/qubo-support-dashboard && bash deploy/scripts/run_pipeline.sh >> /var/log/qubo-pipeline.log 2>&1
```

Option 2: systemd timer

```bash
sudo cp deploy/systemd/qubo-dashboard-pipeline.service /etc/systemd/system/
sudo cp deploy/systemd/qubo-dashboard-pipeline.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now qubo-dashboard-pipeline.timer
```

## Required secrets

- all `QUBO_ZOHO_*`
- all `QUBO_AGG_*`
- `QUBO_CORS_ALLOWED_ORIGINS`

## Important operational notes

- `raw_ticket_cache` is required in production; the pipeline uses it for incremental fetches
- local development still uses [run_local.py](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/run_local.py) and is not changed by these deployment files
- the backend should not serve the frontend in production unless intentionally configured that way
