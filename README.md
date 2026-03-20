# Qubo CS Dashboard

Qubo CS Dashboard is split into two deployable parts:

- `qubo_dashboard/`: FastAPI backend + pipeline
- `frontend/`: static dashboard UI

Local development remains unchanged. Production deployment is intended as:

- backend on a VM or container host
- frontend on a static hosting service such as Netlify

## Repo layout

- [qubo_dashboard](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/qubo_dashboard)
- [frontend](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/frontend)
- [deploy](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy)
- [run_local.py](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/run_local.py)

## Local run

Use the local runner:

```powershell
python run_local.py
```

Manual local backend run:

```powershell
python -m uvicorn qubo_dashboard.main:app --host 127.0.0.1 --port 8010 --reload
```

Manual pipeline run:

```powershell
python -m qubo_dashboard.pipeline.run
```

## Production deployment model

- backend API domain, for example `https://qubo-support-api.companydomain.com`
- frontend domain, for example `https://qubo-support.companydomain.com`
- frontend points to backend through `frontend/config.js`
- backend CORS allows the frontend domain

Frontend config example:

```js
window.QUBO_APP_CONFIG = {
  apiBaseUrl: "https://qubo-support-api.companydomain.com",
};
```

Backend CORS example:

```env
QUBO_CORS_ALLOWED_ORIGINS=https://qubo-support.companydomain.com
```

## DevOps handoff files

Primary handoff docs:

- [DEVOPS_HANDOFF.md](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/DEVOPS_HANDOFF.md)
- [DEVOPS_DEPLOYMENT_GUIDE.md](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/DEVOPS_DEPLOYMENT_GUIDE.md)
- [DEVOPS_CHECKLIST.md](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/DEVOPS_CHECKLIST.md)
- [CICD_GUIDE.md](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/CICD_GUIDE.md)

Production config/templates:

- [deploy/.env.backend.production.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/.env.backend.production.example)
- [frontend.config.production.example.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/frontend.config.production.example.js)
- [docker-compose.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/docker-compose.yml)
- [Dockerfile](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/Dockerfile)
- [deploy/nginx/qubo-dashboard-api.conf.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/nginx/qubo-dashboard-api.conf.example)

SQL and verification:

- [bootstrap_analytics.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/sql/bootstrap_analytics.sql)
- [verification_queries.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/sql/verification_queries.sql)

Operational scripts:

- [bootstrap_analytics.sh](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/scripts/bootstrap_analytics.sh)
- [run_pipeline.sh](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/scripts/run_pipeline.sh)
- [deploy_backend.sh](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/scripts/deploy_backend.sh)
- [qubo-dashboard-pipeline.service](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/systemd/qubo-dashboard-pipeline.service)
- [qubo-dashboard-pipeline.timer](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/systemd/qubo-dashboard-pipeline.timer)

## Production backend quick start

1. Copy the repo to the VM.
2. Copy [deploy/.env.backend.production.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/.env.backend.production.example) to `.env` and fill in real values.
3. Bootstrap schema:

```bash
bash deploy/scripts/bootstrap_analytics.sh
```

4. Start API:

```bash
docker compose up -d --build
```

5. Health check:

```bash
curl http://127.0.0.1:8000/api/health
```

6. First pipeline run:

```bash
bash deploy/scripts/run_pipeline.sh
```

## Frontend deployment quick start

1. Set production API URL in deployed `frontend/config.js`
2. Publish the `frontend/` directory to Netlify, Vercel, S3, or Nginx static hosting
3. Ensure backend CORS allows the frontend hostname

## CI/CD

GitHub workflows:

- [ci.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/.github/workflows/ci.yml)
- [deploy-backend.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/.github/workflows/deploy-backend.yml)
- [deploy-frontend.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/.github/workflows/deploy-frontend.yml)

## Notes

- Production deployment files do not change local `run_local.py` behavior.
- `docker-compose.yml` now exposes `${QUBO_PUBLIC_PORT:-8000}` for VM flexibility.
- The pipeline now relies on `raw_ticket_cache` for incremental source fetches, so that table is part of the required production schema.
