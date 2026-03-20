# DevOps Checklist

## Access and infra

- VM available for backend
- Docker Engine installed
- Docker Compose plugin installed
- DNS for backend API hostname
- DNS for frontend hostname
- TLS configured for backend API hostname

## Databases

- Remote Zoho MySQL reachable from backend VM
- Analytics MySQL reachable from backend VM
- Analytics DB user has create/update/select permissions
- Source DB user has read access

## Required files

- [deploy/.env.backend.production.example](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/.env.backend.production.example)
- [bootstrap_analytics.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/sql/bootstrap_analytics.sql)
- [verification_queries.sql](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/sql/verification_queries.sql)
- [docker-compose.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/docker-compose.yml)
- [frontend.config.production.example.js](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20Bot%20Analytics/deploy/frontend.config.production.example.js)

## Backend deploy steps

- repo cloned to VM
- `.env` created from production template
- analytics schema bootstrapped
- `docker compose up -d --build` completed
- `curl http://127.0.0.1:8000/api/health` returns success
- first pipeline run completed

## Frontend deploy steps

- `frontend/` published to static host
- deployed `config.js` points to production backend API domain
- backend CORS allows the frontend domain

## Pipeline

- first run succeeded
- schedule configured by cron or systemd timer
- latest `pipeline_log` row shows success
- `raw_ticket_cache` is populating

## Verification

- dashboard loads
- issue drawer loads
- period breakdown drawer loads
- pipeline status endpoint works
- warehouse mode is visible in UI

## Monitoring

- backend logs captured
- `/api/health` monitored
- pipeline failures alerting enabled
