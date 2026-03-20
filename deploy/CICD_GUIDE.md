# Qubo Support Health Command Center
# CI/CD Guide

This guide explains how to set up and use CI/CD for this project.

## 1. What the pipeline does

There are 3 GitHub Actions workflows:

- `CI`
  - runs on pushes and pull requests
  - compiles Python
  - checks frontend JavaScript syntax

- `Deploy Frontend`
  - runs on pushes to `main` when frontend files change
  - deploys the static `frontend/` directory to Netlify

- `Deploy Backend`
  - runs on pushes to `main` when backend or deployment files change
  - SSHes into the production VM
  - pulls latest code
  - rebuilds and restarts Docker

## 2. Files added

- [ci.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20/Bot%20Analytics/.github/workflows/ci.yml)
- [deploy-frontend.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20/Bot%20Analytics/.github/workflows/deploy-frontend.yml)
- [deploy-backend.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20/Bot%20Analytics/.github/workflows/deploy-backend.yml)

## 3. Prerequisites

Before CI/CD can work, the following must already exist:

### GitHub

- code pushed to a GitHub repository
- default production branch should be `main`

### Backend VM

- Linux VM available
- Docker and Docker Compose installed
- repo cloned on the VM
- production `.env` already placed on the VM
- backend working manually with:

```bash
docker compose up -d --build
```

### Frontend hosting

- Netlify site created
- frontend domain decided
- backend API domain decided

## 4. GitHub secrets required

Add these repository secrets in GitHub:

### Backend deploy secrets

- `QUBO_VM_HOST`
  - production VM hostname or IP
- `QUBO_VM_USER`
  - SSH username
- `QUBO_VM_SSH_KEY`
  - private SSH key used by GitHub Actions
- `QUBO_VM_PORT`
  - SSH port, usually `22`
- `QUBO_VM_APP_DIR`
  - absolute path to the repo on the VM
  - example: `/opt/qubo-support-dashboard`

### Frontend deploy secrets

- `NETLIFY_AUTH_TOKEN`
  - Netlify personal or team token
- `NETLIFY_SITE_ID`
  - Netlify site ID
- `QUBO_FRONTEND_API_BASE_URL`
  - backend production API URL
  - example: `https://qubo-support-api.companydomain.com`

## 5. One-time setup on the backend VM

On the VM:

1. Clone the repo into the final path
2. Create the production `.env`
3. Run the SQL bootstrap on the analytics DB
4. Start the backend once manually

Example:

```bash
cd /opt
git clone <repo-url> qubo-support-dashboard
cd qubo-support-dashboard
cp deploy/.env.backend.production.example .env
# fill in real values
docker compose up -d --build
```

## 6. How deployment works after setup

### Frontend

If you change only frontend files:

- `frontend/`
- `netlify.toml`

and push to `main`, GitHub Actions will deploy the frontend to Netlify automatically.

### Backend

If you change backend files:

- `qubo_dashboard/`
- `Dockerfile`
- `docker-compose.yml`
- `requirements.txt`

and push to `main`, GitHub Actions will SSH into the VM and run:

```bash
git fetch --all
git checkout main
git pull origin main
docker compose up -d --build
```

## 7. Your daily workflow

Typical workflow from your terminal:

```bash
git add .
git commit -m "Describe the change"
git push origin main
```

After that:

- CI runs automatically
- frontend deploy runs if frontend files changed
- backend deploy runs if backend files changed

## 8. Recommended safer workflow

If you want a safer production process:

1. work on a feature branch
2. open a pull request
3. let `CI` validate the code
4. merge into `main`
5. deployment happens automatically

This is better than pushing directly to `main` once the project becomes more active.

## 9. How to maintain the workflows

### To change frontend deployment behavior

Edit:

- [deploy-frontend.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20/Bot%20Analytics/.github/workflows/deploy-frontend.yml)

Use this if:

- frontend host changes
- Netlify site changes
- you want staging/prod split

### To change backend deployment behavior

Edit:

- [deploy-backend.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20/Bot%20Analytics/.github/workflows/deploy-backend.yml)

Use this if:

- VM host changes
- deployment path changes
- backend restart command changes

### To change validation

Edit:

- [ci.yml](C:/Users/User/OneDrive%20-%20Hero%20Electronix%20Pvt.%20Ltd/Desktop%20/Bot%20Analytics/.github/workflows/ci.yml)

Use this if:

- you add tests
- you add linting
- you add build checks

## 10. How to trigger manually from GitHub

Each deploy workflow includes `workflow_dispatch`.

That means you can manually trigger it in GitHub Actions even without a new push.

Useful for:

- redeploying after infra changes
- redeploying after fixing secrets
- rerunning frontend deploy

## 11. Common issues

### Backend deploy fails

Check:

- VM is reachable over SSH
- SSH key in GitHub secrets is correct
- repo path in `QUBO_VM_APP_DIR` is correct
- `.env` exists on the VM
- Docker works on the VM

### Frontend deploy fails

Check:

- Netlify token is valid
- site ID is correct
- API base URL secret is set

### App deploys but frontend cannot load data

Check:

- frontend config is pointing to the correct backend API domain
- backend CORS allows the frontend domain
- backend `/api/health` works publicly

## 12. Recommended next improvement

Once this baseline is stable, the next upgrades should be:

- staging environment
- approval gate before production backend deploy
- automated tests beyond compile checks
