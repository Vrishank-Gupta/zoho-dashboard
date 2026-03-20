#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/opt/qubo-support-dashboard}"

cd "${APP_DIR}"
git fetch --all
git checkout main
git pull --ff-only origin main
docker compose up -d --build
docker compose ps
