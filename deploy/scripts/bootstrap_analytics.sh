#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  echo "Missing ${ROOT_DIR}/.env"
  exit 1
fi

set -a
source "${ROOT_DIR}/.env"
set +a

mysql \
  -h "${QUBO_AGG_DB_HOST}" \
  -P "${QUBO_AGG_DB_PORT:-3306}" \
  -u "${QUBO_AGG_DB_USER}" \
  -p"${QUBO_AGG_DB_PASSWORD}" \
  "${QUBO_AGG_DB_NAME}" \
  < "${ROOT_DIR}/deploy/sql/bootstrap_analytics.sql"

echo "Analytics schema bootstrapped."
