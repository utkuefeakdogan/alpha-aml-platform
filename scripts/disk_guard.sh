#!/usr/bin/env bash
# Host-side disk guard — run via cron every 15 min, e.g.:
# */15 * * * * /home/ubuntu/data-project/scripts/disk_guard.sh >> /var/log/aml-disk-guard.log 2>&1
set -euo pipefail

WARN_PCT="${DISK_WARN_PCT:-80}"
CRITICAL_PCT="${DISK_CRITICAL_PCT:-90}"
COMPOSE_DIR="${COMPOSE_DIR:-$HOME/data-project}"

used_pct="$(df --output=pcent / | tail -1 | tr -dc '0-9')"
echo "$(date -Is) root_disk_used_pct=${used_pct}"

if (( used_pct >= CRITICAL_PCT )); then
  echo "$(date -Is) CRITICAL: disk at ${used_pct}% — truncating aml.account_window_metrics"
  docker compose -f "$COMPOSE_DIR/docker-compose.yml" exec -T postgres \
    psql -U "${POSTGRES_USER:-user}" -d "${POSTGRES_DB:-datadb}" \
    -c "TRUNCATE aml.account_window_metrics;"
  exit 2
fi

if (( used_pct >= WARN_PCT )); then
  echo "$(date -Is) WARN: disk at ${used_pct}% (threshold ${WARN_PCT}%)"
  exit 1
fi

echo "$(date -Is) OK: disk at ${used_pct}%"
exit 0
