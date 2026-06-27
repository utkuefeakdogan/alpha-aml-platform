#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-core,app}"

echo "Starting Alpha AML Pipeline (profiles: $PROFILE)"
docker compose --profile core --profile app up -d --build

echo ""
echo "Services:"
echo "  Kafka:      internal only (kafka:29092 on the Docker network)"
echo "  PostgreSQL: internal only (postgres:5432; use 'docker exec ... psql')"
echo "  Dashboard:  http://localhost:8501"
echo ""
echo "Optional ops profile: docker compose --profile ops up -d"
