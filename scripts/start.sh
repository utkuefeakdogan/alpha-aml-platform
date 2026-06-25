#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-core,app}"

echo "Starting Alpha AML Pipeline (profiles: $PROFILE)"
docker compose --profile core --profile app up -d --build

echo ""
echo "Services:"
echo "  Kafka:      localhost:9092"
echo "  PostgreSQL: localhost:5432"
echo "  Dashboard:  http://localhost:8501"
echo ""
echo "Optional ops profile: docker compose --profile ops up -d"
