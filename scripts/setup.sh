#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Setting up migration lab..."

# Copy .env.example to .env if not exists
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        echo "Creating .env from .env.example"
        cp .env.example .env
    fi
fi

mkdir -p checkpoints
chmod +x scripts/*.sh 2>/dev/null || true

# Wait for Docker services to be healthy
echo "Checking Docker services..."
echo "Waiting for postgres..."
until python3 -c "from src.common.config import get_pg_connection; conn=get_pg_connection(); conn.close()" > /dev/null 2>&1; do
    sleep 2
done
echo "Postgres is ready."

echo "Waiting for mongodb..."
until python3 -c "from src.common.config import get_mongo_client; get_mongo_client().admin.command('ping')" > /dev/null 2>&1; do
    sleep 2
done
echo "MongoDB is ready."

echo "Waiting for schema-registry..."
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}
until curl -sf "$SCHEMA_REGISTRY_URL/subjects" > /dev/null 2>&1; do
    sleep 2
done
echo "Schema Registry is ready."

echo "Waiting for kafka-connect..."
KAFKA_CONNECT_URL=${KAFKA_CONNECT_URL:-http://kafka-connect:8083}
until curl -sf "$KAFKA_CONNECT_URL/connectors" > /dev/null 2>&1; do
    sleep 2
done
echo "Kafka Connect is ready."

echo "Setup complete!"
