#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

KAFKA_CONNECT_URL=${KAFKA_CONNECT_URL:-http://kafka-connect:8083}

echo '═══ Starting CDC Pipeline ═══'

# 1. Wait for Kafka Connect to be ready
echo 'Waiting for Kafka Connect...'
until curl -sf "$KAFKA_CONNECT_URL/connectors" > /dev/null 2>&1; do
    sleep 2
    echo '  Still waiting...'
done
echo 'Kafka Connect is ready ✅'

# 2. Check if connector already exists
if curl -sf "$KAFKA_CONNECT_URL/connectors/postgres-connector" > /dev/null 2>&1; then
    echo 'Connector already exists. Checking status...'
    STATUS=$(curl -sf "$KAFKA_CONNECT_URL/connectors/postgres-connector/status" | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])")
    echo "Connector status: $STATUS"
    if [ "$STATUS" = "RUNNING" ]; then
        echo 'Connector is already running ✅'
        exit 0
    fi
fi

# 3. Register connector
echo 'Registering Debezium PostgreSQL connector...'
curl -sf -X POST "$KAFKA_CONNECT_URL/connectors" \
    -H 'Content-Type: application/json' \
    -d @infra/debezium/connector.json

echo ''
echo 'Connector registered ✅'

# 4. Wait for connector to start and verify
sleep 5
STATUS=$(curl -sf "$KAFKA_CONNECT_URL/connectors/postgres-connector/status" | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])")
echo "Connector status: $STATUS"

if [ "$STATUS" = "RUNNING" ]; then
    echo '═══ CDC Pipeline Active ═══'
else
    echo 'WARNING: Connector is not RUNNING. Check logs: docker compose logs kafka-connect'
    exit 1
fi
