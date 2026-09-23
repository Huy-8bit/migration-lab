#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Source .env if exists
[ -f .env ] && set -a && source .env && set +a

echo ''
echo '╔══════════════════════════════════════════════════════════════╗'
echo '║   PostgreSQL → MongoDB Migration - Full Orchestration       ║'
echo '╚══════════════════════════════════════════════════════════════╝'
echo ''

# T0: Verify infrastructure
echo '─── T0: Verifying Infrastructure ─────────────────────────────'
# Check PostgreSQL via Python
python3 -c "from src.common.config import get_pg_connection; conn=get_pg_connection(); conn.close()" >/dev/null 2>&1 || { echo 'PostgreSQL is not ready!'; exit 1; }
echo '  PostgreSQL: ✅'

# Check MongoDB via Python
python3 -c "from src.common.config import get_mongo_client; get_mongo_client().admin.command('ping')" >/dev/null 2>&1 || { echo 'MongoDB is not ready!'; exit 1; }
echo '  MongoDB: ✅'

# Check Kafka Connect via curl
curl -sf ${KAFKA_CONNECT_URL:-http://kafka-connect:8083}/connectors > /dev/null 2>&1 || { echo 'Kafka Connect is not ready!'; exit 1; }
echo '  Kafka Connect: ✅'
echo ''

# T0: Start CDC
echo '─── T0: Starting CDC Capture ─────────────────────────────────'
bash scripts/start_cdc.sh
echo ''
sleep 3

# T1: Start load generator in background
echo '─── T1: Starting Load Generator ─────────────────────────────'
python3 -m src.load_generator.main &
LOAD_PID=$!
echo "  Load generator PID: $LOAD_PID"
echo ''
sleep 2

# T1: Start bulk migration
echo '─── T1: Starting Bulk Migration ──────────────────────────────'
BULK_START=$(date +%s)
python3 -m src.bulk_migrator.main
BULK_END=$(date +%s)
BULK_DURATION=$((BULK_END - BULK_START))
echo "  Bulk migration completed in ${BULK_DURATION}s"
echo ''

# T2: Start CDC consumer to catch up
echo '─── T2: Starting CDC Consumer (catch-up) ────────────────────'
echo '  Processing buffered Kafka events...'
echo '  Will run for 60 seconds or until lag reaches 0...'
timeout 120 python3 -m src.cdc_consumer.main --timeout 60 || true
echo '  CDC catch-up complete'
echo ''

# T3: Stop load generator
echo '─── T3: Stopping Load Generator ──────────────────────────────'
kill $LOAD_PID 2>/dev/null || true
wait $LOAD_PID 2>/dev/null || true
echo '  Load generator stopped'
echo ''

# T4: Final CDC catch-up (drain remaining events)
echo '─── T4: Final CDC Drain ──────────────────────────────────────'
timeout 30 python3 -m src.cdc_consumer.main --timeout 15 || true
echo '  Final drain complete'
echo ''

# T5: Validation
echo '─── T5: Running Validation ────────────────────────────────────'
python3 -m src.validator.main
echo ''

# T6: Done
echo '╔══════════════════════════════════════════════════════════════╗'
echo '║   Migration Complete!                                       ║'
echo '╚══════════════════════════════════════════════════════════════╝'
