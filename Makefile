.PHONY: help setup up down generate start-cdc start-load stop-load \
       bulk-migrate cdc-consume migrate monitor validate \
       failure-test logs clean reset

# Default target
help:
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║   PostgreSQL → MongoDB Migration Lab (Pure Docker)          ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║                                                            ║"
	@echo "║  Setup & Infrastructure                                    ║"
	@echo "║    make setup          Copy .env & build Docker containers ║"
	@echo "║    make up             Start infrastructure containers     ║"
	@echo "║    make down           Stop all services + remove volumes  ║"
	@echo "║                                                            ║"
	@echo "║  Data Generation                                           ║"
	@echo "║    make generate       Generate ~4M records in PostgreSQL  ║"
	@echo "║                                                            ║"
	@echo "║  Migration                                                 ║"
	@echo "║    make start-cdc      Register Debezium connector         ║"
	@echo "║    make start-load     Start continuous source traffic      ║"
	@echo "║    make stop-load      Stop load generator                 ║"
	@echo "║    make bulk-migrate   Run bulk migration                  ║"
	@echo "║    make cdc-consume    Start CDC consumer                  ║"
	@echo "║    make migrate        Full orchestrated migration         ║"
	@echo "║                                                            ║"
	@echo "║  Monitoring & Validation                                   ║"
	@echo "║    make monitor        Real-time migration dashboard       ║"
	@echo "║    make validate       Run 7-point validation              ║"
	@echo "║                                                            ║"
	@echo "║  Testing                                                   ║"
	@echo "║    make failure-test   Run failure recovery scenarios       ║"
	@echo "║                                                            ║"
	@echo "║  Utilities                                                 ║"
	@echo "║    make logs           Tail Docker logs                    ║"
	@echo "║    make clean          Remove checkpoints + reports         ║"
	@echo "║    make reset          Full reset (down + clean)           ║"
	@echo "║                                                            ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"

# ─── Setup ──────────────────────────────────────────────────────────────────
setup:
	@test -f .env || cp .env.example .env
	@mkdir -p checkpoints
	docker compose build
	@echo "✅ Setup complete! Docker containers built."

# ─── Infrastructure ────────────────────────────────────────────────────────
up:
	docker compose up -d postgres mongodb kafka schema-registry kafka-connect kafka-ui
	@echo ""
	@echo "⏳ Waiting for infrastructure services to become healthy..."
	@echo "   (Kafka Connect may take 1-2 minutes)"
	@echo ""
	@sleep 5
	@docker compose ps
	@echo ""
	@echo "🌐 Kafka UI Web Dashboard: http://localhost:8080"

down:
	docker compose down -v
	@echo "✅ All services stopped and volumes removed."

# ─── Data Generation ───────────────────────────────────────────────────────
generate:
	docker compose run --rm data-generator

# ─── CDC ────────────────────────────────────────────────────────────────────
start-cdc:
	docker compose run --rm runner bash scripts/start_cdc.sh

# ─── Load Generator ────────────────────────────────────────────────────────
start-load:
	docker compose up -d load-generator

stop-load:
	docker compose stop load-generator
	docker compose rm -f load-generator

# ─── Bulk Migration ────────────────────────────────────────────────────────
bulk-migrate:
	docker compose run --rm bulk-migrator

# ─── CDC Consumer ───────────────────────────────────────────────────────────
cdc-consume:
	docker compose run --rm cdc-consumer

# ─── Full Migration ────────────────────────────────────────────────────────
migrate:
	docker compose run --rm runner bash scripts/start_migration.sh

# ─── Monitoring ─────────────────────────────────────────────────────────────
monitor:
	docker compose run --rm -it monitoring

# ─── Validation ─────────────────────────────────────────────────────────────
validate:
	docker compose run --rm validator

# ─── Failure Testing ───────────────────────────────────────────────────────
failure-test:
	docker compose run --rm runner bash scripts/failure_test.sh

# ─── Utilities ──────────────────────────────────────────────────────────────
logs:
	docker compose logs -f

logs-kafka:
	docker compose logs -f kafka

logs-connect:
	docker compose logs -f kafka-connect

logs-debezium:
	docker compose logs -f kafka-connect 2>&1 | grep -i debezium

clean:
	rm -rf checkpoints/
	rm -f migration_report.json
	@echo "✅ Cleaned checkpoints and reports."

reset: down clean
	@echo "✅ Full reset complete."
