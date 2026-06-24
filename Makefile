# ── CDC Pipeline Makefile ────────────────────────────────────────────────
# Usage: make <target>
#
# Targets:
#   up           Start all Docker services
#   init         Initialize MongoDB replica set + register connectors
#   test         Run end-to-end pipeline test
#   demo         Run the Python demo application
#   logs-connect Tail Kafka Connect logs
#   status       Show connector status from Kafka Connect REST API
#   down         Stop and remove all containers and volumes
#   reset        Full teardown and restart from scratch

.PHONY: up init init-mongo init-connectors test demo logs-connect status down reset help

COMPOSE := docker compose
CONNECT_URL := http://localhost:8083

# ── Default target ─────────────────────────────────────────────────────
help: ## Show this help
	@echo ""
	@echo "CDC Pipeline — Available targets:"
	@echo "──────────────────────────────────────────────────"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ── Infrastructure ─────────────────────────────────────────────────────
up: ## Start all Docker services
	@echo "Starting CDC pipeline services..."
	@cp -n .env.example .env 2>/dev/null || true
	$(COMPOSE) up -d
	@echo ""
	@echo "Services starting. Use 'make status' to check connector health."
	@echo "  Kafka UI:    http://localhost:8080"
	@echo "  Kibana:      http://localhost:5601"
	@echo "  ES:          http://localhost:9200"
	@echo "  Connect:     http://localhost:8083"

# ── Initialization ─────────────────────────────────────────────────────
init-mongo: ## Initialize MongoDB replica set and seed data
	@echo "Initializing MongoDB replica set..."
	docker exec mongodb mongosh --quiet /docker-entrypoint-initdb.d/init-mongo.js

init-connectors: ## Register Kafka Connect connectors
	@echo "Registering connectors..."
	bash scripts/init-connectors.sh

init: init-mongo init-connectors ## Initialize MongoDB RS + register connectors

# ── Testing ────────────────────────────────────────────────────────────
test: ## Run end-to-end pipeline test (insert → update → delete)
	@echo "Running pipeline tests..."
	bash scripts/test-pipeline.sh

# ── Demo ───────────────────────────────────────────────────────────────
demo: ## Run the Python demo application
	@echo "Starting CDC demo application..."
	cd app && pip install -q -r requirements.txt && python demo.py

# ── Observability ──────────────────────────────────────────────────────
logs-connect: ## Tail Kafka Connect logs
	$(COMPOSE) logs -f kafka-connect

status: ## Show connector status from Kafka Connect REST API
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  Kafka Connect Connectors"
	@echo "═══════════════════════════════════════════════════════════"
	@echo ""
	@echo "Registered connectors:"
	@curl -sf $(CONNECT_URL)/connectors | python3 -m json.tool 2>/dev/null || echo "  (Kafka Connect not reachable)"
	@echo ""
	@echo "── mongodb-source ──────────────────────────────────────────"
	@curl -sf $(CONNECT_URL)/connectors/mongodb-source/status | python3 -m json.tool 2>/dev/null || echo "  (not registered)"
	@echo ""
	@echo "── elasticsearch-sink ──────────────────────────────────────"
	@curl -sf $(CONNECT_URL)/connectors/elasticsearch-sink/status | python3 -m json.tool 2>/dev/null || echo "  (not registered)"
	@echo ""

# ── Teardown ───────────────────────────────────────────────────────────
down: ## Stop and remove all containers and volumes
	@echo "Tearing down CDC pipeline..."
	$(COMPOSE) down -v --remove-orphans
	@echo "All containers and volumes removed."

reset: ## Full teardown and restart from scratch
	@echo "Full reset — removing everything..."
	$(COMPOSE) down -v --remove-orphans
	@echo "Removing Kafka data volume to prevent stale CLUSTER_ID..."
	docker volume rm -f cdc_kafka_data cdc_mongodb_data cdc_es_data cdc_connect_plugins 2>/dev/null || true
	@echo "Restarting..."
	$(MAKE) up
	@echo ""
	@echo "Services restarting. Wait ~60s for Kafka Connect, then run:"
	@echo "  make init"
