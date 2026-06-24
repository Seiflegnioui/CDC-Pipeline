#!/usr/bin/env bash
set -euo pipefail

# ── CDC Connector Registration Script ───────────────────────────────────
# Registers the Debezium MongoDB Source and Elasticsearch Sink connectors
# with Kafka Connect. Waits for Connect to be healthy before proceeding.

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTORS_DIR="${SCRIPT_DIR}/../connectors"
TIMEOUT=60

# ── Colors for output ──────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── Step 1: Wait for Kafka Connect to be healthy ───────────────────────
log_info "Waiting for Kafka Connect at ${CONNECT_URL} ..."
elapsed=0
while [ $elapsed -lt $TIMEOUT ]; do
  if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
    log_ok "Kafka Connect is healthy (took ${elapsed}s)"
    break
  fi
  sleep 2
  elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $TIMEOUT ]; then
  log_error "Kafka Connect not ready after ${TIMEOUT}s — aborting"
  exit 1
fi

# ── Step 2: Register Debezium MongoDB Source Connector ─────────────────
log_info "Registering Debezium MongoDB Source Connector..."

SOURCE_RESPONSE=$(curl -sf -w "\n%{http_code}" -X POST \
  "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${CONNECTORS_DIR}/mongodb-source.json" 2>&1) || true

SOURCE_HTTP_CODE=$(echo "$SOURCE_RESPONSE" | tail -1)
SOURCE_BODY=$(echo "$SOURCE_RESPONSE" | sed '$d')

if [ "$SOURCE_HTTP_CODE" = "201" ] || [ "$SOURCE_HTTP_CODE" = "200" ]; then
  log_ok "MongoDB Source Connector registered successfully (HTTP ${SOURCE_HTTP_CODE})"
elif [ "$SOURCE_HTTP_CODE" = "409" ]; then
  log_warn "MongoDB Source Connector already exists (HTTP 409) — skipping"
else
  log_error "Failed to register MongoDB Source Connector (HTTP ${SOURCE_HTTP_CODE})"
  echo "$SOURCE_BODY"
  exit 1
fi

# ── Step 3: Register Elasticsearch Sink Connector ──────────────────────
log_info "Registering Elasticsearch Sink Connector..."

SINK_RESPONSE=$(curl -sf -w "\n%{http_code}" -X POST \
  "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${CONNECTORS_DIR}/elasticsearch-sink.json" 2>&1) || true

SINK_HTTP_CODE=$(echo "$SINK_RESPONSE" | tail -1)
SINK_BODY=$(echo "$SINK_RESPONSE" | sed '$d')

if [ "$SINK_HTTP_CODE" = "201" ] || [ "$SINK_HTTP_CODE" = "200" ]; then
  log_ok "Elasticsearch Sink Connector registered successfully (HTTP ${SINK_HTTP_CODE})"
elif [ "$SINK_HTTP_CODE" = "409" ]; then
  log_warn "Elasticsearch Sink Connector already exists (HTTP 409) — skipping"
else
  log_error "Failed to register Elasticsearch Sink Connector (HTTP ${SINK_HTTP_CODE})"
  echo "$SINK_BODY"
  exit 1
fi

# ── Step 4: Print connector status ─────────────────────────────────────
log_info "Waiting 5 seconds for connectors to initialize..."
sleep 5

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  CONNECTOR STATUS"
echo "═══════════════════════════════════════════════════════════════"

for connector in mongodb-source elasticsearch-sink; do
  echo ""
  log_info "Connector: ${connector}"
  STATUS=$(curl -sf "${CONNECT_URL}/connectors/${connector}/status" 2>&1) || true
  if [ -n "$STATUS" ]; then
    echo "$STATUS" | python3 -m json.tool 2>/dev/null || echo "$STATUS"
  else
    log_warn "Could not retrieve status for ${connector}"
  fi
done

echo ""
echo "═══════════════════════════════════════════════════════════════"
log_ok "Connector registration complete"
echo "═══════════════════════════════════════════════════════════════"
