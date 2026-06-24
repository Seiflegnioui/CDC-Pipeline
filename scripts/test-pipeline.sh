#!/usr/bin/env bash
set -euo pipefail

# ── CDC Pipeline End-to-End Test ────────────────────────────────────────
# Verifies INSERT → UPDATE → DELETE propagation from MongoDB to Elasticsearch.

MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
ES_URL="${ES_URL:-http://localhost:9200}"
WAIT_SECONDS=2
TIMEOUT=10
FAILURES=0

# ── Colors ──────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}✔ PASS${NC} $*"; }
fail() { echo -e "  ${RED}✘ FAIL${NC} $*"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "${CYAN}[TEST]${NC} $*"; }

# Generate a unique test ID
TEST_ID="test-$(date +%s)-$$"
TEST_EMAIL="testuser-${TEST_ID}@example.com"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo -e "${BOLD}  CDC Pipeline End-to-End Test${NC}"
echo "═══════════════════════════════════════════════════════════════"
echo -e "  Test ID:    ${TEST_ID}"
echo -e "  MongoDB:    ${MONGO_URI}"
echo -e "  ES:         ${ES_URL}"
echo ""

# ── Helper: Query ES for a document by ID ──────────────────────────────
query_es() {
  local doc_id="$1"
  local elapsed=0
  local result=""

  while [ $elapsed -lt $TIMEOUT ]; do
    # Refresh the index first to ensure near-real-time visibility
    curl -sf -X POST "${ES_URL}/users/_refresh" > /dev/null 2>&1 || true

    result=$(curl -sf "${ES_URL}/users/_search" \
      -H "Content-Type: application/json" \
      -d "{\"query\":{\"match\":{\"_id\":\"${doc_id}\"}}}" 2>&1) || true

    if [ -n "$result" ]; then
      local hits
      hits=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('hits',{}).get('total',{}).get('value',0))" 2>/dev/null) || hits="0"

      if [ "$hits" != "0" ]; then
        echo "$result"
        return 0
      fi
    fi

    sleep 1
    elapsed=$((elapsed + 1))
  done

  echo "$result"
  return 0
}

# ── TEST 1: INSERT ─────────────────────────────────────────────────────
info "TEST 1/3 — INSERT a new document into MongoDB"

INSERT_RESULT=$(mongosh --quiet "${MONGO_URI}/appdb" --eval "
  const result = db.users.insertOne({
    name: 'Test User ${TEST_ID}',
    email: '${TEST_EMAIL}',
    age: 25,
    role: 'tester',
    createdAt: new Date()
  });
  print(result.insertedId.toString());
")
DOC_ID=$(echo "$INSERT_RESULT" | tail -1 | tr -d "[:space:]" | sed "s/ObjectId('\(.*\)')/\1/" | sed "s/'//g")
echo -e "  Inserted doc _id: ${DOC_ID}"

info "Waiting ${WAIT_SECONDS}s for propagation..."
sleep $WAIT_SECONDS

ES_RESULT=$(query_es "$DOC_ID")
ES_HITS=$(echo "$ES_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('hits',{}).get('total',{}).get('value',0))" 2>/dev/null) || ES_HITS="0"

if [ "$ES_HITS" -gt "0" ] 2>/dev/null; then
  pass "INSERT propagated to Elasticsearch (hits: ${ES_HITS})"
  echo "$ES_RESULT" | python3 -m json.tool 2>/dev/null | head -20
else
  fail "INSERT not found in Elasticsearch"
  echo "  ES response: $ES_RESULT"
fi

echo ""

# ── TEST 2: UPDATE ─────────────────────────────────────────────────────
info "TEST 2/3 — UPDATE the document (change email)"

UPDATED_EMAIL="updated-${TEST_EMAIL}"
mongosh --quiet "${MONGO_URI}/appdb" --eval "
  db.users.updateOne(
    { _id: ObjectId('${DOC_ID}') },
    { \$set: { email: '${UPDATED_EMAIL}', age: 30 } }
  );
  print('Update applied');
"

info "Waiting ${WAIT_SECONDS}s for propagation..."
sleep $WAIT_SECONDS

ES_RESULT=$(query_es "$DOC_ID")
ES_EMAIL=$(echo "$ES_RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
hits = data.get('hits',{}).get('hits',[])
if hits:
    print(hits[0].get('_source',{}).get('email',''))
else:
    print('')
" 2>/dev/null) || ES_EMAIL=""

if [ "$ES_EMAIL" = "$UPDATED_EMAIL" ]; then
  pass "UPDATE propagated to Elasticsearch (email: ${ES_EMAIL})"
  echo "$ES_RESULT" | python3 -m json.tool 2>/dev/null | head -20
else
  fail "UPDATE not reflected in Elasticsearch (expected: ${UPDATED_EMAIL}, got: ${ES_EMAIL})"
  echo "  ES response: $ES_RESULT"
fi

echo ""

# ── TEST 3: DELETE ─────────────────────────────────────────────────────
info "TEST 3/3 — DELETE the document from MongoDB"

mongosh --quiet "${MONGO_URI}/appdb" --eval "
  db.users.deleteOne({ _id: ObjectId('${DOC_ID}') });
  print('Delete applied');
"

info "Waiting ${WAIT_SECONDS}s for propagation..."
sleep $WAIT_SECONDS

# For delete, we need to check the document is gone
curl -sf -X POST "${ES_URL}/users/_refresh" > /dev/null 2>&1 || true
sleep 1
ES_RESULT=$(curl -sf "${ES_URL}/users/_search" \
  -H "Content-Type: application/json" \
  -d "{\"query\":{\"match\":{\"_id\":\"${DOC_ID}\"}}}" 2>&1) || ES_RESULT="{}"

ES_HITS=$(echo "$ES_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('hits',{}).get('total',{}).get('value',0))" 2>/dev/null) || ES_HITS="1"

if [ "$ES_HITS" = "0" ]; then
  pass "DELETE propagated to Elasticsearch (document removed)"
else
  # Check if the document has __deleted field set to true (rewrite mode)
  DELETED_FLAG=$(echo "$ES_RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
hits = data.get('hits',{}).get('hits',[])
if hits:
    print(hits[0].get('_source',{}).get('__deleted',''))
else:
    print('')
" 2>/dev/null) || DELETED_FLAG=""

  if [ "$DELETED_FLAG" = "true" ] || [ "$DELETED_FLAG" = "True" ]; then
    pass "DELETE propagated (document marked as __deleted=true)"
  else
    fail "DELETE not reflected in Elasticsearch (hits: ${ES_HITS})"
    echo "  ES response: $ES_RESULT"
  fi
fi

echo ""

# ── Summary ────────────────────────────────────────────────────────────
echo "═══════════════════════════════════════════════════════════════"
if [ $FAILURES -eq 0 ]; then
  echo -e "${GREEN}${BOLD}  ALL TESTS PASSED ✔${NC}"
else
  echo -e "${RED}${BOLD}  ${FAILURES} TEST(S) FAILED ✘${NC}"
fi
echo "═══════════════════════════════════════════════════════════════"
echo ""

exit $FAILURES
