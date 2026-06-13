#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.ai-transport.yml}"
PAUSE_SERVICE="${PAUSE_SERVICE:-sockudo-node-2}"
ALL_URLS="${SOCKUDO_NODE_URLS:-http://127.0.0.1:6001,http://127.0.0.1:6002,http://127.0.0.1:6003}"
HEALTHY_URLS_DURING_PAUSE="${SOCKUDO_PARTITION_HEALTHY_URLS:-http://127.0.0.1:6001,http://127.0.0.1:6003}"
STREAMS="${STREAMS:-10}"
APPENDS_PER_STREAM="${APPENDS_PER_STREAM:-200}"
MAX_APPEND_ADDED_P99_MS="${MAX_APPEND_ADDED_P99_MS:-${MAX_APPEND_P99_MS:-5}}"
PARTITION_MAX_APPEND_ADDED_P99_MS="${PARTITION_MAX_APPEND_ADDED_P99_MS:-50}"

bench() {
  local label="$1"
  local urls="$2"
  local budget_ms="$3"
  echo "== ${label} =="
  node scripts/ai-transport-3node-bench.mjs \
    --urls "${urls}" \
    --streams "${STREAMS}" \
    --appendsPerStream "${APPENDS_PER_STREAM}" \
    --maxAppendAddedP99Ms "${budget_ms}"
}

cleanup() {
  docker compose -f "${COMPOSE_FILE}" unpause "${PAUSE_SERVICE}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

bench "baseline all nodes" "${ALL_URLS}" "${MAX_APPEND_ADDED_P99_MS}"

echo "== pausing ${PAUSE_SERVICE} =="
docker compose -f "${COMPOSE_FILE}" pause "${PAUSE_SERVICE}"
bench "partitioned surviving nodes" "${HEALTHY_URLS_DURING_PAUSE}" "${PARTITION_MAX_APPEND_ADDED_P99_MS}"

echo "== unpausing ${PAUSE_SERVICE} =="
docker compose -f "${COMPOSE_FILE}" unpause "${PAUSE_SERVICE}"
sleep "${HEAL_WAIT_SECONDS:-5}"
bench "after heal all nodes" "${ALL_URLS}" "${PARTITION_MAX_APPEND_ADDED_P99_MS}"

echo "AI Transport jepsen-lite pause/unpause check completed"
