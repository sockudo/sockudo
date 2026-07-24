#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.ai-transport.yml}"
SCALE_COMPOSE_FILE="${SCALE_COMPOSE_FILE:-tests/load/docker-compose.ai-transport-scale.yml}"
RESULT_DIR="${RESULT_DIR:-target/ai-chaos}"
PROFILE="${PROFILE:-tests/load/profiles/smoke.json}"
NODE_URLS="${SOCKUDO_NODE_URLS:-http://127.0.0.1:6001,http://127.0.0.1:6002,http://127.0.0.1:6003,http://127.0.0.1:6004,http://127.0.0.1:6005}"
METRICS_URLS="${SOCKUDO_METRICS_URLS:-http://127.0.0.1:9601/metrics,http://127.0.0.1:9602/metrics,http://127.0.0.1:9603/metrics,http://127.0.0.1:9604/metrics,http://127.0.0.1:9605/metrics}"
export AI_REDIS_HOST_PORT="${AI_REDIS_HOST_PORT:-26379}"
export AI_POSTGRES_HOST_PORT="${AI_POSTGRES_HOST_PORT:-25433}"

scenario="${1:-all}"
CHAOS_FAILED=0

cd "$ROOT_DIR"
mkdir -p "$RESULT_DIR"

compose() {
  docker compose -f "$COMPOSE_FILE" -f "$SCALE_COMPOSE_FILE" "$@"
}

wait_for_http() {
  local url="$1"
  local deadline=$((SECONDS + 90))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS > deadline )); then
      echo "timed out waiting for $url" >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_write_ready() {
  local name="$1"
  local deadline=$((SECONDS + 90))
  until node tests/load/ai-scale-runner.mjs \
    --profile "$PROFILE" \
    --urls "$NODE_URLS" \
    --metricsUrls "$METRICS_URLS" \
    --output "$RESULT_DIR/readiness-${name}.json" \
    --name "readiness-${name}" \
    --activeStreams 5 \
    --durationSeconds 1 \
    --meanResponseTokens 1 \
    --sampleTranscriptCount 5 \
    --requestTimeoutMs 5000 \
    >/dev/null 2>&1; do
    if (( SECONDS > deadline )); then
      echo "timed out waiting for write readiness; inspect $RESULT_DIR/readiness-${name}.json" >&2
      return 1
    fi
    sleep 2
  done
}

ensure_cluster() {
  compose up -d --build sockudo-node-1 sockudo-node-2 sockudo-node-3 sockudo-node-4 sockudo-node-5 redis postgres
  wait_for_http "http://127.0.0.1:6001/up/app-id"
  wait_for_http "http://127.0.0.1:6002/up/app-id"
  wait_for_http "http://127.0.0.1:6003/up/app-id"
  wait_for_http "http://127.0.0.1:6004/up/app-id"
  wait_for_http "http://127.0.0.1:6005/up/app-id"
  wait_for_write_ready "cluster"
}

run_load() {
  local name="$1"
  shift
  node tests/load/ai-scale-runner.mjs \
    --profile "$PROFILE" \
    --urls "$NODE_URLS" \
    --metricsUrls "$METRICS_URLS" \
    --output "$RESULT_DIR/${name}.json" \
    "$@"
}

run_load_background() {
  local name="$1"
  shift
  run_load "$name" "$@" &
  LOAD_PID=$!
}

wait_load() {
  local pid="$1"
  wait "$pid"
}

wait_load_recording_failure() {
  local pid="$1"
  if ! wait_load "$pid"; then
    CHAOS_FAILED=1
  fi
}

container_id() {
  compose ps -q "$1"
}

container_network() {
  docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$1"
}

scenario_node_kill() {
  echo "chaos: node kill mid-stream"
  ensure_cluster
  run_load_background "node-kill-during" --durationSeconds 30 --activeStreams 24 --sampleTranscriptCount 24
  sleep 5
  compose kill sockudo-node-2
  sleep 5
  compose up -d sockudo-node-2
  wait_for_http "http://127.0.0.1:6002/up/app-id"
  wait_load_recording_failure "$LOAD_PID"
  run_load "node-kill-after" --durationSeconds 5 --activeStreams 8 --sampleTranscriptCount 8
}

scenario_redis_failover() {
  echo "chaos: redis restart during streams"
  ensure_cluster
  run_load_background "redis-restart-during" --durationSeconds 30 --activeStreams 24 --sampleTranscriptCount 24
  sleep 5
  compose restart redis
  wait_load_recording_failure "$LOAD_PID"
  wait_for_http "http://127.0.0.1:6001/up/app-id"
  wait_for_write_ready "redis-restart"
  run_load "redis-restart-after" --durationSeconds 5 --activeStreams 8 --sampleTranscriptCount 8
}

scenario_partition() {
  echo "chaos: inter-node partition and heal"
  ensure_cluster
  local container
  local network
  container="$(container_id sockudo-node-3)"
  network="$(container_network "$container")"
  run_load_background "partition-during" --durationSeconds 30 --activeStreams 24 --sampleTranscriptCount 24
  sleep 5
  docker network disconnect "$network" "$container"
  sleep 10
  docker network connect "$network" "$container"
  wait_for_http "http://127.0.0.1:6003/up/app-id"
  wait_load_recording_failure "$LOAD_PID"
  run_load "partition-after" --durationSeconds 5 --activeStreams 8 --sampleTranscriptCount 8
}

scenario_slow_subscriber() {
  echo "chaos: slow-subscriber pressure proxy"
  ensure_cluster
  run_load "slow-subscriber-pressure" \
    --logicalConnections 5000 \
    --sessions 1000 \
    --activeStreams 40 \
    --durationSeconds 20 \
    --subscribersMin 5 \
    --subscribersMax 5 \
    --sampleTranscriptCount 40
}

scenario_clock_skew() {
  echo "chaos: client timestamp skew serial audit"
  ensure_cluster
  run_load "clock-skew-positive" --durationSeconds 10 --activeStreams 16 --sampleTranscriptCount 16 --clientClockSkewMs 15000
  run_load "clock-skew-negative" --durationSeconds 10 --activeStreams 16 --sampleTranscriptCount 16 --clientClockSkewMs -15000
}

case "$scenario" in
  all)
    scenario_node_kill
    scenario_redis_failover
    scenario_partition
    scenario_slow_subscriber
    scenario_clock_skew
    ;;
  node-kill)
    scenario_node_kill
    ;;
  redis-failover)
    scenario_redis_failover
    ;;
  partition)
    scenario_partition
    ;;
  slow-subscriber)
    scenario_slow_subscriber
    ;;
  clock-skew)
    scenario_clock_skew
    ;;
  *)
    echo "unknown scenario: $scenario" >&2
    echo "usage: $0 [all|node-kill|redis-failover|partition|slow-subscriber|clock-skew]" >&2
    exit 64
    ;;
esac

if (( CHAOS_FAILED != 0 )); then
  echo "one or more chaos workloads reported failures; inspect $RESULT_DIR" >&2
  exit 1
fi
