#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mode="${1:-local}"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

require_in_file() {
  local file="$1"
  local needle="$2"
  if ! grep -Fq "$needle" "$file"; then
    echo "missing required text in $file: $needle" >&2
    exit 1
  fi
}

case "$mode" in
  local)
    run "$0" perf-doc
    run cargo fmt --all -- --check
    run cargo test -p sockudo-push
    run cargo test -p sockudo-push --all-features
    run cargo test -p sockudo --features push push
    run cargo test -p sockudo --features full push
    run cargo build -p sockudo --no-default-features --features "v2"
    run cargo build -p sockudo --features "push,push-fcm,postgres,kafka"
    run cargo build -p sockudo --features "push,push-apns,scylladb,iggy"
    run cargo build -p sockudo --features "push,push-webpush,dynamodb,sqs"
    run cargo build -p sockudo --features "push,push-hms,mysql,rabbitmq"
    run cargo build -p sockudo --features "push,push-wns,postgres,redis"
    ;;
  perf-doc)
    results_doc="docs/research/ably-parity/sockudo-push-perf-results-4.5.md"
    test -f "$results_doc"
    for required in \
      "## Topology" \
      "## Exact Commands" \
      "## Feature Flags" \
      "## Workload Generator Config" \
      "## Gate Results" \
      "Acceptance latency" \
      "Single-recipient e2e" \
      "Mid-fanout" \
      "Mega-fanout" \
      "Sustained mixed throughput" \
      "Burst handling" \
      "Tenant fairness" \
      "Provider failure isolation" \
      "Token-store stress" \
      "Memory/task stability soak" \
      "p50" \
      "p99" \
      "p99.9" \
      "Throughput" \
      "Resource utilization" \
      "Headroom" \
      "Bottleneck" \
      "Result" \
      "Release progression is stopped"; do
      require_in_file "$results_doc" "$required"
    done
    ;;
  list)
    cat <<'MATRIX'
local:
  scripts/push-verification-matrix.sh perf-doc
  cargo fmt --all -- --check
  cargo test -p sockudo-push
  cargo test -p sockudo-push --all-features
  cargo test -p sockudo --features push push
  cargo test -p sockudo --features full push
  cargo build -p sockudo --no-default-features --features "v2"
  cargo build -p sockudo --features "push,push-fcm,postgres,kafka"
  cargo build -p sockudo --features "push,push-apns,scylladb,iggy"
  cargo build -p sockudo --features "push,push-webpush,dynamodb,sqs"
  cargo build -p sockudo --features "push,push-hms,mysql,rabbitmq"
  cargo build -p sockudo --features "push,push-wns,postgres,redis"

external:
  Backend live conformance: PostgreSQL, MySQL, DynamoDB, SurrealDB, ScyllaDB
  Queue live conformance: Redis, Redis Cluster, NATS, Pulsar, RabbitMQ,
    Google Pub/Sub, Kafka/Redpanda, Iggy, SQS, SNS-paired ingress
  Provider live/mock harness: FCM, APNs HTTP/2, Web Push, HMS, WNS
  Fanout/load/chaos/soak: requires provisioned release-candidate cluster
MATRIX
    ;;
  *)
    echo "usage: $0 [local|perf-doc|list]" >&2
    exit 64
    ;;
esac
