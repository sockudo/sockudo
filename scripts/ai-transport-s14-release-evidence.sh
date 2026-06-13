#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIRM="${AIT_RELEASE_EVIDENCE_CONFIRM:-}"
OUTPUT_DIR="${OUTPUT_DIR:-docs/specs/ai-transport-results}"

if [[ "$CONFIRM" != "external-rc" ]]; then
  cat >&2 <<'EOF'
Refusing to run S14 release profiles without explicit RC confirmation.

Set:
  AIT_RELEASE_EVIDENCE_CONFIRM=external-rc
  SOCKUDO_NODE_URLS=http://node-1:6001,...
  SOCKUDO_METRICS_URLS=http://node-1:9601/metrics,...

These profiles are intended for production-like release-candidate fleets. Do not run or record
developer-scale substitutes as GA evidence.
EOF
  exit 64
fi

if [[ -z "${SOCKUDO_NODE_URLS:-}" || -z "${SOCKUDO_METRICS_URLS:-}" ]]; then
  echo "SOCKUDO_NODE_URLS and SOCKUDO_METRICS_URLS are required for release evidence" >&2
  exit 64
fi

mkdir -p "$OUTPUT_DIR"

run_profile() {
  local profile="$1"
  local output="$2"
  node test/load/ai-scale-runner.mjs \
    --profile "test/load/profiles/${profile}.json" \
    --execute \
    --output "$OUTPUT_DIR/${output}.json"
}

run_profile "headline-1m" "headline-1m"
run_profile "reconnect-storm" "reconnect-storm"
run_profile "cancel-storm" "cancel-storm"
run_profile "soak-20pct" "soak-20pct"
