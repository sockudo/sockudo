#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

BUNDLE_ID="${BUNDLE_ID:-com.sockudo.apnsprobe}"

export PUSH_APNS_ENABLED="${PUSH_APNS_ENABLED:-true}"
export PUSH_APNS_ENDPOINT="${PUSH_APNS_ENDPOINT:-https://api.sandbox.push.apple.com}"
export PUSH_APNS_TEAM_ID="${PUSH_APNS_TEAM_ID:-YOUR_TEAM_ID}"
export PUSH_APNS_KEY_ID="${PUSH_APNS_KEY_ID:-YOUR_KEY_ID}"
export PUSH_APNS_PRIVATE_KEY_PATH="${PUSH_APNS_PRIVATE_KEY_PATH:-/path/to/AuthKey_YOUR_KEY_ID.p8}"
export PUSH_APNS_TOPIC="${PUSH_APNS_TOPIC:-$BUNDLE_ID}"
export PUSH_STORAGE_DRIVER="${PUSH_STORAGE_DRIVER:-memory}"
export PUSH_QUEUE_DRIVER="${PUSH_QUEUE_DRIVER:-memory}"
export RUST_LOG="${RUST_LOG:-sockudo=info,sockudo_push=info,warn}"

echo "Starting Sockudo APNs monolith with topic: $PUSH_APNS_TOPIC"
cargo run -p sockudo --features push-apns,monolith --bin sockudo -- --config config/config.toml
