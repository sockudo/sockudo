#!/usr/bin/env bash
# One-shot end-to-end run: mock APNs in Docker, Sockudo from this checkout, the ride backend,
# and the scenario runner. Leaves nothing running unless KEEP_RUNNING=1.
set -euo pipefail
cd "$(dirname "$0")"
EXAMPLE_DIR=$(pwd)
REPO_ROOT=$(cd ../.. && pwd)
mkdir -p .run

scripts/gen-certs.sh
docker compose up -d --wait

echo "building sockudo (push-apns,monolith)"
(cd "$REPO_ROOT" && cargo build -p sockudo --features "push-apns,monolith")

cleanup() {
  [[ -f .run/backend.pid ]] && kill "$(cat .run/backend.pid)" 2>/dev/null || true
  [[ -f .run/sockudo.pid ]] && kill "$(cat .run/sockudo.pid)" 2>/dev/null || true
  if [[ "${KEEP_RUNNING:-0}" != "1" ]]; then docker compose down >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

PUSH_APNS_TEAM_ID=MOCKTEAM01 \
PUSH_APNS_KEY_ID=MOCKKEY001 \
PUSH_APNS_PRIVATE_KEY_PATH="$EXAMPLE_DIR/certs/AuthKey_MOCKKEY001.p8" \
PUSH_APNS_CA_CERTIFICATE_PATH="$EXAMPLE_DIR/certs/ca.pem" \
RUST_LOG=${RUST_LOG:-info} \
  "$REPO_ROOT/target/debug/sockudo" --config "$EXAMPLE_DIR/sockudo.toml" > .run/sockudo.log 2>&1 &
echo $! > .run/sockudo.pid

for _ in $(seq 1 50); do
  curl -sf -o /dev/null http://127.0.0.1:6001/up/rides-app && break
  sleep 0.2
done

(cd backend && npm install --no-audit --no-fund >/dev/null)
(cd backend && exec node server.mjs) > .run/backend.log 2>&1 &
echo $! > .run/backend.pid
sleep 1
(cd backend && node scenario.mjs)
