#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/tests/ai-conformance"

if [[ "${AIT_CONFORMANCE_OFFLINE:-0}" == "1" ]]; then
  node src/run.mjs
  exit 0
fi

: "${SOCKUDO_BASE_URL:=http://127.0.0.1:6001}"
: "${SOCKUDO_WS_URL:=ws://127.0.0.1:6001/app/app-key?protocol=2&client=ait-conformance&version=0}"
: "${SOCKUDO_APP_ID:=app-id}"
: "${SOCKUDO_APP_KEY:=app-key}"
: "${SOCKUDO_APP_SECRET:=app-secret}"

export SOCKUDO_BASE_URL SOCKUDO_WS_URL SOCKUDO_APP_ID SOCKUDO_APP_KEY SOCKUDO_APP_SECRET
node src/run.mjs
