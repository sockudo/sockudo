#!/usr/bin/env bash
# Test Suite shards: a partition of the workspace members, balanced by test
# count. `test-shards.sh <name>` prints that shard's cargo package flags;
# `test-shards.sh --list` prints the shard names.
set -euo pipefail

case "${1:-}" in
  --list) echo transport core storage server ;;
  transport) echo -p sockudo-adapter ;;
  core) echo -p sockudo-core -p sockudo-protocol ;;
  storage)
    echo -p sockudo-push -p sockudo-ai-transport -p sockudo-ai-benches \
      -p sockudo-app -p sockudo-queue -p sockudo-cache -p sockudo-delta \
      -p sockudo-rate-limiter -p sockudo-metrics -p sockudo-filter \
      -p sockudo-webhook
    ;;
  server) echo -p sockudo -p sockudo-ably-compat -p sockudo-mcp -p sockudo-simulator ;;
  *)
    echo "unknown shard: ${1:-}" >&2
    exit 2
    ;;
esac
