#!/usr/bin/env bash
# Run the Rust client's live suite against a Sockudo server built from this tree.
#
#   scripts/rust-client-live.sh [--compression] [path/to/sockudo]
#
# The server config is config/config.toml with node-local drivers (no MySQL or
# Redis needed). --compression enables WebSocket permessage-deflate on the
# server and makes the suite assert that the client negotiated it.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compression=0
server_bin=""
for arg in "$@"; do
  case "$arg" in
    --compression) compression=1 ;;
    *) server_bin="$arg" ;;
  esac
done
server_bin="${server_bin:-$repo_root/target/debug/sockudo}"
if [[ ! -x "$server_bin" ]]; then
  echo "server binary not found: $server_bin (build with: cargo build -p sockudo)" >&2
  exit 1
fi

port="${SOCKUDO_PORT:-6001}"
work_dir="$(mktemp -d)"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

config="$work_dir/config.toml"
python3 - "$repo_root/config/config.toml" "$config" "$compression" "$port" <<'PY'
import re
import sys

source, target, compression, port = sys.argv[1], sys.argv[2], sys.argv[3] == "1", sys.argv[4]
section = None
out = []
for line in open(source, encoding="utf-8").read().split("\n"):
    header = re.match(r"^\[([^\]]+)\]", line)
    if header:
        section = header.group(1)
    if section is None and re.match(r"^port\s*=", line):
        line = f"port = {port}"
    if section == "push" and re.match(r"^(storage_driver|queue_driver)\s*=", line):
        line = re.sub(r'"[^"]*"', '"memory"', line)
    if compression and section == "websocket" and re.match(r"^compression\s*=", line):
        line = 'compression = "shared"'
    out.append(line)
open(target, "w", encoding="utf-8").write("\n".join(out))
PY

"$server_bin" --config "$config" >"$work_dir/server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 120); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    echo "server exited during startup:" >&2
    cat "$work_dir/server.log" >&2
    exit 1
  fi
  sleep 0.25
done

cd "$repo_root/client-sdks/sockudo-rust"
status=0
SOCKUDO_LIVE_TESTS=1 \
  SOCKUDO_PORT="$port" \
  SOCKUDO_LIVE_EXPECT_DEFLATE="$compression" \
  cargo test --all-features --test live -- --test-threads=4 || status=$?
if [[ "$status" -ne 0 ]]; then
  echo "--- server log (tail) ---" >&2
  tail -n 80 "$work_dir/server.log" >&2
fi
exit "$status"
