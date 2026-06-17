#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/ai-transport-ga-gate.sh <command>

Commands:
  ci-evidence       Check deterministic GA evidence that can be validated in CI.
  release-evidence  Require all GA evidence, including external RC result manifests.
  matrix            Run the local server feature matrix check/clippy/test build.

Environment for matrix:
  MATRIX_TEST_MODE=no-run|run  Defaults to no-run.
EOF
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "missing required file: $path" >&2
    exit 1
  fi
}

require_grep() {
  local pattern="$1"
  local path="$2"
  if ! grep -Eq "$pattern" "$path"; then
    echo "missing required pattern '$pattern' in $path" >&2
    exit 1
  fi
}

check_ci_evidence() {
  require_file "docs/specs/ai-transport-wire-protocol.md"
  require_file "docs/specs/ai-transport-ga-readiness.md"
  require_file "docs/specs/ai-transport-security-review.md"
  require_file "docs/specs/ai-transport-capacity.md"
  require_file "docs/content/docs/server/ai-transport-production-checklist.mdx"
  require_file "docs/content/docs/reference/compatibility.mdx"
  require_file "config/ai-transport.example.toml"
  require_file "deny.toml"
  require_file "tests/ai-conformance/package.json"
  require_file "benches/ai/baselines/ai_hot_paths.budgets.json"
  require_file "scripts/ai-transport-s14-release-evidence.sh"
  require_file "scripts/ai-transport-rolling-upgrade-redis.mjs"
  require_file "scripts/sdk-compat-full-matrix.mjs"

  require_grep "Wire protocol version: 1" "docs/specs/ai-transport-wire-protocol.md"
  require_grep "Compatibility Promise" "docs/specs/ai-transport-wire-protocol.md"
  require_grep "AI Transport GA Readiness" "docs/specs/ai-transport-ga-readiness.md"
  require_grep "SDK plans status" "docs/specs/ai-transport-ga-readiness.md"
  require_grep "ai-transport-ga-gate.sh release-evidence" "docs/content/docs/server/ai-transport-production-checklist.mdx"
  require_grep "config/ai-transport.example.toml" "CHANGELOG.md"
}

check_release_evidence() {
  check_ci_evidence

  local missing=0
  if find docs/specs/ai-transport-results -name '*.not-run.json' -print -quit | grep -q .; then
    echo "external RC scale/chaos manifests are still marked not-run:" >&2
    find docs/specs/ai-transport-results -name '*.not-run.json' -print >&2
    missing=1
  fi

  for path in \
    docs/specs/ai-transport-results/headline-1m.json \
    docs/specs/ai-transport-results/reconnect-storm.json \
    docs/specs/ai-transport-results/cancel-storm.json \
    docs/specs/ai-transport-results/soak-20pct.json \
    docs/specs/ai-transport-results/rolling-upgrade-redis.json \
    docs/specs/ai-transport-results/sdk-compat-full-matrix.json
  do
    if [[ ! -f "$path" ]]; then
      echo "missing release evidence: $path" >&2
      missing=1
    fi
  done

  if [[ "$missing" -ne 0 ]]; then
    exit 1
  fi

  if ! node <<'NODE'
const fs = require("node:fs");

function read(path) {
  return JSON.parse(fs.readFileSync(path, "utf8"));
}

function fail(message) {
  console.error(message);
  process.exitCode = 1;
}

const scale = [
  ["docs/specs/ai-transport-results/headline-1m.json", "ai-headline-1m"],
  ["docs/specs/ai-transport-results/reconnect-storm.json", "ai-reconnect-storm"],
  ["docs/specs/ai-transport-results/cancel-storm.json", "ai-cancel-storm"],
  ["docs/specs/ai-transport-results/soak-20pct.json", "ai-soak-20pct"],
];

for (const [path, profile] of scale) {
  const manifest = read(path);
  if (manifest.schema !== "sockudo.ai-transport.scale-result.v1") {
    fail(`${path}: invalid schema ${manifest.schema}`);
  }
  if (manifest.status !== "passed") {
    fail(`${path}: status must be passed, got ${manifest.status}`);
  }
  if (manifest.profile !== profile) {
    fail(`${path}: profile must be ${profile}, got ${manifest.profile}`);
  }
  if (manifest.workload?.skipped) {
    fail(`${path}: workload was skipped`);
  }
  if (manifest.workload?.failures !== 0) {
    fail(`${path}: workload failures must be 0`);
  }
  if (manifest.workload?.transcriptAudit?.mismatches !== 0) {
    fail(`${path}: transcript mismatches must be 0`);
  }
}

const rolling = read("docs/specs/ai-transport-results/rolling-upgrade-redis.json");
if (rolling.schema !== "sockudo.ai-transport.rolling-upgrade-result.v1") {
  fail("rolling-upgrade-redis.json: invalid schema");
}
if (rolling.status !== "passed") {
  fail(`rolling-upgrade-redis.json: status must be passed, got ${rolling.status}`);
}
if (!Array.isArray(rolling.phases) || rolling.phases.length !== 3) {
  fail("rolling-upgrade-redis.json: expected three old/mixed/new phases");
}
for (const phase of rolling.phases ?? []) {
  if (phase.status !== "passed") {
    fail(`rolling-upgrade-redis.json: phase ${phase.name} is ${phase.status}`);
  }
}

const sdk = read("docs/specs/ai-transport-results/sdk-compat-full-matrix.json");
if (sdk.schema !== "sockudo.ai-transport.sdk-compat-matrix.v1") {
  fail("sdk-compat-full-matrix.json: invalid schema");
}
if (sdk.status !== "passed") {
  fail(`sdk-compat-full-matrix.json: status must be passed, got ${sdk.status}`);
}
if (sdk.counts?.clientSdks !== 5 || sdk.counts?.serverHttpSdks !== 9 || sdk.counts?.canaries !== 1) {
  fail("sdk-compat-full-matrix.json: expected 5 client SDKs, 9 server HTTP SDKs, and 1 canary");
}
if ((sdk.counts?.failed ?? 1) !== 0 || (sdk.counts?.planned ?? 1) !== 0) {
  fail("sdk-compat-full-matrix.json: all lanes must be executed and passed");
}
for (const lane of sdk.lanes ?? []) {
  const profileNames = new Set((lane.profileResults ?? []).map((profile) => profile.profile));
  if (!profileNames.has("default") || !profileNames.has("ai-enabled")) {
    fail(`sdk-compat-full-matrix.json: lane ${lane.name} must record both config profiles`);
  }
  for (const profile of lane.profileResults ?? []) {
    if (profile.status !== "passed") {
      fail(`sdk-compat-full-matrix.json: lane ${lane.name}/${profile.profile} is ${profile.status}`);
    }
  }
}
NODE
  then
    exit 1
  fi
}

run_cargo_for_combo() {
  local name="$1"
  shift
  local -a args=("$@")
  local test_mode="${MATRIX_TEST_MODE:-no-run}"

  echo "==> $name: cargo check"
  if ((${#args[@]} == 0)); then
    cargo check -p sockudo
  else
    cargo check -p sockudo "${args[@]}"
  fi

  echo "==> $name: cargo clippy"
  if ((${#args[@]} == 0)); then
    cargo clippy -p sockudo --all-targets -- -D warnings
  else
    cargo clippy -p sockudo --all-targets "${args[@]}" -- -D warnings
  fi

  echo "==> $name: cargo test"
  if [[ "$test_mode" == "run" ]]; then
    if ((${#args[@]} == 0)); then
      cargo test -p sockudo --all-targets
    else
      cargo test -p sockudo --all-targets "${args[@]}"
    fi
  elif [[ "$test_mode" == "no-run" ]]; then
    if ((${#args[@]} == 0)); then
      cargo test -p sockudo --all-targets --no-run
    else
      cargo test -p sockudo --all-targets --no-run "${args[@]}"
    fi
  else
    echo "invalid MATRIX_TEST_MODE: $test_mode" >&2
    exit 1
  fi
}

check_default_has_no_ai_transport() {
  if cargo tree -p sockudo -e features | grep -q 'ai-transport'; then
    echo "default feature graph contains ai-transport" >&2
    exit 1
  fi
}

run_matrix() {
  cargo fmt --all -- --check

  run_cargo_for_combo "default-no-ai" 
  check_default_has_no_ai_transport
  run_cargo_for_combo "ai-transport-alone" --no-default-features --features "ai-transport"
  run_cargo_for_combo "ai-transport-redis" --no-default-features --features "v2,ai-transport,redis"
  run_cargo_for_combo "ai-transport-redis-cluster" --no-default-features --features "v2,ai-transport,redis-cluster"
  run_cargo_for_combo "ai-transport-nats" --no-default-features --features "v2,ai-transport,nats"
  run_cargo_for_combo "full" --no-default-features --features "full"
  run_cargo_for_combo "no-default-pusher" --no-default-features
}

case "${1:-}" in
  ci-evidence)
    check_ci_evidence
    ;;
  release-evidence)
    check_release_evidence
    ;;
  matrix)
    run_matrix
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
