#!/usr/bin/env bash
# Prebuild the exact artifact sets the cached CI jobs compile, for the cache
# seeder in ci.yml. .github/actions/rust-setup hashes this file and
# test-shards.sh into the cache key: the seeder saves only on a key miss, so
# editing either file is what gets a new prebuild set into the cache.
set -euo pipefail

cargo test --workspace --all-features --all-targets --no-run
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Each shard resolves features over its own members, so the workspace build
# above does not produce the artifacts the shards use.
for shard in $(scripts/ci/test-shards.sh --list); do
  # shellcheck disable=SC2046 # one word per package flag
  cargo test --all-features $(scripts/ci/test-shards.sh "$shard") --no-run
done

# The benchmark guards build in the bench profile (target/release), which
# nothing above writes.
cargo bench -p sockudo-ably-compat --features bench --bench compatibility_hot_paths --no-run
cargo bench -p sockudo-ai-benches --bench ai_hot_paths --no-run

# Ably upstream compatibility builds the server with a narrow feature set
# that resolves differently from --all-features. Keep in sync with
# ably-upstream-compat.yml.
cargo build -p sockudo --features v2,ai-transport,ably-compat,redis,postgres,push,monolith
