#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUDGET_FILE="${1:-$ROOT_DIR/benches/presence/presence_registry.budgets.json}"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT_DIR/target}"

cd "$ROOT_DIR"

mkdir -p "$TARGET_DIR/criterion"
{
  echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "uname=$(uname -a)"
  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n machdep.cpu.brand_string 2>/dev/null | sed 's/^/cpu=/' || true
    sysctl -n hw.ncpu 2>/dev/null | sed 's/^/logical_cpus=/' || true
    sysctl -n hw.memsize 2>/dev/null | sed 's/^/mem_bytes=/' || true
  fi
  if command -v lscpu >/dev/null 2>&1; then
    lscpu | sed 's/^/lscpu: /'
  fi
} > "$TARGET_DIR/criterion/presence-hardware.txt"

cargo bench -p sockudo-core --bench presence_registry -- --noplot --quiet

python3 - "$BUDGET_FILE" "$TARGET_DIR/criterion" <<'PY'
import json
import sys
from pathlib import Path

budget = json.loads(Path(sys.argv[1]).read_text())
criterion_root = Path(sys.argv[2]) / "presence_member_cycle"
maximum_ratio = float(budget["maximum_native_to_legacy_ratio"])
maximum_parallel_ratio = float(budget["maximum_parallel_native_to_legacy_ratio"])

def slope(benchmark: str) -> dict:
    path = criterion_root / benchmark / "new" / "estimates.json"
    if not path.exists():
        print(f"Missing Criterion estimate: {path}")
        sys.exit(2)
    return json.loads(path.read_text())["slope"]

legacy = slope("legacy_facade_dashmap")
native = slope("native_authority")
legacy_lower = float(legacy["confidence_interval"]["lower_bound"])
native_upper = float(native["confidence_interval"]["upper_bound"])
ratio = float(native["point_estimate"]) / float(legacy["point_estimate"])
allowed = legacy_lower * maximum_ratio

if native_upper > allowed:
    print("Presence benchmark regression:")
    print(
        f"  native upper bound {native_upper:.2f} ns exceeds "
        f"legacy lower bound budget {allowed:.2f} ns"
    )
    print(f"  point-estimate ratio: {ratio:.3f}")
    sys.exit(1)

def parallel_slope(benchmark: str) -> dict:
    path = (
        Path(sys.argv[2])
        / f"presence_parallel_cycle_{benchmark}"
        / "new"
        / "estimates.json"
    )
    if not path.exists():
        print(f"Missing Criterion estimate: {path}")
        sys.exit(2)
    return json.loads(path.read_text())["slope"]

legacy_parallel = parallel_slope("legacy_disjoint_clients_8")
native_parallel = parallel_slope("disjoint_clients_8")
legacy_parallel_lower = float(
    legacy_parallel["confidence_interval"]["lower_bound"]
)
native_parallel_upper = float(
    native_parallel["confidence_interval"]["upper_bound"]
)
parallel_ratio = (
    float(native_parallel["point_estimate"])
    / float(legacy_parallel["point_estimate"])
)
parallel_allowed = legacy_parallel_lower * maximum_parallel_ratio

if native_parallel_upper > parallel_allowed:
    print("Parallel presence benchmark regression:")
    print(
        f"  native upper bound {native_parallel_upper:.2f} ns exceeds "
        f"legacy lower bound budget {parallel_allowed:.2f} ns"
    )
    print(f"  point-estimate ratio: {parallel_ratio:.3f}")
    sys.exit(1)

print("Presence benchmark guard passed:")
print(f"  native/legacy point-estimate ratio: {ratio:.3f}")
print(f"  native upper bound: {native_upper:.2f} ns")
print(f"  legacy lower bound: {legacy_lower:.2f} ns")
print(f"  parallel native/legacy point-estimate ratio: {parallel_ratio:.3f}")
print(f"  parallel native upper bound: {native_parallel_upper:.2f} ns")
print(f"  parallel legacy lower bound: {legacy_parallel_lower:.2f} ns")
PY
