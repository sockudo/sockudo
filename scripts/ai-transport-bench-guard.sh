#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUDGET_FILE="${1:-$ROOT_DIR/benches/ai/baselines/ai_hot_paths.budgets.json}"
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
} > "$TARGET_DIR/criterion/ai-transport-hardware.txt"

cargo bench -p sockudo-ai-benches --bench ai_hot_paths -- --noplot --quiet

python3 - "$BUDGET_FILE" "$TARGET_DIR/criterion" <<'PY'
import json
import sys
from pathlib import Path

budget_path = Path(sys.argv[1])
criterion_root = Path(sys.argv[2])
budget = json.loads(budget_path.read_text())
threshold = float(budget.get("threshold_percent", 10)) / 100.0
failures = []
missing = []

estimates = {}
for path in criterion_root.rglob("new/estimates.json"):
    bench_id = path.relative_to(criterion_root).parent.parent.as_posix()
    data = json.loads(path.read_text())
    mean = data.get("mean", {}).get("point_estimate")
    if mean is not None:
        estimates[bench_id] = float(mean)

for entry in budget["benchmarks"]:
    needle = entry["id_substring"]
    matches = [(bench_id, mean) for bench_id, mean in estimates.items() if needle in bench_id]
    if not matches:
        missing.append(needle)
        continue
    max_mean = float(entry["max_mean_ns"]) * (1.0 + threshold)
    for bench_id, mean in matches:
        if mean > max_mean:
            failures.append((bench_id, mean, max_mean))

if missing:
    print("Missing Criterion estimates for AI Transport budget entries:")
    for needle in missing:
        print(f"  - {needle}")
    sys.exit(2)

if failures:
    print("AI Transport benchmark budget regressions:")
    for bench_id, mean, max_mean in failures:
        print(f"  - {bench_id}: mean {mean:.0f} ns > allowed {max_mean:.0f} ns")
    sys.exit(1)

print("AI Transport benchmark budgets passed:")
for bench_id, mean in sorted(estimates.items()):
    if "ai_hot_paths" in bench_id:
        print(f"  - {bench_id}: mean {mean:.0f} ns")
PY
