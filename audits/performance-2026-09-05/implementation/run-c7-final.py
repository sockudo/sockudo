"""C7-only repeated actual-store batch reads; run in a coordinated quiet window."""
from pathlib import Path
import datetime
import hashlib
import json
import os
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "results/core"
BINARIES = {side: ROOT / "binaries" / f"c7-final-{side}" for side in ("baseline", "after")}
CASE = "history::audit_version_batch_bench::benchmark_durable_latest_batch"
backends = sys.argv[1:] or ["postgres", "mysql", "dynamodb", "scylladb", "surrealdb"]
manifest = {
    "baseline_commit": (ROOT / "baseline-commit.txt").read_text().strip(),
    "scope": "C7-only batch overrides on unchanged baseline production",
    "fixture": "initial revision 1 only; exact bytes, 100 unique requested, duplicate and missing IDs",
    "profile": "release LTO, identical five-database and versioned-messages features",
    "started": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "fixture_sha256": hashlib.sha256((ROOT / "durable-version-batch-bench.rs").read_bytes()).hexdigest(),
    "binaries": {side: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                 for side, path in BINARIES.items()},
    "runs": [],
}
assert manifest["binaries"]["baseline"]["sha256"] != manifest["binaries"]["after"]["sha256"]
for backend in backends:
    for count in [int(n) for n in os.environ.get("AUDIT_COUNTS", "1000,10000").split(",")]:
        baseline_incompatible = False
        for repeat in range(1, 4):
            for side in (["baseline", "after"] if repeat % 2 else ["after", "baseline"]):
                if side == "baseline" and baseline_incompatible:
                    continue
                prefix = OUT / f"c7-final-{backend}-{count}-{side}-{repeat}"
                command = ["/usr/bin/time", "-v", str(BINARIES[side]), CASE,
                           "--exact", "--ignored", "--nocapture", "--test-threads=1"]
                environment = os.environ | {"AUDIT_BACKEND": backend, "AUDIT_MESSAGES": str(count)}
                with prefix.with_suffix(".txt").open("w") as out, prefix.with_suffix(".resources.txt").open("w") as err:
                    try:
                        result = subprocess.run(command, env=environment, stdout=out, stderr=err, timeout=1200)
                        status = result.returncode
                    except subprocess.TimeoutExpired:
                        status = "timeout1200seconds"
                output = prefix.with_suffix(".txt").read_text()
                errors = prefix.with_suffix(".resources.txt").read_text()
                if status == 0:
                    assert "1 passed; 0 failed" in output
                    samples = [line for line in output.splitlines() if line.startswith(f"BATCH_CSV,{backend},")]
                    assert len(samples) == 9, len(samples)
                known_failure = (backend == "surrealdb" and side == "baseline" and status != 0
                                 and "Missing order idiom" in output + errors)
                manifest["runs"].append({"backend": backend, "count": count, "side": side, "repeat": repeat,
                                         "command": command, "exit_code": status,
                                         "known_baseline_query_incompatibility": known_failure})
                (OUT / f"c7-final-{backends[0]}-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
                print(f"C7 {backend} {count} {side} {repeat}: {status}", flush=True)
                if known_failure:
                    baseline_incompatible = True
                    print("Baseline query incompatibility retained; after exactness continues without a speedup claim", flush=True)
                elif status:
                    raise SystemExit("Unexpected correctness/runtime failure retained; stop for diagnosis")
