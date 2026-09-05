"""Run preserved source-isolated probes; one quiet timing window at a time."""
from pathlib import Path
import datetime
import hashlib
import json
import os
import platform
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "results/core"
CASE = "middleware::audit_body_bench::benchmark_auth_body_admission"

def binary_for(finding, side):
    names = {
        "c2": (f"target-c2-memory-{side}", "sockudo-compact-memory-audit"),
        "c3": (f"target-c3-memory-{side}", "sockudo-annotation-memory-audit"),
        "c10": (f"target-c10-{side}", "sockudo-idle-history-audit"),
    }
    if finding in names:
        target, binary = names[finding]
        return ROOT / target / "release" / binary
    for path in (ROOT / f"target-c9-{side}/release/deps").glob("sockudo-*"):
        if path.is_file() and os.access(path, os.X_OK):
            result = subprocess.run([str(path), "--list"], capture_output=True, text=True)
            if CASE in result.stdout:
                return path
    raise RuntimeError("compiled C9 probe not found")

for finding in sys.argv[1:]:
    binaries = {side: binary_for(finding, side) for side in ["baseline", "after"]}
    manifest = {
        "started": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "baseline_commit": (ROOT / "baseline-commit.txt").read_text().strip(),
        "profile": "release; Cargo manifests and build logs retained alongside probes",
        "host": platform.platform(),
        "cpu": subprocess.check_output(["lscpu"], text=True),
        "finding": finding,
        "repeats": 5,
        "binaries": {side: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for side, path in binaries.items()},
        "runs": [],
    }
    for repeat in range(1, 6):
        for side in (["baseline", "after"] if repeat % 2 else ["after", "baseline"]):
            command = ["/usr/bin/time", "-v", str(binaries[side])]
            env = os.environ.copy()
            if finding == "c9":
                command += [CASE, "--exact", "--ignored", "--nocapture", "--test-threads=1"]
                env["EXPECT_BOUNDED_AUTH"] = str(int(side == "after"))
            if finding == "c10":
                env["EXPECT_IDLE_CLEANUP"] = str(int(side == "after"))
            prefix = OUT / f"{finding}-isolated-{side}-{repeat}"
            with prefix.with_suffix(".txt").open("w") as output, prefix.with_suffix(".resources.txt").open("w") as errors:
                result = subprocess.run(command, env=env, stdout=output, stderr=errors)
            manifest["runs"].append({"command": command, "side": side, "repeat": repeat, "exit_code": result.returncode})
            (OUT / f"{finding}-isolated-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            if result.returncode:
                raise SystemExit(f"{finding} {side} {repeat} failed; see {prefix}")
    print(f"{finding}: all 5 alternating process pairs passed", flush=True)
