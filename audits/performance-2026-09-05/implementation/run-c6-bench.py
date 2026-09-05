from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys

root = Path(__file__).resolve().parent
results = root / "results/fanout"
manifest = {
    "profile": "release (codegen-units=1, LTO=true)",
    "scope": "C6 history append/accounting/retention only; C8 reads restored to baseline",
    "baseline_commit": (root / "baseline-commit.txt").read_text().strip(),
    "features": "default v2 + versioned-messages,ai-transport,postgres,mysql,dynamodb,surrealdb,scylladb",
    "samples_per_shape": 9,
    "writes_per_sample": 8,
    "shapes": {"retained_records": [64, 1024], "payload_bytes": [128, 16384]},
    "commands": [],
    "binaries": {},
    "exit_codes": {},
}
for backend in sys.argv[1:] or ["postgres", "mysql", "dynamodb", "surreal", "scylla"]:
    assert backend in ["postgres", "mysql", "dynamodb", "surreal", "scylla"]
    for variant in ["baseline", "after"]:
        binary = root / "binaries" / f"c6-{variant}"
        manifest["binaries"][variant] = hashlib.sha256(binary.read_bytes()).hexdigest()
        cmd = [
            "/usr/bin/time", "-v", str(binary),
            f"history::benchmark_tests::benchmark_history_{backend}",
            "--ignored", "--exact", "--nocapture", "--test-threads=1",
        ]
        manifest["commands"].append(cmd)
        prefix = results / f"C6-{backend}-{variant}"
        print(f"running {backend} {variant}", flush=True)
        with prefix.with_suffix(".txt").open("w") as out, Path(str(prefix) + ".resources.txt").open("w") as err:
            run = subprocess.run(cmd, stdout=out, stderr=err, env={**os.environ, "SOCKUDO_C8_SCYLLA_ADDR": "127.0.0.1:19043"})
        manifest["exit_codes"][f"{backend}-{variant}"] = run.returncode
        manifest_file = results / "C6-manifest.json"
        previous = json.loads(manifest_file.read_text()) if manifest_file.exists() else {}
        previous.setdefault("exit_codes", {}).update(manifest["exit_codes"])
        manifest["exit_codes"] = previous["exit_codes"]
        previous.setdefault("commands", []).extend(cmd for cmd in manifest["commands"] if cmd not in previous["commands"])
        manifest["commands"] = previous["commands"]
        manifest_file.write_text(json.dumps(manifest, indent=2) + "\n")
        if run.returncode:
            raise SystemExit(run.returncode)
        assert "1 passed; 0 failed" in prefix.with_suffix(".txt").read_text(), "benchmark must execute exactly one successful test"
