"""Isolated single-finding component pairs; run only in a coordinated quiet window."""
from pathlib import Path
import hashlib
import json
import subprocess

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results/services"
BASELINE = ROOT / "target-services-bench-baseline/release/services-extra"


def main():
    manifest = {
        "baseline_commit": "5613bb291032b6b7660352974b03da9eb0646da0",
        "profile": "release, LTO=true, codegen-units=1",
        "process_repeats": 3,
        "commands": [],
        "sha256": {},
    }
    for repeat in range(3):
        for finding, mode in [("s2", "readiness"), ("s3", "apps"), ("s5", "metrics"), ("s6", "metrics"), ("s12", "lambda"), ("s13", "http")]:
            for side in (["baseline", "current"] if repeat % 2 == 0 else ["current", "baseline"]):
                if finding in ["s12", "s13"]:
                    binary = BASELINE if side == "baseline" else RESULTS / f"{finding}-isolated-binary"
                else:
                    binary = RESULTS / f"components-{'baseline' if side == 'baseline' else finding}-binary"
                manifest["sha256"][str(binary.relative_to(ROOT))] = hashlib.sha256(binary.read_bytes()).hexdigest()
                command = ["/usr/bin/time", "-v", str(binary), mode]
                manifest["commands"].append(command)
                prefix = RESULTS / f"components-final-{finding}-{side}-{repeat}"
                with prefix.with_suffix(".jsonl").open("w") as output, prefix.with_suffix(".stderr").open("w") as errors:
                    subprocess.run(command, stdout=output, stderr=errors, check=True)
                print(f"{finding} {side} repeat {repeat} passed", flush=True)
    (RESULTS / "components-final-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
