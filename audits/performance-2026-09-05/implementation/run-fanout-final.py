"""Run already-built isolated variants; preserve commands, hashes and process resources."""
from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys

root = Path(__file__).resolve().parent
results = root / "results/fanout"
cases = {
    "F1": ("f1-f4-baseline", "f1-after", "local_adapter::broadcast::regression_tests::benchmark_shared_fanout_preparation", {}, {}),
    "F2-redis": ("f2-f5-baseline", "f2-after", "benchmark_redis_ingress", {}, {"SOCKUDO_EXPECT_BOUNDED_INGRESS": "1"}),
    "F2-nats": ("f2-rabbit-baseline", "f2-rabbit-after", "benchmark_nats_ingress", {}, {"SOCKUDO_EXPECT_BOUNDED_INGRESS": "1"}),
    "F2-rabbitmq": ("f2-rabbit-baseline", "f2-rabbit-after", "benchmark_rabbitmq_ingress", {"SOCKUDO_RABBITMQ_TEST_URL": "amqp://guest:guest@127.0.0.1:15691/%2f"}, {"SOCKUDO_EXPECT_BOUNDED_INGRESS": "1"}),
    "F5": ("f2-f5-baseline", "f5-after", "benchmark_kafka_partitioned_fanout", {"SOCKUDO_KAFKA_TEST_BROKER": "127.0.0.1:19094"}, {"SOCKUDO_EXPECT_PARTITIONED_KAFKA": "1"}),
    "F5-health": ("f2-f5-baseline", "f5-after", "benchmark_kafka_blocked_health", {"SOCKUDO_KAFKA_TEST_BROKER": "127.0.0.1:19094", "SOCKUDO_KAFKA_BENCH_CONTAINER": "sockudo-perf-services-kafka"}, {"SOCKUDO_EXPECT_PARTITIONED_KAFKA": "1"}),
    "F8-versioned": ("f8-versioned-baseline", "f8-versioned-after", None, {"SOCKUDO_F8_VERSIONED_ONLY": "1"}, {}),
    "F8-ordinary": ("f8-versioned-baseline", "f8-versioned-after", None, {}, {}),
}
for case in sys.argv[1:]:
    baseline, after, test, shared_env, after_env = cases[case]
    manifest = {"case": case, "baseline_commit": (root / "baseline-commit.txt").read_text().strip(), "profile": "release; compilation and fixture setup excluded", "runs": []}
    for variant, name in [("baseline", baseline), ("after", after)]:
        binary = root / "binaries" / name
        cmd = ["/usr/bin/time", "-v", str(binary)]
        if test:
            cmd += [test, "--ignored", "--exact", "--nocapture", "--test-threads=1"]
        environment = {**shared_env, **(after_env if variant == "after" else {})}
        prefix = results / f"{case}-{variant}"
        print(f"running {case} {variant}", flush=True)
        with prefix.with_suffix(".txt").open("w") as out, Path(str(prefix) + ".resources.txt").open("w") as err:
            run = subprocess.run(cmd, stdout=out, stderr=err, env={**os.environ, **environment})
        manifest["runs"].append({"variant": variant, "command": cmd, "environment": environment, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "exit_code": run.returncode})
        (results / f"{case}-final-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        if run.returncode:
            raise SystemExit(run.returncode)
        if test:
            assert "1 passed; 0 failed" in prefix.with_suffix(".txt").read_text()
