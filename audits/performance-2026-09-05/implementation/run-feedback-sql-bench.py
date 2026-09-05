"""Run repeated integrated P5/P6 PostgreSQL pairs in fresh synthetic databases."""
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[3]
IMPLEMENTATION = Path(__file__).resolve().parent
RESULTS = IMPLEMENTATION / "results/services"


def sql(container, database, statement):
    return subprocess.run(
        ["podman", "exec", "-i", container, "psql", "--no-psqlrc", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database],
        input=statement,
        text=True,
        capture_output=True,
        check=True,
    )


def main():
    controlled = os.environ.get("SOCKUDO_FEEDBACK_BENCH_CONTROLLED") == "1"
    label = "p5-controlled" if controlled else "p5-p6-sql-fair"
    binary_label = "p5-controlled" if controlled else "p5-sql"
    containers = subprocess.check_output(
        ["podman", "ps", "--format", "{{.Names}} {{.Ports}}"], text=True
    ).splitlines()
    matches = [line.split()[0] for line in containers if "15432->5432" in line]
    assert len(matches) == 1, matches
    container = matches[0]
    migration = "\n".join(
        (ROOT / "ops/migrations/postgres" / name).read_text()
        for name in ["001_push_schema.sql", "003_push_lifecycle.sql"]
    )
    partitions = """
DO $fixture$
DECLARE parent RECORD;
BEGIN
  FOR parent IN
    SELECT c.relname, p.partstrat FROM pg_partitioned_table p
    JOIN pg_class c ON c.oid = p.partrelid
    WHERE c.relname LIKE 'push_%'
      AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhparent = c.oid)
  LOOP
    EXECUTE format('CREATE TABLE %I PARTITION OF %I %s',
      parent.relname || '_audit', parent.relname,
      CASE WHEN parent.partstrat = 'h'
        THEN 'FOR VALUES WITH (MODULUS 1, REMAINDER 0)' ELSE 'DEFAULT' END);
  END LOOP;
END $fixture$;
"""
    manifest = {
        "baseline_commit": "5613bb291032b6b7660352974b03da9eb0646da0",
        "scope": "controlled P5 algorithm on identical current P6/PG storage" if controlled else "integrated P5 feedback plus P6 SQL storage",
        "fixture": container,
        "schema": ["001_push_schema.sql", "003_push_lifecycle.sql"],
        "repeats": 3,
        "inner_samples_per_group": 3,
        "outcomes_per_sample": 256,
        "groups": [1, 64],
        "queue_batch_limit": {"group1": 64, "group64": 4},
    }
    (RESULTS / f"{label}-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for repeat in range(3):
        for side in (["baseline", "current"] if repeat % 2 == 0 else ["current", "baseline"]):
            database = f"sockudo_feedback_bench_{os.getpid()}_{time.time_ns()}"
            sql(container, "postgres", f"CREATE DATABASE {database};")
            try:
                sql(container, database, migration + partitions)
                environment = os.environ | {
                    "SOCKUDO_FEEDBACK_BENCH_POSTGRES_URL": f"postgres://postgres:postgres123@127.0.0.1:15432/{database}"
                }
                prefix = RESULTS / f"{label}-{side}-{repeat}"
                with prefix.with_suffix(".jsonl").open("w") as output, prefix.with_suffix(".stderr").open("w") as errors:
                    subprocess.run(
                        ["/usr/bin/time", "-v", str(RESULTS / f"{binary_label}-{side}-binary")],
                        env=environment,
                        stdout=output,
                        stderr=errors,
                        check=True,
                    )
                print(f"{side} repeat {repeat} passed", flush=True)
            finally:
                sql(container, "postgres", f"DROP DATABASE {database};")


if __name__ == "__main__":
    main()
