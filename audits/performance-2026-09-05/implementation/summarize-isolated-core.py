from pathlib import Path
import csv
import json
import statistics

root = Path(__file__).resolve().parent / "results/core"
summary = {}
for finding in ["c2", "c3", "c9", "c10"]:
    summary[finding] = {}
    for side in ["baseline", "after"]:
        groups = {}
        for path in sorted(root.glob(f"{finding}-isolated-{side}-[1-5].txt")):
            lines = path.read_text().splitlines()
            if finding == "c9":
                lines = [line.removeprefix("AUTH_CSV,") for line in lines if line.startswith("AUTH_CSV,")]
                lines.insert(0, "offered_bytes,limit_bytes,sample,polled_bytes,status,elapsed_us")
            for row in csv.DictReader(lines):
                key = row[{"c2": "appends", "c3": "retained", "c9": "offered_bytes", "c10": "channels"}[finding]]
                groups.setdefault(key, []).append(row)
        summary[finding][side] = {}
        for key, rows in groups.items():
            fields = {}
            for field in rows[0]:
                try:
                    values = [int(row[field]) for row in rows]
                except ValueError:
                    continue
                fields[field] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
            summary[finding][side][key] = {"rows": len(rows), "metrics": fields}
(root / "isolated-core-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
