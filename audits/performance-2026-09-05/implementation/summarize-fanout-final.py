from pathlib import Path
import json
import statistics

root = Path(__file__).resolve().parent / "results/fanout"
cases = {
    "F2-redis": ("F2,", ["payload"], ["active", "rss_kib", "control_ns", "admitted", "gap_notifications"]),
    "F2-nats": ("F2,", ["payload"], ["active", "rss_kib", "control_ns", "admitted", "gap_notifications"]),
    "F2-rabbitmq": ("F2-rabbitmq,", ["payload"], ["admitted", "rejected", "rss_kib", "publish_ns", "control_ns"]),
    "F5": ("F5,", ["channels"], ["ns", "delivered"]),
    "F5-health": ("F5-health,", ["metadata_timeout_ms"], ["timer_ns"]),
    "F8-versioned": ("F8-versioned ", ["format", "kind", "size"], ["ns", "requested_bytes"]),
    "F8-ordinary": ("F8 ", ["format", "kind", "size"], ["ns", "requested_bytes"]),
}
for case, (marker, fields, metrics) in cases.items():
    result = {}
    for variant in ["baseline", "after"]:
        file = root / f"{case}-{variant}.txt"
        if not file.exists() or (not case.startswith("F8-") and "1 passed; 0 failed" not in file.read_text()):
            break
        groups = {}
        for line in file.read_text().splitlines():
            if marker not in line:
                continue
            values = line.split(marker, 1)[1]
            row = dict(value.split("=", 1) for value in (values.split() if marker.endswith(" ") else values.split(",")))
            key = ",".join(f"{field}={row[field]}" for field in fields)
            groups.setdefault(key, []).append(row)
        assert groups and all(len(rows) == 9 for rows in groups.values()), (case, groups.keys())
        result[variant] = {key: {metric: {"median": statistics.median(int(row[metric]) for row in rows), "min": min(int(row[metric]) for row in rows), "max": max(int(row[metric]) for row in rows), "samples": len(rows)} for metric in metrics} for key, rows in groups.items()}
    if len(result) == 2:
        (root / f"{case}-summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(case, {key: {metric: (result["baseline"][key][metric]["median"], result["after"][key][metric]["median"]) for metric in metrics} for key in result["baseline"]})
