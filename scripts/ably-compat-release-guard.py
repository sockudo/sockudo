#!/usr/bin/env python3
"""Validate Criterion and repeated real-topology compatibility evidence."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--budgets", required=True, type=Path)
    parser.add_argument("--criterion-root", type=Path)
    parser.add_argument("--require-criterion-comparison", action="store_true")
    parser.add_argument("--load-result", action="append", default=[], type=Path)
    parser.add_argument("--baseline-load-result", action="append", default=[], type=Path)
    parser.add_argument("--require-independent-runs", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = arguments()
    budgets = read_json(args.budgets)
    failures: list[str] = []
    checked: list[str] = []

    if args.criterion_root:
        check_criterion(
            args.criterion_root,
            budgets["criterion"],
            failures,
            checked,
            args.require_criterion_comparison,
        )

    current = [read_json(path) for path in args.load_result]
    baseline = [read_json(path) for path in args.baseline_load_result]
    minimum_runs = int(budgets["load"]["minimumIndependentRuns"])
    if args.require_independent_runs and len(current) < minimum_runs:
        failures.append(f"release evidence requires {minimum_runs} independent load runs; found {len(current)}")
    for path, result in zip(args.load_result, current):
        check_load_result(path, result, budgets["load"], failures, checked)

    if baseline:
        if len(current) < minimum_runs or len(baseline) < minimum_runs:
            failures.append(
                f"statistical comparison requires at least {minimum_runs} current and baseline runs"
            )
        else:
            check_statistical_regressions(current, baseline, budgets["load"], failures, checked)

    if failures:
        print("Ably compatibility release guard failed:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("Ably compatibility release guard passed:")
    for item in checked:
        print(f"  - {item}")
    return 0


def check_criterion(
    root: Path,
    budget: dict[str, Any],
    failures: list[str],
    checked: list[str],
    require_comparison: bool,
) -> None:
    estimates = [
        bench_id(path, root)
        for path in root.rglob("new/estimates.json")
        if bench_id(path, root).startswith("ably_compat_")
    ]
    for required in budget.get("requiredBenchmarks", []):
        if not any(required in estimate for estimate in estimates):
            failures.append(f"missing required Criterion estimate matching {required!r}")
    if estimates:
        checked.append(f"{len(estimates)} Criterion estimates discovered")
    changes = [
        path
        for path in root.rglob("change/estimates.json")
        if bench_id(path, root).startswith("ably_compat_")
    ]
    threshold = float(budget["maxSignificantRegressionPercent"]) / 100.0
    if not changes:
        message = "Criterion estimates present; no saved-baseline change intervals to gate"
        if require_comparison:
            failures.append(message)
        else:
            checked.append(message)
        return
    for path in changes:
        data = read_json(path)
        mean = data.get("mean", {})
        confidence = mean.get("confidence_interval", {})
        lower = confidence.get("lower_bound")
        upper = confidence.get("upper_bound")
        if lower is None or upper is None:
            failures.append(f"{path}: missing Criterion mean change confidence interval")
            continue
        bench = bench_id(path, root)
        if float(lower) > threshold:
            failures.append(
                f"{bench}: statistically significant mean regression lower bound "
                f"{float(lower) * 100:.2f}% exceeds {threshold * 100:.2f}%"
            )
        else:
            checked.append(
                f"{bench}: change CI [{float(lower) * 100:.2f}%, {float(upper) * 100:.2f}%]"
            )


def check_load_result(path: Path, result: dict[str, Any], budget: dict[str, Any], failures: list[str], checked: list[str]) -> None:
    if result.get("schema") != "sockudo.ably-compat.capacity-result.v1":
        failures.append(f"{path}: not an executable compatibility capacity result")
        return
    if result.get("status") != "passed":
        failures.append(f"{path}: result status is {result.get('status')!r}")
    if not result.get("source", {}).get("binarySha256"):
        failures.append(f"{path}: missing binary hash")
    harness = result.get("harness", {})
    for required_harness_file in (
        "tests/load/ably-compat/capacity-runner.mjs",
        "tests/load/ably-compat/capacity-evidence.mjs",
        "tests/load/ably-compat/budgets.json",
    ):
        if not harness.get(required_harness_file):
            failures.append(f"{path}: missing hash for {required_harness_file}")
    topologies = result.get("topologies", [])
    present_topologies = {topology.get("topology") for topology in topologies}
    missing_topologies = set(budget.get("requiredTopologies", [])) - present_topologies
    if missing_topologies:
        failures.append(f"{path}: missing required topologies {sorted(missing_topologies)}")
    for topology in topologies:
        label = f"{path}:{topology.get('topology')}"
        if topology.get("status") != "passed":
            failures.append(f"{label}: topology failed")
        if topology.get("topology") == "two":
            readiness = topology.get("readiness", {})
            if readiness.get("crossNodeFanout") != "ready":
                failures.append(f"{label}: cross-node fanout did not become ready")
            elif float(readiness.get("durationMs", math.inf)) > float(budget["maxCrossNodeReadyMs"]):
                failures.append(f"{label}: cross-node fanout readiness exceeded budget")
        plateau = topology.get("resources", {}).get("plateau", {})
        if not plateau.get("stalledPeersExercised"):
            failures.append(f"{label}: stalled peers were not exercised")
        if not plateau.get("stalledPeersPlateau"):
            failures.append(f"{label}: RSS did not plateau with stalled peers")
        if not plateau.get("postDisconnectPlateau"):
            failures.append(f"{label}: RSS did not plateau after disconnect")
        for node in plateau.get("nodes", []):
            if float(node.get("stalledSlopeBytesPerSecond", math.inf)) > float(budget["maxRssPlateauSlopeBytesPerSecond"]):
                failures.append(f"{label}: node {node.get('node')} stalled-peer RSS slope exceeded budget")
            if float(node.get("postDisconnectSlopeBytesPerSecond", math.inf)) > float(budget["maxRssPlateauSlopeBytesPerSecond"]):
                failures.append(f"{label}: node {node.get('node')} post-disconnect RSS slope exceeded budget")
            if float(node.get("postDisconnectRatio", math.inf)) > float(budget["maxPostDisconnectRssRatio"]):
                failures.append(f"{label}: node {node.get('node')} post-disconnect RSS ratio exceeded budget")
        for shutdown in topology.get("shutdown", []):
            if not shutdown.get("continuityExplicit"):
                failures.append(f"{label}: node {shutdown.get('node')} shutdown neither drained nor marked continuity")
            if shutdown.get("sensitiveDataLogged") is not False:
                failures.append(f"{label}: node {shutdown.get('node')} did not prove secret/raw-payload log safety")
        scenarios = topology.get("scenarios", [])
        present_scenarios = {scenario.get("name") for scenario in scenarios}
        missing_scenarios = set(budget.get("requiredScenarios", [])) - present_scenarios
        if missing_scenarios:
            failures.append(f"{label}: missing required scenarios {sorted(missing_scenarios)}")
        for scenario in scenarios:
            scenario_label = f"{label}:{scenario.get('name')}"
            if scenario.get("status") != "passed":
                failures.append(f"{scenario_label}: scenario failed")
            correctness = scenario.get("correctness", {})
            for key in ("loss", "duplicates", "reordered", "unexpected", "gaps"):
                if int(correctness.get(key, 0)) != 0:
                    failures.append(f"{scenario_label}: {key}={correctness[key]}")
            encoding = scenario.get("encoding")
            if scenario.get("name") in {
                "steady_publish",
                "burst",
                "payload_64k",
                "encrypted_binary",
                "fanout_1000",
                "slow_consumers",
            } and not encoding:
                failures.append(f"{scenario_label}: missing active-format encode evidence")
            if encoding and not encoding.get("exactlyOncePerFormat"):
                failures.append(f"{scenario_label}: encode count was not exactly once per active format")
            for snapshot in scenario.get("runtimeAfter", []):
                aggregation = snapshot.get("aggregation")
                if aggregation:
                    backlog = int(aggregation.get("backlog", -1))
                    capacity = int(aggregation.get("queueCapacity", -1))
                    if backlog < 0 or capacity <= 0 or backlog > capacity:
                        failures.append(f"{scenario_label}: stats backlog was missing or exceeded its bounded queue")
                    if int(aggregation.get("dropped", 0)) != 0:
                        failures.append(f"{scenario_label}: stats observations were dropped")
            if scenario.get("name") == "recovery_storm":
                if int(scenario.get("resumed", -1)) != int(scenario.get("attempted", 0)):
                    failures.append(f"{scenario_label}: not every connection recovered")
                if int(scenario.get("replaySource", 0)) <= 0:
                    failures.append(f"{scenario_label}: no canonical replay source was observed")
                if int(scenario.get("backendCalls", 0)) <= 0:
                    failures.append(f"{scenario_label}: no recovery backend boundary was observed")
                if int(scenario.get("backendCalls", 0)) > int(scenario.get("backendCallBudget", -1)):
                    failures.append(f"{scenario_label}: recovery backend calls exceeded the constant budget")
            check_latency(scenario_label, scenario, "publishLatencyMs", budget["maxPublishP99Ms"], failures)
            check_latency(scenario_label, scenario, "deliveryLatencyMs", budget["maxDeliveryP99Ms"], failures)
        checked.append(f"{label}: real topology and correctness invariants")


def check_latency(label: str, scenario: dict[str, Any], field: str, maximum: float, failures: list[str]) -> None:
    summary = scenario.get(field, {})
    p99 = summary.get("p99")
    if p99 is not None and float(p99) > float(maximum):
        failures.append(f"{label}: {field}.p99={p99}ms exceeds {maximum}ms")
    if "sampleCount" in summary:
        sample_count = int(summary.get("sampleCount", -1))
        sample_limit = int(summary.get("sampleLimit", -1))
        observation_count = int(summary.get("count", -1))
        if sample_count < 0 or sample_limit <= 0 or sample_count > sample_limit:
            failures.append(f"{label}: {field} sample bound is invalid")
        if observation_count < sample_count:
            failures.append(f"{label}: {field} retained more samples than observations")


def bench_id(path: Path, root: Path) -> str:
    return path.relative_to(root).parent.parent.as_posix()


def check_statistical_regressions(current: list[dict[str, Any]], baseline: list[dict[str, Any]], budget: dict[str, Any], failures: list[str], checked: list[str]) -> None:
    current_samples = collect_samples(current)
    baseline_samples = collect_samples(baseline)
    z_limit = float(budget["significanceZ"])
    limits = {
        "publish_p99": float(budget["maxSignificantLatencyRegressionPercent"]),
        "delivery_p99": float(budget["maxSignificantLatencyRegressionPercent"]),
        "throughput": float(budget["maxSignificantThroughputRegressionPercent"]),
        "peak_rss": float(budget["maxSignificantRssRegressionPercent"]),
    }
    for key in sorted(current_samples.keys() & baseline_samples.keys()):
        for metric, current_values in current_samples[key].items():
            baseline_values = baseline_samples[key].get(metric, [])
            if len(current_values) < 2 or len(baseline_values) < 2:
                continue
            current_mean = statistics.mean(current_values)
            baseline_mean = statistics.mean(baseline_values)
            if baseline_mean == 0:
                continue
            regression = (
                (baseline_mean - current_mean) / baseline_mean * 100
                if metric == "throughput"
                else (current_mean - baseline_mean) / baseline_mean * 100
            )
            z_score = welch_z(current_values, baseline_values, metric == "throughput")
            if regression > limits[metric] and z_score > z_limit:
                failures.append(
                    f"{key}:{metric}: significant regression {regression:.2f}% "
                    f"(z={z_score:.3f}) exceeds {limits[metric]:.2f}%"
                )
            else:
                checked.append(f"{key}:{metric}: regression={regression:.2f}% z={z_score:.3f}")


def collect_samples(results: list[dict[str, Any]]) -> dict[str, dict[str, list[float]]]:
    samples: dict[str, dict[str, list[float]]] = {}
    for result in results:
        for topology in result.get("topologies", []):
            topology_name = topology.get("topology")
            peak_rss = max(
                (float(node.get("peakRssBytes", 0)) for node in topology.get("resources", {}).get("plateau", {}).get("nodes", [])),
                default=0,
            )
            for scenario in topology.get("scenarios", []):
                key = f"{topology_name}:{scenario.get('name')}"
                target = samples.setdefault(key, {"publish_p99": [], "delivery_p99": [], "throughput": [], "peak_rss": []})
                append_if_number(target["publish_p99"], scenario.get("publishLatencyMs", {}).get("p99"))
                append_if_number(target["delivery_p99"], scenario.get("deliveryLatencyMs", {}).get("p99"))
                append_if_number(target["throughput"], scenario.get("throughputPerSecond"))
                append_if_number(target["peak_rss"], peak_rss)
    return samples


def append_if_number(target: list[float], value: Any) -> None:
    if isinstance(value, (int, float)) and math.isfinite(value):
        target.append(float(value))


def welch_z(current: list[float], baseline: list[float], throughput: bool) -> float:
    difference = statistics.mean(baseline) - statistics.mean(current) if throughput else statistics.mean(current) - statistics.mean(baseline)
    variance = statistics.variance(current) / len(current) + statistics.variance(baseline) / len(baseline)
    return 0.0 if variance <= 0 else difference / math.sqrt(variance)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


if __name__ == "__main__":
    sys.exit(main())
