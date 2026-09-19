#!/usr/bin/env python3
# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# SPDX-License-Identifier: MPL-2.0
"""Fail closed on state changes or an unproven 1% throughput non-regression.

Run identical baseline/candidate binaries in interleaved fresh processes on the
same physical CPU set. Each case has at most two predeclared looks, each with a
one-sided 97.5% lower confidence bound >= 0.99 (a nominal 5% false acceptance budget
across both looks). Uncertain first looks extend to a fixed maximum using ALL
samples. Preserve outliers. Do not build or run tests concurrently.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys


STATE_FIELDS = ("predicates", "parallel", "ticks", "sent", "received", "checksum", "digest", "overflow")
MIN_SPEEDUP = 0.99
LOOK_CONFIDENCE = 0.975
MAX_CALIBRATION_ROUNDS = 5
MAX_CYCLES = 1000000000


def calibrate(case: dict, target: float, run, save=None) -> list[dict]:
    """Fix identical work after checking the scaled run, including startup effects.

    Calibration is never included in acceptance samples. The workload cycle cap
    can limit duration; retain that fact rather than silently claiming the target.
    """
    records = []
    for attempt in range(MAX_CALIBRATION_ROUNDS):
        trials = {variant: run(variant, case, f"calibration-{attempt}")
                  for variant in ("baseline", "candidate")}
        elapsed = {variant: result[0] for variant, result in trials.items()}
        if any(not math.isfinite(t) or t <= 0 for t in elapsed.values()):
            raise ValueError("invalid calibration elapsed time")
        equal = trials["baseline"][1] == trials["candidate"][1]
        reached = min(elapsed.values()) >= target
        capped = case["cycles"] >= MAX_CYCLES
        records.append({"cycles": case["cycles"], "seconds": elapsed,
                        "state_matches": equal, "target_reached": reached, "capped": capped})
        if save is not None:
            save(records)
        if not equal:
            raise RuntimeError(f"calibration determinism mismatch: {case['name']}")
        if reached or capped:
            return records
        if attempt + 1 == MAX_CALIBRATION_ROUNDS:
            raise RuntimeError(f"calibration target not reached: {case['name']}")
        case["cycles"] = min(MAX_CYCLES, max(case["cycles"] + 1,
            math.ceil(case["cycles"] * target * 1.05 / min(elapsed.values()))))
    raise AssertionError("unreachable calibration state")


def confidence(samples: list[float], seed: int = 149) -> dict:
    if len(samples) < 15 or any(not math.isfinite(x) or x <= 0 for x in samples):
        raise ValueError("at least 15 finite positive paired speedups are required")
    logs = [math.log(x) for x in samples]
    rng = random.Random(seed)
    bootstrap = sorted(statistics.median(rng.choices(logs, k=len(logs))) for _ in range(20000))
    return {
        "median_speedup": math.exp(statistics.median(logs)),
        "lower_speedup": math.exp(bootstrap[499]),
        "upper_speedup": math.exp(bootstrap[19500]),
        "confidence_level": LOOK_CONFIDENCE,
        "sample_count": len(samples),
        "min_speedup": min(samples),
        "max_speedup": max(samples),
        "pass": math.exp(bootstrap[499]) >= MIN_SPEEDUP,
    }


def needs_extension(result: dict, maximum: int) -> bool:
    """Only an uncertain first look may consume the predeclared second batch."""
    return (not result["pass"] and result["upper_speedup"] >= MIN_SPEEDUP
            and result["sample_count"] < maximum)


def physical_cpus() -> list[int]:
    allowed = os.sched_getaffinity(0)
    rows = subprocess.check_output(["lscpu", "-p=CPU,CORE,SOCKET"], text=True)
    cores = {}
    for line in rows.splitlines():
        if line.startswith("#"):
            continue
        cpu, core, socket = map(int, line.split(","))
        if cpu in allowed:
            cores.setdefault((socket, core), cpu)
    return list(cores.values())[:4]


def cases(workers: int) -> list[dict]:
    result = [{"name": "single-clock-floor", "kind": "floor", "cycles": 10000000}]
    for clock in (0, 1):
        for threads in (1, workers):
            for dynamic in ((0,) if threads == 1 else (0, 1)):
                for interval in (0, 1, 64):
                    result.append({"name": f"clock{clock}-threads{threads}-dynamic{dynamic}-poll{interval}",
                                   "kind": "scheduler", "args": [clock, threads, 4, 64, 1, dynamic, interval],
                                   "cycles": 2000})
    for profile in ("nucleus", "port", "broadcast", "backpressure", "memory"):
        for threads in (1, workers):
            result.append({"name": f"{profile}-threads{threads}", "kind": "representative",
                           "profile": profile, "threads": threads, "cycles": 1000})
    return result


def executable(case: dict) -> str:
    return {"floor": "chronon_single_clock_regression_benchmark",
            "scheduler": "chronon_scheduler_invocation_benchmark",
            "representative": "chronon_representative_workload_benchmark"}[case["kind"]]


def command(build: Path, case: dict, cpus: list[int]) -> list[str]:
    mask = cpus[:1] if case["kind"] == "floor" else cpus
    argv = ["taskset", "-c", ",".join(map(str, mask)), str(build / "benchmark" / executable(case))]
    if case["kind"] == "floor":
        return argv + [str(case["cycles"])]
    if case["kind"] == "scheduler":
        return argv + [*map(str, case["args"]), str(case["cycles"])]
    return argv + ["--profile", case["profile"], "--seed", "149", "--units", "16",
                   "--threads", str(case["threads"]), "--warmup", "512",
                   "--cycles", str(case["cycles"]), "--repetitions", "1"]


def parse(case: dict, output: str) -> tuple[float, dict]:
    lines = output.strip().splitlines()
    if case["kind"] in ("floor", "scheduler"):
        row = next(csv.DictReader(io.StringIO("\n".join(lines[-2:]))))
        keys = ("cycles", "digest") if case["kind"] == "floor" else STATE_FIELDS
        if case["kind"] == "scheduler" and row["overflow"] != "0":
            raise ValueError("transport overflow")
        seconds = float(row["wall_s" if case["kind"] == "floor" else "run_s"])
        state = {key: row[key] for key in keys}
    else:
        machine = [line for line in lines if line.startswith("RESULT ")]
        digests = [line.strip() for line in lines if line.strip().startswith("digest=")]
        if len(machine) != 1 or len(digests) != 1:
            raise ValueError("expected exactly one result and one state digest")
        row = dict(field.split("=", 1) for field in machine[0].split()[1:])
        seconds = float(row["median_seconds"])
        state = dict(field.split("=", 1) for field in digests[0].split())
        state["mode"] = row["mode"]
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("invalid elapsed time")
    return seconds, state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--repeats", type=int, default=51)
    parser.add_argument("--max-repeats", type=int, default=201,
                        help="fixed second-look total for uncertain cases; retains first batch")
    parser.add_argument("--seconds", type=float, default=2.0)
    parser.add_argument("--cpus", help="homogeneous physical CPU IDs; default first four physical cores")
    parser.add_argument("--case", action="append", help="exact case names for diagnosis; never a full gate")
    args = parser.parse_args()
    if args.repeats < 15 or args.seconds < 0.25:
        parser.error("at least 15 pairs and 0.25 seconds per sample are required")
    if args.max_repeats < args.repeats:
        parser.error("max-repeats must be at least repeats")
    cpus = list(map(int, args.cpus.split(","))) if args.cpus else physical_cpus()
    if len(cpus) < 2 or len(set(cpus)) != len(cpus) or not set(cpus) <= os.sched_getaffinity(0):
        parser.error("at least two distinct allowed physical CPUs are required")
    matrix = cases(min(4, len(cpus)))
    if args.case:
        requested = set(args.case)
        matrix = [case for case in matrix if case["name"] in requested]
        if {case["name"] for case in matrix} != requested:
            parser.error("unknown case name")
    builds = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {"base_sha": args.base_sha, "head_sha": args.head_sha, "platform": platform.platform(),
                "cpus": cpus, "repeats": args.repeats, "max_repeats": args.max_repeats,
                "minimum_speedup": MIN_SPEEDUP, "per_look_confidence": LOOK_CONFIDENCE,
                "maximum_looks": 2, "false_acceptance_budget": 0.05,
                "target_seconds": args.seconds, "complete_matrix": not args.case,
                "maximum_calibration_rounds": MAX_CALIBRATION_ROUNDS,
                "calibration_cycle_cap": MAX_CYCLES,
                "lscpu": subprocess.check_output(["lscpu"], text=True), "binaries": {}}
    for variant, build in builds.items():
        metadata["binaries"][variant] = {}
        for name in sorted({executable(case) for case in matrix}):
            binary = build / "benchmark" / name
            metadata["binaries"][variant][name] = hashlib.sha256(binary.read_bytes()).hexdigest()
        metadata[variant + "_cache"] = (build / "CMakeCache.txt").read_text()
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    env = os.environ.copy()
    env["CHRONON_BENCH_PIN_WORKERS"] = "1"
    env["CHRONON_BENCH_WARM_CPUS"] = "1"
    rng = random.Random(149)
    results = []

    def run(variant: str, case: dict, label: str) -> tuple[float, dict]:
        argv = command(builds[variant], case, cpus)
        proc = subprocess.run(argv, env=env, text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=180)
        stem = args.output / f"{case['name']}-{label}-{variant}"
        stem.with_suffix(".log").write_text(proc.stdout)
        if proc.returncode:
            raise RuntimeError(f"benchmark failed: {argv}; see {stem}.log")
        return parse(case, proc.stdout)

    for case in matrix:
        # Calibration fixes identical work for both variants; it is never a sample.
        def save_calibration(records):
            (args.output / f"{case['name']}-calibration.json").write_text(
                json.dumps(records, indent=2) + "\n")
            (args.output / "cases.json").write_text(json.dumps(matrix, indent=2) + "\n")
        calibrate(case, args.seconds, run, save_calibration)
        reference = None
        pairs = []
        samples = []
        looks = []
        for target in dict.fromkeys((args.repeats, args.max_repeats)):
            for repetition in range(len(samples), target):
                order = list(builds)
                rng.shuffle(order)
                pair = {}
                for variant in order:
                    seconds, state = run(variant, case, str(repetition))
                    if reference is not None and state != reference:
                        raise RuntimeError(f"determinism mismatch: {case['name']} {variant}: {state} != {reference}")
                    reference = state
                    pair[variant] = seconds
                pairs.append(pair)
                samples.append(pair["baseline"] / pair["candidate"])
                (args.output / f"{case['name']}-samples.json").write_text(json.dumps(pairs, indent=2) + "\n")
            look = confidence(samples)
            looks.append(look)
            (args.output / f"{case['name']}-looks.json").write_text(json.dumps(looks, indent=2) + "\n")
            if not needs_extension(look, args.max_repeats):
                break
            print(f"{case['name']}: uncertain at {len(samples)} pairs; "
                  f"extend once to {args.max_repeats}, retaining all samples", flush=True)
        result = {"case": case, "state": reference, "looks": looks, **looks[-1]}
        results.append(result)
        (args.output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
        print(f"{case['name']}: median={result['median_speedup']:.5f} "
              f"lower97.5={result['lower_speedup']:.5f} pairs={result['sample_count']} "
              f"{'PASS' if result['pass'] else 'FAIL/UNCERTAIN'}",
              flush=True)
        # A final negative result already rejects this revision. Return its raw
        # evidence to CI immediately; later cases cannot compensate for it.
        if not result["pass"]:
            break
    complete = len(results) == len(matrix)
    passed = bool(results) and complete and all(result["pass"] for result in results)
    (args.output / "verdict.json").write_text(json.dumps({"pass": passed,
        "complete_matrix": not args.case and complete,
        "completed_cases": len(results), "expected_cases": len(matrix),
        "base_sha": args.base_sha, "head_sha": args.head_sha}, indent=2) + "\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
