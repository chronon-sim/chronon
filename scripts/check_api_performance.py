#!/usr/bin/env python3
# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# SPDX-License-Identifier: MPL-2.0
"""Compare one complete baseline/candidate run per scenario; timing is advisory.

Both revisions use identical fixed work and CPU affinity. Record process wall
time and the benchmark's timed simulation region, and require identical state.
No calibration, repeated sampling, confidence interval, or performance threshold.
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
import subprocess
import sys
import time

STATE_FIELDS = ("predicates", "parallel", "ticks", "sent", "received", "checksum", "digest", "overflow")
METHOD = "single-run-wall-time"


def physical_cpus(limit: int | None = 4) -> list[int]:
    allowed = os.sched_getaffinity(0)
    rows = subprocess.check_output(["lscpu", "-p=CPU,CORE,SOCKET"], text=True)
    cores = {}
    for line in rows.splitlines():
        if line.startswith("#"):
            continue
        cpu, core, socket = map(int, line.split(","))
        if cpu in allowed:
            cores.setdefault((socket, core), cpu)
    return list(cores.values())[:limit]


def cases(workers: int) -> list[dict]:
    result = [{"name": "single-clock-floor", "kind": "floor", "cycles": 100000000}]
    for clock in (0, 1):
        for threads in (1, workers):
            for dynamic in ((0,) if threads == 1 else (0, 1)):
                for interval in (0, 1, 64):
                    result.append({"name": f"clock{clock}-threads{threads}-dynamic{dynamic}-poll{interval}",
                                   "kind": "scheduler", "args": [clock, threads, 4, 64, 1, dynamic, interval],
                                   "cycles": 100000 if interval == 1 else 1000000})
    for profile in ("nucleus", "port", "broadcast", "backpressure", "memory"):
        for threads in (1, workers):
            result.append({"name": f"{profile}-threads{threads}", "kind": "representative",
                           "profile": profile, "threads": threads, "cycles": 50000})
    # Keep the representative port/scheduler paths near the start of the queue.
    priority = {f"nucleus-threads{workers}": 0,
                f"clock0-threads{workers}-dynamic1-poll1": 1,
                f"clock1-threads{workers}-dynamic0-poll1": 2,
                f"backpressure-threads{workers}": 3,
                f"clock0-threads{workers}-dynamic0-poll64": 4,
                f"clock0-threads{workers}-dynamic0-poll0": 5,
                f"clock0-threads{workers}-dynamic0-poll1": 6}
    return sorted(result, key=lambda case: priority.get(case["name"], 7))


def executable(case: dict) -> str:
    return {"floor": "chronon_single_clock_regression_benchmark",
            "scheduler": "chronon_scheduler_invocation_benchmark",
            "representative": "chronon_representative_workload_benchmark"}[case["kind"]]


def command(build: Path, case: dict, cpus: list[int]) -> list[str]:
    threads = (case["args"][1] if case["kind"] == "scheduler"
               else case.get("threads", 1))
    mask = cpus[:1] if threads == 1 else cpus
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


def run_benchmark(argv: list[str], env: dict, log: Path, timeout: float):
    """Retain partial output when a benchmark hangs, as well as normal failures."""
    started = time.monotonic()
    try:
        proc = subprocess.run(argv, env=env, text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=timeout)
        wall_seconds = time.monotonic() - started
    except subprocess.TimeoutExpired as error:
        output = error.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        log.write_text(output)
        raise RuntimeError(f"benchmark timed out after {timeout:.1f}s: {argv}; see {log}") from error
    log.write_text(proc.stdout)
    if proc.returncode:
        raise RuntimeError(f"benchmark failed (exit {proc.returncode}): {argv}; see {log}")
    return proc.stdout, wall_seconds


def remaining_timeout(deadline: float, maximum: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("scenario wall-time budget exhausted; incomplete measurement")
    return min(maximum, remaining)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--cpus", help="physical CPUs shared by workers and caller")
    parser.add_argument("--workers", type=int, choices=(2, 4), default=2)
    parser.add_argument("--case-timeout", type=float, default=120)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    args = parser.parse_args()
    if not math.isfinite(args.case_timeout) or args.case_timeout <= 0:
        parser.error("case timeout must be finite and positive")
    cpus = list(map(int, args.cpus.split(","))) if args.cpus else physical_cpus(args.workers)
    if (len(cpus) != args.workers or len(set(cpus)) != len(cpus)
            or not set(cpus) <= os.sched_getaffinity(0)):
        parser.error("one distinct allowed physical CPU per worker is required")
    matrix = cases(args.workers)
    if not 0 <= args.shard_index < args.shard_count <= len(matrix):
        parser.error("invalid shard index/count")
    matrix = matrix[args.shard_index::args.shard_count]
    builds = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    metadata = {"base_sha": args.base_sha, "head_sha": args.head_sha,
                "platform": platform.platform(), "cpus": cpus, "workers": args.workers,
                "shard_index": args.shard_index, "shard_count": args.shard_count,
                "case_order": [case["name"] for case in matrix],
                "measurement_method": METHOD, "performance_enforced": False,
                "runs_per_variant": 1, "case_timeout": args.case_timeout,
                "lscpu": subprocess.check_output(["lscpu"], text=True), "binaries": {}}
    for variant, build in builds.items():
        metadata["binaries"][variant] = {
            name: hashlib.sha256((build / "benchmark" / name).read_bytes()).hexdigest()
            for name in {executable(case) for case in matrix}}
        metadata[variant + "_cache"] = (build / "CMakeCache.txt").read_text()
    args.output.mkdir(parents=True, exist_ok=False)

    def write(name, value):
        (args.output / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")

    write("metadata", metadata)
    env = {**os.environ, "CHRONON_BENCH_PIN_WORKERS": "1", "CHRONON_BENCH_WARM_CPUS": "1"}
    results = []
    error = None
    for case in matrix:
        deadline = time.monotonic() + args.case_timeout
        # Different cases do not all run the baseline first on the shared host.
        order = ["baseline", "candidate"]
        random.Random(f"api-performance-149:{case['name']}").shuffle(order)
        runs = {}
        variant = None
        try:
            for variant in order:
                argv = command(builds[variant], case, cpus)
                output, wall_seconds = run_benchmark(
                    argv, env, args.output / f"{case['name']}-{variant}.log",
                    remaining_timeout(deadline, args.case_timeout))
                benchmark_seconds, state = parse(case, output)
                if not math.isfinite(wall_seconds) or wall_seconds <= 0:
                    raise ValueError("invalid process wall time")
                runs[variant] = {"wall_seconds": wall_seconds,
                                 "benchmark_seconds": benchmark_seconds, "state": state}
                write(f"{case['name']}-runs", runs)
            if runs["baseline"]["state"] != runs["candidate"]["state"]:
                raise RuntimeError(f"determinism mismatch: {case['name']}")
            results.append({"case": case, "runs": runs, "state_matches": True})
            print(f"{case['name']}: baseline={runs['baseline']['wall_seconds']:.6f}s "
                  f"candidate={runs['candidate']['wall_seconds']:.6f}s state=MATCH "
                  "(timing informational)", flush=True)
        except (RuntimeError, ValueError, OSError, KeyError, StopIteration) as exc:
            error = str(exc)
            write("failure", {"case": case["name"], "variant": variant, "error": error})
            break
        finally:
            write("summary", results)
    complete = len(results) == len(matrix) and error is None
    verdict = {"base_sha": args.base_sha, "head_sha": args.head_sha,
               "pass": complete, "complete_shard": complete,
               "complete_matrix": complete and args.shard_count == 1,
               "completed_cases": len(results), "expected_cases": len(matrix),
               "measurement_method": METHOD, "performance_enforced": False}
    if error is not None:
        verdict["error"] = error
        print(f"FAIL: {error}", file=sys.stderr, flush=True)
    write("verdict", verdict)
    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
