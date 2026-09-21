#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Measure all scenarios once on disjoint CPU groups; timing is informational."""

import argparse
from contextlib import suppress
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from check_api_performance import cases, physical_cpus
from collect_api_performance import collect, render_report


def cpu_groups(cpus, workers, jobs):
    """Input contains one allowed logical CPU per physical core, never siblings."""
    # The caller shares a worker CPU, allowing eight two-core tasks on 16 cores.
    width = workers
    if workers < 2 or jobs < 1 or len(cpus) < width or len(set(cpus)) != len(cpus):
        raise ValueError("not enough distinct physical CPUs for workers")
    count = min(jobs, len(cpus) // width)
    return [cpus[i * width:(i + 1) * width] for i in range(count)]


def stop_processes(processes):
    # Every checker owns a session containing its taskset/benchmark children.
    # Killing only the Python checker would leave the measurement burning CPUs.
    for process in processes:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
    deadline = time.monotonic() + 3
    for process in processes:
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=max(0, deadline - time.monotonic()))
    for process in processes:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def run_tasks(count, groups, command, timeout):
    """Dynamically fill CPU slots; any failure/timeout stops every process tree."""
    active, results = {}, {}
    pending = iter(range(count))
    exhausted = False
    deadline = time.monotonic() + timeout
    try:
        while active or not exhausted:
            if time.monotonic() >= deadline:
                raise RuntimeError("performance measurement wall-time budget exhausted")
            # Observe every completion before starting new work after a failure.
            for slot, (index, process) in list(active.items()):
                status = process.poll()
                if status is None:
                    continue
                if status != 0:
                    raise RuntimeError(f"measurement task {index} failed with exit {status}")
                results[index] = status
                del active[slot]
            for slot, cpus in enumerate(groups):
                if slot in active or exhausted:
                    continue
                index = next(pending, None)
                if index is None:
                    exhausted = True
                    break
                print(f"Starting task {index + 1}/{count} on CPUs {cpus}", flush=True)
                argv = ["taskset", "-c", ",".join(map(str, cpus)), *command(index, cpus)]
                active[slot] = (index, subprocess.Popen(argv, start_new_session=True))
            if active:
                time.sleep(0.1)
    finally:
        stop_processes([process for _, process in active.values()])
    return [results[index] for index in range(count)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--workers", type=int, choices=(2, 4), default=2)
    parser.add_argument("--jobs", type=int, default=8,
                        help="maximum concurrent scenarios, limited by physical CPU topology")
    parser.add_argument("--timeout", type=float, default=600,
                        help="total measurement wall-time budget, in seconds")
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("timeout must be finite and positive")
    try:
        groups = cpu_groups(physical_cpus(limit=None), args.workers, args.jobs)
    except ValueError as error:
        parser.error(str(error))
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = output / "shards"
    root.mkdir()
    matrix = cases(args.workers)
    deadline = time.monotonic() + args.timeout
    verdict = {"pass": False, "complete_matrix": False, "base_sha": args.base_sha,
               "head_sha": args.head_sha, "expected_cases": len(matrix)}

    def write(name, value):
        (output / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")

    execution = {"cpu_groups": groups, "concurrent_scenarios": len(groups),
                 "workers": args.workers, "timeout_seconds": args.timeout,
                 "case_order": [case["name"] for case in matrix]}
    write("execution", execution)
    write("verdict", verdict)
    print(f"Using {len(groups)} isolated CPU groups: {groups}", flush=True)

    def command(index, cpus):
        return [sys.executable, str(Path(__file__).with_name("check_api_performance.py")),
                str(args.baseline.resolve()), str(args.candidate.resolve()),
                str(root / f"api-performance-{index}"),
                "--base-sha", args.base_sha, "--head-sha", args.head_sha,
                "--workers", str(args.workers), "--cpus", ",".join(map(str, cpus)),
                "--shard-index", str(index), "--shard-count", str(len(matrix))]

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"received signal {signum}")

    handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    results = []
    try:
        count = len(matrix)
        run_tasks(count, groups, command, deadline - time.monotonic())
        results, metadata = collect(root, args.base_sha, args.head_sha, count, args.workers)
        # Write success only after validating every evidence shard.
        success = {**verdict, "pass": True, "complete_matrix": True, "completed_cases": len(results)}
        write("summary", results)
        write("metadata", {**success, "shards": metadata})
        verdict = success
        write("verdict", verdict)
        print(f"PASS: all {len(results)} scenarios across {count} evidence shards", flush=True)
        return 0
    except (RuntimeError, ValueError, OSError, KeyError, KeyboardInterrupt) as error:
        verdict["error"] = str(error)
        write("verdict", verdict)
        print(f"FAIL: {error}; partial evidence retained in {output}", file=sys.stderr, flush=True)
        return 1
    finally:
        (output / "report.md").write_text(render_report(results, verdict, execution))
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    sys.exit(main())
