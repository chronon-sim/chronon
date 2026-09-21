#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Run the unchanged performance gate on disjoint physical CPUs on one host.

Each scenario is a separate evidence shard. As soon as a CPU group becomes free,
it takes the next scenario, so a second sampling batch cannot hold up a static
shard's remaining cases. Baseline and candidate always share the same CPU group
and run sequentially. Builds and correctness tests must finish before this runs.
"""

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
from collect_api_performance import collect


def cpu_groups(cpus, workers, jobs):
    """Input contains one allowed logical CPU per physical core, never siblings."""
    if workers < 2 or jobs < 1 or len(cpus) < workers or len(set(cpus)) != len(cpus):
        raise ValueError("not enough distinct physical CPUs for measurement workers")
    count = min(jobs, len(cpus) // workers)
    return [cpus[i * workers:(i + 1) * workers] for i in range(count)]


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


def run_tasks(count, groups, command, timeout, accepted=(0,)):
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
                if status not in accepted:
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
    parser.add_argument("--timeout", type=float, default=4200,
                        help="total wall-time budget including identity checks, in seconds")
    parser.add_argument("--force-measurement", action="store_true")
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

    write("execution", {"cpu_groups": groups, "concurrent_scenarios": len(groups),
                        "workers": args.workers, "timeout_seconds": args.timeout,
                        "case_order": [case["name"] for case in matrix]})
    write("verdict", verdict)
    print(f"Using {len(groups)} isolated CPU groups: {groups}", flush=True)

    def command(index, cpus, count=1):
        return [sys.executable, str(Path(__file__).with_name("check_api_performance.py")),
                str(args.baseline.resolve()), str(args.candidate.resolve()),
                str(root / f"api-performance-{index}"),
                "--base-sha", args.base_sha, "--head-sha", args.head_sha,
                "--workers", str(args.workers), "--cpus", ",".join(map(str, cpus)),
                "--shard-index", str(index), "--shard-count", str(count),
                *(["--force-measurement"] if args.force_measurement else [])]

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"received signal {signum}")

    handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        # Preserve the cheap whole-matrix identity path before scheduling timing.
        status = run_tasks(1, groups[:1],
                           lambda index, cpus: command(index, cpus) + ["--defer-measurement"],
                           deadline - time.monotonic(), accepted=(0, 3))[0]
        count = 1
        if status == 3:
            count = len(matrix)
            run_tasks(count, groups, lambda index, cpus: command(index, cpus, count),
                      deadline - time.monotonic())
        results, metadata = collect(root, args.base_sha, args.head_sha, count, args.workers)
        # Write success only after validating every evidence shard.
        success = {**verdict, "pass": True, "complete_matrix": True, "completed_cases": len(results)}
        write("summary", results)
        write("metadata", {**success, "shards": metadata})
        write("verdict", success)
        print(f"PASS: all {len(results)} scenarios across {count} evidence shards", flush=True)
        return 0
    except (RuntimeError, ValueError, OSError, KeyError, KeyboardInterrupt) as error:
        verdict["error"] = str(error)
        write("verdict", verdict)
        print(f"FAIL: {error}; partial evidence retained in {output}", file=sys.stderr, flush=True)
        return 1
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    sys.exit(main())
