#!/usr/bin/env python3
"""Bounded, sequential PMU/profile diagnosis of three Chronon revisions.

Uses all six balanced permutations for stat (18 runs), then one profile per revision.
All runs use broadcast/seed149/16units/2workers/512warmup/2500000measured cycles.
Requires identical affinity-enabled representative harnesses in all revisions.
Counters/profiles cover the whole process, including the 150ms CPU warmup;
RESULT median_seconds covers only the simulation's measured region.
No product sources, scheduler settings, model state, or machine controls change.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import time

BINARY = "chronon_representative_workload_benchmark"
VARIANTS = ("baseline", "original", "candidate")
ORDERS = tuple(itertools.permutations(VARIANTS))
EXPECTED_RUNS = 21
EVENTS = ("task-clock", "context-switches", "cpu-migrations", "cycles",
          "instructions", "branches", "branch-misses")
CYCLES = 2_500_000


def resolve_binary(path: Path) -> Path:
    path = path.resolve()
    if path.is_dir():
        path = path / "benchmark" / BINARY
    if not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError(f"benchmark executable missing: {path}")
    return path


def benchmark_result(output: str) -> dict:
    lines = output.splitlines()
    machine = [line for line in lines if line.startswith("RESULT ")]
    digests = [line.strip() for line in lines if line.strip().startswith("digest=")]
    if len(machine) != 1 or len(digests) != 1:
        raise ValueError("expected one RESULT and one digest")
    result = dict(field.split("=", 1) for field in machine[0].split()[1:])
    for field, expected in (("workers", "2"), ("units", "16"), ("cycles", str(CYCLES)),
                            ("mode", "epoch-free")):
        if result.get(field) != expected:
            raise ValueError(f"unexpected {field}: {result.get(field)}")
    seconds = float(result["median_seconds"])
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("invalid measured simulation time")
    state = dict(field.split("=", 1) for field in digests[0].split())
    state["mode"] = result["mode"]
    return {"simulation_seconds": seconds, "state": state}


def stat_counters(path: Path) -> dict:
    result = {}
    for row in csv.reader(path.read_text().splitlines(), delimiter=";"):
        if len(row) < 5 or row[0].startswith("#") or row[2] not in EVENTS:
            continue
        value, running = float(row[0].strip()), float(row[4].strip().rstrip("%"))
        if not math.isfinite(value) or value < 0 or not 0 < running <= 100:
            raise ValueError(f"invalid counter row: {row}")
        if running < 99.999:
            raise ValueError(f"counter not running 100% (multiplexed): {row}")
        result[row[2]] = {"value": value, "unit": row[1], "running_percent": running}
    if set(result) != set(EVENTS):
        raise ValueError(f"missing/unsupported PMU counters: {set(EVENTS) - set(result)}")
    if not result["cycles"]["value"] or not result["instructions"]["value"]:
        raise ValueError("hardware cycles/instructions were not counted")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    for variant in VARIANTS:
        parser.add_argument(variant, type=Path, help="build directory or exact executable")
    parser.add_argument("output", type=Path)
    parser.add_argument("--perf", default=os.environ.get("PERF_BINARY", "perf"))
    parser.add_argument("--cpus", default="0,1", help="two allowed CPU IDs, same for every run")
    parser.add_argument("--command-timeout", type=float, default=90)
    parser.add_argument("--budget-seconds", type=float, default=600)
    args = parser.parse_args()
    if any(not math.isfinite(v) or v <= 0
           for v in (args.command_timeout, args.budget_seconds)):
        parser.error("timeouts must be finite and positive")
    try:
        cpus = list(map(int, args.cpus.split(",")))
        if len(cpus) != 2 or len(set(cpus)) != 2 or not set(cpus) <= os.sched_getaffinity(0):
            raise ValueError("two distinct allowed CPUs are required")
        binaries = {v: resolve_binary(getattr(args, v)) for v in VARIANTS}
    except (ValueError, OSError) as error:
        parser.error(str(error))
    args.output.mkdir(parents=True, exist_ok=False)
    deadline = time.monotonic() + args.budget_seconds
    env = {**os.environ, "LC_ALL": "C", "CHRONON_BENCH_PIN_WORKERS": "1",
           "CHRONON_BENCH_WARM_CPUS": "1"}
    # Timing the same binaries with extra placement diagnostics is a separate experiment.
    env.pop("CHRONON_BENCH_DIAG_WORKERS", None)
    rows, errors = [], []
    expected_state = None

    def write(name: str, value) -> None:
        (args.output / name).write_text(json.dumps(value, indent=2) + "\n")

    def run(argv: list[str], log: Path) -> dict:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("total diagnostic budget exhausted")
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        started = time.monotonic()
        with log.open("w") as stream:
            proc = subprocess.Popen(argv, env=env, stdout=stream, stderr=subprocess.STDOUT,
                                    start_new_session=True)
            try:
                code = proc.wait(timeout=min(args.command_timeout, remaining))
            except BaseException:
                try:
                    os.killpg(proc.pid, 9)
                except ProcessLookupError:
                    pass
                proc.wait()
                raise
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        return {"command": argv, "exit_code": code,
                "process_wall_seconds": time.monotonic() - started,
                "user_seconds": after.ru_utime - before.ru_utime,
                "system_seconds": after.ru_stime - before.ru_stime,
                "voluntary_switches": after.ru_nvcsw - before.ru_nvcsw,
                "involuntary_switches": after.ru_nivcsw - before.ru_nivcsw}

    metadata = {"method": "six balanced stat permutations, one profile per revision, no concurrent runs",
                "stat_orders": ORDERS, "record_order": VARIANTS, "cpus": cpus, "cycles": CYCLES,
                "controls": {k: env[k] for k in ("CHRONON_BENCH_PIN_WORKERS",
                                                 "CHRONON_BENCH_WARM_CPUS")},
                "whole_process_profile": True, "events": EVENTS, "perf": args.perf,
                "budget_seconds": args.budget_seconds, "binaries": {}}
    try:
        for variant, binary in binaries.items():
            saved = args.output / "binaries" / variant
            saved.mkdir(parents=True)
            shutil.copy2(binary, saved / BINARY)
            info = {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            cache = binary.parent.parent / "CMakeCache.txt"
            if cache.exists():
                shutil.copy2(cache, saved / "CMakeCache.txt")
            metadata["binaries"][variant] = info
            for name, command in (("symbols.txt", ["nm", "-SC", "--defined-only", str(binary)]),
                                  ("build-id.txt", ["readelf", "-n", str(binary)]),
                                  ("ldd.txt", ["ldd", str(binary)])):
                info[name] = run(command, saved / name)["exit_code"]
        for name, command in (("lscpu.txt", ["lscpu"]), ("kernel.txt", ["uname", "-a"]),
                              ("perf-version.txt", [args.perf, "--version"]),
                              ("perf-list.txt", [args.perf, "list", "--raw-dump"])):
            metadata[name] = run(command, args.output / name)["exit_code"]
        for name in ("perf_event_paranoid", "kptr_restrict"):
            metadata[name] = Path("/proc/sys/kernel", name).read_text().strip()
        write("metadata.json", metadata)
        for phase in ("stat", "record"):
            for block, order in enumerate(ORDERS if phase == "stat" else (VARIANTS,)):
                for variant in order:
                    stem = args.output / f"{phase}-{block}-{variant}"
                    bench = ["taskset", "-c", ",".join(map(str, cpus)), str(binaries[variant]),
                             "--profile", "broadcast", "--seed", "149", "--units", "16",
                             "--threads", "2", "--warmup", "512", "--cycles", str(CYCLES),
                             "--repetitions", "1"]
                    if phase == "stat":
                        argv = [args.perf, "stat", "--no-big-num", "-x", ";", "-e",
                                ",".join(EVENTS), "-o", str(stem) + ".stat", "--", *bench]
                    else:
                        argv = [args.perf, "record", "-o", str(stem) + ".data", "-F", "499",
                                "-e", "cpu-clock", "--call-graph", "dwarf,8192", "--", *bench]
                    row = {"phase": phase, "block": block, "variant": variant, "order": order}
                    rows.append(row)
                    try:
                        log = Path(str(stem) + ".log")
                        row.update(run(argv, log))
                        if row["exit_code"]:
                            raise RuntimeError(f"{phase} failed: {log}")
                        row.update(benchmark_result(log.read_text()))
                        if expected_state is None:
                            expected_state = row["state"]
                        elif row["state"] != expected_state:
                            raise ValueError(f"model state mismatch: {log}")
                        if phase == "stat":
                            row["counters"] = stat_counters(Path(str(stem) + ".stat"))
                            counts = row["counters"]
                            row["ipc"] = counts["instructions"]["value"] / counts["cycles"]["value"]
                        else:
                            row["reports"] = {}
                            for sort in ("dso,symbol", "comm,pid,cpu,symbol"):
                                report = Path(str(stem) + "." + sort.replace(",", "-") + ".txt")
                                command = [args.perf, "report", "-i", str(stem) + ".data", "--stdio",
                                           "--no-children", "--percent-limit", "0.5", "--sort", sort]
                                row["reports"][sort] = run(command, report)["exit_code"]
                            # perf report 6.8 has no 'tid' sort key; perf script exposes actual TIDs.
                            command = [args.perf, "script", "-i", str(stem) + ".data", "-F",
                                       "comm,pid,tid,cpu,time,event,ip,sym,dso"]
                            row["reports"]["thread-samples"] = run(
                                command, Path(str(stem) + ".samples.txt"))["exit_code"]
                            if any(row["reports"].values()):
                                raise RuntimeError(f"one or more profile reports failed: {stem}")
                        print(phase, block, variant, "state=MATCH", flush=True)
                    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                        row["error"] = str(error)
                        errors.append(str(error))
                        print("ERROR", error, flush=True)
                    finally:
                        write("runs.json", rows)
    except (OSError, RuntimeError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        errors.append(str(error))
    finally:
        write("metadata.json", metadata)
        summary = {}
        for phase in ("stat", "record"):
            summary[phase] = {}
            for variant in VARIANTS:
                valid = [r for r in rows if r["phase"] == phase and r["variant"] == variant
                         and "error" not in r and "simulation_seconds" in r]
                summary[phase][variant] = {"valid_runs": len(valid),
                    "simulation_seconds": [r["simulation_seconds"] for r in valid],
                    "median_simulation_seconds": statistics.median(
                        r["simulation_seconds"] for r in valid) if valid else None}
        complete = len(rows) == EXPECTED_RUNS and not errors
        write("summary.json", summary)
        write("verdict.json", {"pass": complete, "attempted_runs": len(rows),
                                "completed_runs": sum("simulation_seconds" in r and "error" not in r
                                                      for r in rows),
                                "expected_runs": EXPECTED_RUNS, "errors": errors,
                                "state": expected_state})
    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
