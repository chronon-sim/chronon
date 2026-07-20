#!/usr/bin/env python3
# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# SPDX-License-Identifier: MPL-2.0

"""Run Chronon's representative throughput and memory-footprint matrix.

Each worker/repetition pair runs in a separate process. This makes peak RSS a
property of one benchmark configuration instead of the maximum of a worker
sweep, while the driver still interleaves worker counts to reduce ordering bias.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import platform
import random
import re
import resource
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from collections import defaultdict


MAX_WORKERS = 256
MAX_UNITS = 4096
MAX_TOTAL_WORKING_SET_BYTES = 256 * 1024 * 1024
KNOWN_PROFILES = {
    "nucleus",
    "scheduler",
    "scheduler-floor",
    "memory",
    "port",
    "hotspot",
    "broadcast",
    "backpressure",
    "saturation",
    "random",
}
RESULT_REQUIRED = {
    "workers",
    "units",
    "cycles",
    "median_seconds",
    "mcycles_per_second",
    "mcycles_units_per_second",
    "mode",
    "modeled_working_set_bytes",
    "estimated_port_storage_bytes",
}


@dataclasses.dataclass(frozen=True)
class BenchmarkCase:
    name: str
    profile: str
    units: int
    working_set_bytes_per_unit: int | None = None


def comma_strings(value: str) -> list[str]:
    values = [item.strip() for item in value.split(",") if item.strip()]
    if not values:
        raise argparse.ArgumentTypeError("list must not be empty")
    return values


def parse_size(value: str) -> int:
    match = re.fullmatch(r"\s*(\d+)\s*([kmgt]?)(?:i?b)?\s*", value, re.IGNORECASE)
    if match is None:
        raise argparse.ArgumentTypeError(f"invalid byte size: {value}")
    number = int(match.group(1))
    shift = {"": 0, "k": 10, "m": 20, "g": 30, "t": 40}[match.group(2).lower()]
    return number << shift


def comma_positive_ints(value: str) -> list[int]:
    try:
        values = [int(item.strip()) for item in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("values must be positive")
    return values


def parse_result(stdout: str) -> dict[str, str]:
    lines = [line for line in stdout.splitlines() if line.startswith("RESULT ")]
    if len(lines) != 1:
        raise RuntimeError(f"expected exactly one RESULT line, found {len(lines)}")
    result = dict(field.split("=", 1) for field in lines[0].split()[1:])
    missing = RESULT_REQUIRED - result.keys()
    if missing:
        raise RuntimeError(f"RESULT line is missing fields: {sorted(missing)}")
    return result


def measure_subprocess(argv: list[str]) -> int:
    """Internal fresh-process wrapper for an exact per-child rusage sample."""
    if len(argv) < 5 or argv[3] != "--":
        print("internal measurement wrapper received invalid arguments", file=sys.stderr)
        return 125
    measurement_path = pathlib.Path(argv[1])
    timeout_seconds = float(argv[2])
    command = argv[4:]
    started = time.perf_counter()
    child = subprocess.Popen(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    timed_out = False
    try:
        stdout, stderr = child.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(child.pid, signal.SIGKILL)
        stdout, stderr = child.communicate()
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    max_rss_kib = usage.ru_maxrss / 1024.0 if sys.platform == "darwin" else usage.ru_maxrss
    metrics = {
        "process_elapsed_seconds": time.perf_counter() - started,
        "user_seconds": usage.ru_utime,
        "system_seconds": usage.ru_stime,
        "max_rss_kib": max_rss_kib,
        "minor_page_faults": usage.ru_minflt,
        "major_page_faults": usage.ru_majflt,
        "voluntary_context_switches": usage.ru_nvcsw,
        "involuntary_context_switches": usage.ru_nivcsw,
        "timed_out": timed_out,
    }
    measurement_path.write_text(json.dumps(metrics) + "\n", encoding="utf-8")
    sys.stdout.write(stdout)
    sys.stderr.write(stderr)
    return 124 if timed_out else child.returncode


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = quantile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = min(len(ordered) - 1, lower + 1)
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def coefficient_of_variation(values: list[float]) -> float:
    mean = statistics.fmean(values)
    if len(values) < 2 or mean == 0.0:
        return 0.0
    return statistics.stdev(values) / mean


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def capture(command: list[str], cwd: pathlib.Path) -> str | None:
    completed = subprocess.run(
        command, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def cpu_model() -> str | None:
    cpuinfo = pathlib.Path("/proc/cpuinfo")
    if not cpuinfo.is_file():
        return None
    for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("model name") and ":" in line:
            return line.split(":", 1)[1].strip()
    return None


def cmake_build_metadata(benchmark: pathlib.Path) -> dict[str, str] | None:
    wanted = {
        "CMAKE_BUILD_TYPE",
        "CMAKE_CXX_COMPILER",
        "CMAKE_CXX_FLAGS",
        "CMAKE_CXX_FLAGS_RELEASE",
        "CHRONON_BUILD_BENCHMARKS",
        "CHRONON_ENABLE_ASAN",
        "CHRONON_ENABLE_TSAN",
    }
    for directory in benchmark.parents:
        cache = directory / "CMakeCache.txt"
        if not cache.is_file():
            continue
        result: dict[str, str] = {"build_directory": str(directory)}
        for line in cache.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("//") or line.startswith("#") or "=" not in line:
                continue
            typed_key, value = line.split("=", 1)
            key = typed_key.split(":", 1)[0]
            if key in wanted:
                result[key] = value
        return result
    return None


def make_cases(args: argparse.Namespace) -> list[BenchmarkCase]:
    cases: list[BenchmarkCase] = []
    for units in args.units:
        for profile in args.profiles:
            if profile == "memory":
                for working_set in args.memory_working_sets:
                    cases.append(
                        BenchmarkCase(
                            name=f"memory-u{units}-ws{working_set}",
                            profile=profile,
                            units=units,
                            working_set_bytes_per_unit=working_set,
                        )
                    )
            else:
                cases.append(BenchmarkCase(name=f"{profile}-u{units}", profile=profile, units=units))
    return cases


def benchmark_command(
    benchmark: pathlib.Path, case: BenchmarkCase, worker: int, args: argparse.Namespace
) -> list[str]:
    command = [
        str(benchmark),
        "--profile",
        case.profile,
        "--seed",
        str(args.seed),
        "--scenario-count",
        "1",
        "--threads",
        str(worker),
        "--units",
        str(case.units),
        "--warmup",
        str(args.warmup),
        "--repetitions",
        "1",
        "--max-lookahead",
        str(args.max_lookahead),
    ]
    if args.cycles is not None:
        command.extend(["--cycles", str(args.cycles)])
    else:
        command.extend(["--target-seconds", str(args.target_seconds)])
    if case.working_set_bytes_per_unit is not None:
        command.extend(
            [
                "--working-set",
                str(case.working_set_bytes_per_unit),
                "--working-set-sigma",
                str(args.memory_working_set_sigma),
            ]
        )
    if args.rebalance:
        command.append("--rebalance")
    if args.no_precomputed_costs:
        command.append("--no-precomputed-costs")
    if args.cpus:
        command = [args.taskset, "-c", args.cpus, *command]
    return command


def write_csv(path: pathlib.Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def print_summary_table(rows: list[dict[str, object]]) -> None:
    case_width = max(len("case"), *(len(str(row["case"])) for row in rows))
    header = (
        f"{'case':<{case_width}}  {'workers':>7}  {'mode':<10}  "
        f"{'median(s)':>9}  {'Mcycles/s':>10}  {'Mcycles*units/s':>16}  {'speedup':>8}  "
        f"{'peak RSS MiB':>12}  {'CV':>8}"
    )
    print("\n=== Chronon performance summary ===")
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            f"{str(row['case']):<{case_width}}  {int(row['workers']):>7}  "
            f"{str(row['mode']):<10}  {float(row['median_seconds']):>9.3f}  "
            f"{float(row['mcycles_per_second']):>10.3f}  "
            f"{float(row['mcycles_units_per_second']):>16.3f}  "
            f"{float(row['speedup']):>7.2f}x  {float(row['max_peak_rss_mib']):>12.2f}  "
            f"{float(row['cv_percent']):>7.2f}%"
        )


def summarize(raw_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[object, ...], list[dict[str, object]]] = defaultdict(list)
    group_fields = [
        "case",
        "profile",
        "requested_units",
        "requested_working_set_bytes_per_unit",
        "workers",
    ]
    for row in raw_rows:
        grouped[tuple(row[field] for field in group_fields)].append(row)

    summaries: list[dict[str, object]] = []
    for key, rows in grouped.items():
        seconds = [float(row["median_seconds"]) for row in rows]
        cycles = [int(row["cycles"]) for row in rows]
        mcycles = [float(row["mcycles_per_second"]) for row in rows]
        mcycles_units = [float(row["mcycles_units_per_second"]) for row in rows]
        rss_kib = [float(row["max_rss_kib"]) for row in rows]
        process_seconds = [float(row["process_elapsed_seconds"]) for row in rows]
        user_seconds = [float(row["user_seconds"]) for row in rows]
        system_seconds = [float(row["system_seconds"]) for row in rows]
        median_seconds = percentile(seconds, 0.5)
        units = int(rows[0]["units"])
        modeled_bytes = int(rows[0]["modeled_working_set_bytes"])
        port_bytes = int(rows[0]["estimated_port_storage_bytes"])
        modes = sorted({str(row["mode"]) for row in rows})
        summary = dict(zip(group_fields, key))
        summary.update(
            {
                "repetitions": len(rows),
                "mode": ",".join(modes),
                "resolved_cycles_min": min(cycles),
                "resolved_cycles_max": max(cycles),
                "units": units,
                "median_seconds": median_seconds,
                "p10_seconds": percentile(seconds, 0.1),
                "p90_seconds": percentile(seconds, 0.9),
                "cv_percent": 100.0 * coefficient_of_variation(seconds),
                "mcycles_per_second": percentile(mcycles, 0.5),
                "mcycles_units_per_second": percentile(mcycles_units, 0.5),
                "median_peak_rss_mib": percentile(rss_kib, 0.5) / 1024.0,
                "max_peak_rss_mib": max(rss_kib) / 1024.0,
                "modeled_working_set_mib": modeled_bytes / (1024.0 * 1024.0),
                "estimated_port_storage_mib": port_bytes / (1024.0 * 1024.0),
                "rss_minus_modeled_working_set_mib": (
                    max(rss_kib) / 1024.0 - modeled_bytes / (1024.0 * 1024.0)
                ),
                "median_process_elapsed_seconds": percentile(process_seconds, 0.5),
                "median_user_seconds": percentile(user_seconds, 0.5),
                "median_system_seconds": percentile(system_seconds, 0.5),
            }
        )
        summaries.append(summary)

    by_case: dict[str, list[dict[str, object]]] = defaultdict(list)
    for summary in summaries:
        by_case[str(summary["case"])].append(summary)
    for case_summaries in by_case.values():
        baseline = min(case_summaries, key=lambda row: int(row["workers"]))
        baseline_throughput = float(baseline["mcycles_per_second"])
        for summary in case_summaries:
            summary["baseline_workers"] = baseline["workers"]
            summary["speedup"] = (
                float(summary["mcycles_per_second"]) / baseline_throughput
            )

    return sorted(summaries, key=lambda row: (str(row["case"]), int(row["workers"])))


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    unknown = set(args.profiles) - KNOWN_PROFILES
    if unknown:
        parser.error(f"unknown profiles: {sorted(unknown)}")
    if any(worker > MAX_WORKERS for worker in args.workers):
        parser.error(f"worker count exceeds {MAX_WORKERS}")
    if any(units > MAX_UNITS for units in args.units):
        parser.error(f"unit count exceeds {MAX_UNITS}")
    if any(working_set < 64 for working_set in args.memory_working_sets):
        parser.error("memory working sets must be at least one 64-byte cache line")
    if not 0 <= args.memory_working_set_sigma <= 2000:
        parser.error("memory working-set sigma must be in [0, 2000]")
    if args.memory_working_set_sigma == 0:
        for units in args.units:
            for working_set in args.memory_working_sets:
                if units * working_set > MAX_TOTAL_WORKING_SET_BYTES:
                    parser.error(
                        f"memory case {units} units x {working_set} bytes exceeds 256 MiB cap"
                    )
    if args.repetitions <= 0 or args.warmup < 0:
        parser.error("repetitions must be positive and warmup must be non-negative")
    if args.cycles is not None and args.cycles <= 0:
        parser.error("cycles must be positive")
    if args.target_seconds is not None and not 0 < args.target_seconds <= 3600.0:
        parser.error("target seconds must be in (0, 3600]")
    if args.seed < 0 or args.max_lookahead < 0 or args.timeout_seconds <= 0:
        parser.error("seed/lookahead must be non-negative and timeout must be positive")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a reproducible Chronon worker/footprint throughput matrix."
    )
    parser.add_argument(
        "--benchmark",
        default="build-release/benchmark/chronon_representative_workload_benchmark",
        help="representative benchmark executable",
    )
    parser.add_argument(
        "--profiles",
        type=comma_strings,
        default=comma_strings("nucleus,scheduler-floor,memory,port"),
        help="profile list; memory is expanded over --memory-working-sets",
    )
    parser.add_argument("--workers", type=comma_positive_ints, default=[1, 2, 4, 8])
    parser.add_argument("--units", type=comma_positive_ints, default=[64])
    parser.add_argument(
        "--memory-working-sets",
        type=lambda value: [parse_size(item) for item in value.split(",")],
        default=[64 * 1024, 1024 * 1024, 4 * 1024 * 1024],
        metavar="SIZES",
        help="per-unit memory profile sizes, e.g. 64K,1M,4M",
    )
    parser.add_argument("--memory-working-set-sigma", type=int, default=0)
    duration = parser.add_mutually_exclusive_group()
    duration.add_argument(
        "--target-seconds",
        type=float,
        default=1.0,
        help="auto-calibrate every worker run to about this duration (default 1.0)",
    )
    duration.add_argument("--cycles", type=int, help="fixed measured cycles; disables targeting")
    parser.add_argument("--warmup", type=int, default=8192)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--seed", type=int, default=0x4348524F4E4F4E)
    parser.add_argument("--max-lookahead", type=int, default=32)
    parser.add_argument("--rebalance", action="store_true")
    parser.add_argument("--no-precomputed-costs", action="store_true")
    parser.add_argument("--cpus", help="taskset CPU list, e.g. 16-31")
    parser.add_argument("--taskset", default=shutil.which("taskset") or "taskset")
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    parser.add_argument("--output-dir", help="default: out/performance-<UTC>-<git-sha>")
    parser.add_argument("--dry-run", action="store_true", help="print the matrix without running")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="use 2,000 cycles, 256 warmup cycles, and one repetition",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.cycles is not None:
        args.target_seconds = None
    if args.quick:
        args.cycles = 2_000
        args.target_seconds = None
        args.warmup = 256
        args.repetitions = 1
    args.workers = sorted(set(args.workers))
    args.units = sorted(set(args.units))
    validate_args(parser, args)

    repo = pathlib.Path(__file__).resolve().parent.parent
    benchmark = pathlib.Path(args.benchmark)
    if not benchmark.is_absolute():
        benchmark = repo / benchmark
    benchmark = benchmark.resolve()
    cases = make_cases(args)

    if args.dry_run:
        for case in cases:
            for repetition in range(args.repetitions):
                workers = list(args.workers)
                shuffle_seed = (
                    args.seed ^ repetition ^ int.from_bytes(case.name.encode(), "little")
                )
                random.Random(shuffle_seed).shuffle(workers)
                for worker in workers:
                    print(shlex.join(benchmark_command(benchmark, case, worker, args)))
        return 0

    if not benchmark.is_file() or not os.access(benchmark, os.X_OK):
        parser.error(
            f"benchmark executable not found: {benchmark}\n"
            "build it with: cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release "
            "-DCHRONON_BUILD_BENCHMARKS=ON && cmake --build build-release "
            "--target chronon_representative_workload_benchmark"
        )
    if args.cpus and not shutil.which(args.taskset):
        parser.error(f"taskset executable not found: {args.taskset}")

    commit = capture(["git", "rev-parse", "HEAD"], repo) or "unknown"
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        pathlib.Path(args.output_dir)
        if args.output_dir
        else repo / "out" / f"performance-{timestamp}-{commit[:12]}"
    ).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        parser.error(f"refusing to overwrite non-empty output directory: {output_dir}")
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "schema_version": 1,
        "created_utc": timestamp,
        "repository": str(repo),
        "git_commit": commit,
        "git_status_porcelain": capture(["git", "status", "--porcelain"], repo),
        "benchmark": str(benchmark),
        "benchmark_sha256": sha256(benchmark),
        "build": cmake_build_metadata(benchmark),
        "invocation": sys.argv,
        "configuration": {
            "profiles": args.profiles,
            "workers": args.workers,
            "units": args.units,
            "memory_working_sets": args.memory_working_sets,
            "memory_working_set_sigma": args.memory_working_set_sigma,
            "cycles": args.cycles,
            "target_seconds": args.target_seconds,
            "warmup": args.warmup,
            "repetitions": args.repetitions,
            "seed": args.seed,
            "max_lookahead": args.max_lookahead,
            "rebalance": args.rebalance,
            "precomputed_costs": not args.no_precomputed_costs,
            "cpus": args.cpus,
            "timeout_seconds": args.timeout_seconds,
        },
        "cases": [dataclasses.asdict(case) for case in cases],
        "host": {
            "platform": platform.platform(),
            "uname": platform.uname()._asdict(),
            "python": platform.python_version(),
            "cpu_model": cpu_model(),
            "logical_cpus": os.cpu_count(),
            "driver_affinity": (
                sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
            ),
        },
        "memory_measurement": (
            "fresh wrapper process resource.getrusage(RUSAGE_CHILDREN).ru_maxrss "
            "(normalized to KiB)"
        ),
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    raw_rows: list[dict[str, object]] = []
    total_runs = len(cases) * len(args.workers) * args.repetitions
    completed_runs = 0
    for case in cases:
        for repetition in range(args.repetitions):
            workers = list(args.workers)
            shuffle_seed = args.seed ^ repetition ^ int.from_bytes(case.name.encode(), "little")
            random.Random(shuffle_seed).shuffle(workers)
            for worker in workers:
                completed_runs += 1
                run_id = f"{case.name}-w{worker}-r{repetition}"
                command = benchmark_command(benchmark, case, worker, args)
                print(f"[{completed_runs}/{total_runs}] {run_id}", file=sys.stderr, flush=True)
                with tempfile.NamedTemporaryFile(
                    prefix=f"{run_id}-", suffix=".json", dir=output_dir, delete=False
                ) as measurement_file:
                    measurement_path = pathlib.Path(measurement_file.name)
                measured_command = [
                    sys.executable,
                    str(pathlib.Path(__file__).resolve()),
                    "--_measure",
                    str(measurement_path),
                    str(args.timeout_seconds),
                    "--",
                    *command,
                ]
                started = time.perf_counter()
                try:
                    completed = subprocess.run(
                        measured_command,
                        cwd=repo,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=args.timeout_seconds + 10.0,
                    )
                except subprocess.TimeoutExpired as error:
                    (logs_dir / f"{run_id}.log").write_text(
                        f"command: {shlex.join(measured_command)}\n"
                        f"WRAPPER TIMEOUT after {args.timeout_seconds + 10.0}s\n"
                        f"stdout:\n{error.stdout or ''}\nstderr:\n{error.stderr or ''}\n",
                        encoding="utf-8",
                    )
                    raise RuntimeError(f"measurement wrapper timed out: {run_id}") from error
                driver_elapsed = time.perf_counter() - started
                (logs_dir / f"{run_id}.log").write_text(
                    f"command: {shlex.join(command)}\nreturncode: {completed.returncode}\n"
                    f"driver_elapsed_seconds: {driver_elapsed}\n\nstdout:\n{completed.stdout}\n"
                    f"stderr:\n{completed.stderr}\n",
                    encoding="utf-8",
                )
                if completed.returncode != 0:
                    log_path = logs_dir / f"{run_id}.log"
                    raise RuntimeError(
                        f"benchmark failed ({completed.returncode}): {run_id}; see {log_path}"
                    )
                result = parse_result(completed.stdout)
                time_metrics = json.loads(measurement_path.read_text(encoding="utf-8"))
                measurement_path.unlink()
                if int(result["workers"]) != worker or int(result["units"]) != case.units:
                    raise RuntimeError(f"benchmark RESULT dimensions do not match {run_id}")
                row: dict[str, object] = {
                    "case": case.name,
                    "profile": case.profile,
                    "requested_units": case.units,
                    "requested_working_set_bytes_per_unit": (
                        case.working_set_bytes_per_unit
                        if case.working_set_bytes_per_unit is not None
                        else ""
                    ),
                    "repetition": repetition,
                    "driver_elapsed_seconds": driver_elapsed,
                    "command": shlex.join(command),
                    **result,
                    **time_metrics,
                }
                raw_rows.append(row)
                write_csv(output_dir / "raw.csv", raw_rows, list(raw_rows[0].keys()))

    summary_rows = summarize(raw_rows)
    write_csv(output_dir / "summary.csv", summary_rows, list(summary_rows[0].keys()))
    print_summary_table(summary_rows)
    print(f"raw results: {output_dir / 'raw.csv'}")
    print(f"summary: {output_dir / 'summary.csv'}")
    print(f"metadata: {output_dir / 'metadata.json'}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--_measure":
        raise SystemExit(measure_subprocess(sys.argv[1:]))
    raise SystemExit(main())
