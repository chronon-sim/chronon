#!/usr/bin/env python3
"""Fixed-work, interleaved multi-clock/observation measurements with result checks."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess


def execute(binary, arguments, cpus):
    command = [str(binary), *map(str, arguments)]
    if cpus:
        command = ["taskset", "-c", cpus, *command]
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f"benchmark failed ({result.returncode}): {command}\n{result.stderr}")
    rows = list(csv.DictReader(io.StringIO(result.stdout)))
    if len(rows) != 1:
        raise ValueError(f"unexpected benchmark output: {result.stdout}")
    return rows[0]


def host_topology(cpus):
    def capture(command):
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.strip() if result.returncode == 0 else result.stderr.strip()
    rows = capture(["lscpu", "-p=CPU,CORE,SOCKET,NODE"])
    allowed = set(os.sched_getaffinity(0))
    selected = set()
    for part in cpus.split(","):
        if not part:
            continue
        bounds = list(map(int, part.split("-")))
        selected.update(range(bounds[0], bounds[-1] + 1))
    if not selected or not selected <= allowed:
        raise ValueError("--cpus must select CPUs in this process's permitted affinity")
    processors = []
    for row in rows.splitlines():
        if row.startswith("#"):
            continue
        cpu, core, socket, node = row.split(",")
        if int(cpu) in selected:
            frequency = Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/cpuinfo_max_freq")
            processors.append(dict(cpu=int(cpu), core=int(core), socket=int(socket), node=node,
                                   max_khz=frequency.read_text().strip() if frequency.exists() else None))
    return dict(lscpu=capture(["lscpu", "--json"]), selected_processors=processors,
                allowed_cpus=sorted(allowed), selected_logical_cpus=len(selected),
                selected_physical_cores=len({(p["socket"], p["core"]) for p in processors}),
                selected_numa_nodes=sorted({p["node"] for p in processors}),
                affinity="process mask inherited by workers and recorder; no per-thread pinning",
                compiler=capture(["c++", "--version"]),
                git_revision=capture(["git", "rev-parse", "HEAD"]),
                git_status=capture(["git", "status", "--short"]))


def default_cpus():
    # Prefer distinct cores. On hybrid hosts disclose their frequency/topology;
    # lscpu's global threads-per-core is not true of every selected core.
    chosen, cores = [], set()
    for cpu in sorted(os.sched_getaffinity(0)):
        root = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        key = tuple((root / name).read_text().strip() for name in ("physical_package_id", "core_id"))
        if key not in cores:
            cores.add(key)
            chosen.append(cpu)
        if len(chosen) == 8:
            break
    return ",".join(map(str, chosen))


def scaling(args):
    """Same hardware work, independent serial oracle, interleaved host variants."""
    scenarios = [(f"p{pairs}-d{domains}-w{work}-s{skew}", pairs, domains, work, skew, [])
                 for pairs, domains, work, skew in
                 ((4, 2, 0, 1), (32, 2, 0, 1), (32, 8, 0, 1), (32, 32, 0, 1),
                  (4, 2, 4000, 1), (16, 8, 4000, 1), (16, 8, 4000, 16))]
    if args.extended:
        scenarios += [
            ("shared-8", 4, 2, 0, 1, ["--lanes", 8]),
            ("shared-coincident", 4, 2, 0, 1, ["--lanes", 8, "--clocks", "coincident"]),
            ("sparse-64", 64, 64, 0, 1, ["--activity", 16]),
            ("fanin-coprime", 16, 8, 0, 1, ["--topology", "fanin", "--clocks", "coprime"]),
            ("feedback", 8, 8, 8, 1, ["--topology", "ring", "--activity", 4]),
            ("segmented-shared", 4, 4, 0, 1, ["--lanes", 8, "--segment", 137]),
        ]
    if args.scenarios:
        selected = set(args.scenarios.split(","))
        scenarios = [s for s in scenarios if s[0] in selected]
        if {s[0] for s in scenarios} != selected:
            raise ValueError("unknown --scenarios name (extended scenarios need --extended)")
    variants = [("candidate", args.binary)]
    if args.baseline_binary:
        variants.insert(0, ("baseline", args.baseline_binary))
    topology = host_topology(args.cpus)
    threads = sorted(set(map(int, args.threads.split(",")))) if args.threads else [
        t for t in (1, 2, 4, 8) if t <= max(1, topology["selected_physical_cores"])]
    if not threads or threads[0] != 1 or any(t < 1 or t > 64 for t in threads):
        raise ValueError("--threads must include 1 and contain values in [1,64]")
    modes = args.trace_modes.split(",")
    if any(m not in ("off", "text", "perfetto", "both") for m in modes):
        raise ValueError("unknown --trace-modes")
    jobs = [(scenario, variant, binary, count, dynamic, mode)
            for scenario in scenarios for variant, binary in variants for count in threads
            for dynamic in ([0] if count == 1 else [0, 1]) for mode in modes]
    metadata = dict(platform=platform.platform(), cpus=args.cpus, topology=topology, seed=9141326,
                    steps=args.steps, repetitions=args.repetitions, baseline_revision=args.baseline_revision,
                    threads=threads, trace_modes=modes, profile=args.profile, lossy=args.lossy,
                    worker_capacity=[dict(workers=t,
                        exceeds_selected_cores=t > topology["selected_physical_cores"],
                        exceeds_selected_logical_cpus=t > topology["selected_logical_cpus"],
                        workers_plus_recorder_exceeds_cores=t + (m != "off") > topology["selected_physical_cores"],
                        trace=m) for t in threads for m in modes],
                    binaries={v: dict(path=str(binary.resolve()), sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                                     cmake_cache=(binary.parent.parent / "CMakeCache.txt").read_text()
                                     if (binary.parent.parent / "CMakeCache.txt").exists() else None)
                              for v, binary in variants},
                    method="fresh processes; shuffled scenarios/variants; no warmup or speculative ticks; all variants checked against serial state; min/median/max; no fsync",
                    diagnostics="one sweep in 64, worker-staggered; sampled actor_ns includes useful tick/bridge time; instrumentation perturbs timing; separate throughput runs",
                    allocation_scope="wrapped scalar/array C++ new only; excludes aligned new, malloc and shared-library internals")
    (args.output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rng = random.Random(9141326)
    rows, expected, serial_states = [], {}, {}
    with (args.output_dir / "runs.jsonl").open("w") as raw:
        for repetition in range(args.repetitions):
            rng.shuffle(jobs)
            for (name, pairs, domains, work, skew, options), variant, binary, count, dynamic, mode in jobs:
                arguments = ["scaling", args.steps, count, pairs, domains, work, dynamic, skew, *options]
                if mode != "off":
                    arguments += ["--trace", mode, "--output", args.output_dir / f"trace-{name}-{variant}-t{count}-d{dynamic}-{mode}-{repetition}",
                                  "--lossy", int(args.lossy), "--trace-capacity", args.trace_capacity]
                if args.profile:
                    arguments += ["--profile", 1]
                row = execute(binary, arguments, args.cpus)
                # FIFO/lane state is checked where both binaries expose it, with
                # backward compatibility for the original scaling executable.
                keys = ["steps", "unit_ticks", "sent", "received", "checksum", "work_digest"]
                state = tuple(row[key] for key in keys)
                if expected.setdefault(name, state) != state or int(row["overflow"]):
                    raise AssertionError(f"hardware results differ: {name} {variant} T{count} dynamic={dynamic}")
                if "fifo_digest" in row:
                    detailed = (row["fifo_digest"], row["lane_digest"])
                    if expected.setdefault((name, "fifo"), detailed) != detailed:
                        raise AssertionError(f"FIFO/lane state differs: {name}")
                if count == 1:
                    serial_states[name] = state
                if int(row["parallel"]) != (count > 1):
                    raise AssertionError("benchmark silently fell back from requested parallel mode")
                if not args.lossy and int(row.get("dropped", 0)):
                    raise AssertionError("lossless recording dropped events")
                row.update(scenario=name, variant=variant, repetition=repetition, trace=mode)
                rows.append(row)
                raw.write(json.dumps(dict(command=[str(binary), *map(str, arguments)], affinity=args.cpus, result=row)) + "\n")
                raw.flush()
            print(f"scaling repetition {repetition + 1}/{args.repetitions} passed", flush=True)
    assert set(serial_states) == {s[0] for s in scenarios}
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with (args.output_dir / "raw.csv").open("w") as file:
        writer = csv.DictWriter(file, columns)
        writer.writeheader()
        writer.writerows(rows)
    summary = []
    for scenario, variant, _, count, dynamic, mode in sorted(jobs, key=lambda j: (j[0][0], j[1], j[3], j[4], j[5])):
        name = scenario[0]
        selected = [r for r in rows if (r["scenario"], r["variant"], int(r["threads"]), int(r["dynamic"]), r["trace"]) ==
                    (name, variant, count, dynamic, mode)]
        wall = [float(r["run_s"]) for r in selected]
        median = statistics.median(wall)
        serial = statistics.median(float(r["run_s"]) for r in rows
                                   if r["scenario"] == name and r["variant"] == variant and int(r["threads"]) == 1 and r["trace"] == mode)
        item = dict(scenario=name, variant=variant, threads=count, dynamic=dynamic, trace=mode,
                    run_s=median, run_min_s=min(wall), run_max_s=max(wall), serial_speedup=serial / median,
                    unit_ticks_per_s=int(selected[0]["unit_ticks"]) / median)
        for field in ("init_s", "wall_s", "total_s", "close_s", "init_cpu_s", "run_cpu_s", "total_cpu_s",
                      "cpu_s", "migrations", "init_allocations", "run_allocations", "partition_ns", "events", "dropped",
                      "producer_stalls", "producer_stall_ns", "admission_retries", "progress_stalls", "progress_stall_ns"):
            if field in selected[0]:
                item[field] = statistics.median(float(r[field]) for r in selected)
        for field in ("maxrss_kib", "allocated_ingress_bytes", "peak_ingress_bytes", "allocated_staging_bytes",
                      "peak_staging_records", "native_buffer_peak_bytes", "file_bytes"):
            if field in selected[0]:
                item[field] = max(int(r[field]) for r in selected)
        for field in selected[0]:
            if field.startswith("sample_"):
                item[field] = statistics.median(int(r[field]) for r in selected)
        if "events" in item:
            item["events_per_s"] = item["events"] / (median + item["close_s"])
        if args.baseline_binary:
            baseline = statistics.median(float(r["run_s"]) for r in rows
                                         if (r["scenario"], r["variant"], int(r["threads"]), int(r["dynamic"]), r["trace"]) ==
                                         (name, "baseline", count, dynamic, mode))
            item["baseline_speedup"] = baseline / median
        summary.append(item)
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    for item in summary:
        print(json.dumps(item))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--steps", type=int)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--cpus", default=default_cpus())
    parser.add_argument("--baseline-single", type=Path)
    parser.add_argument("--candidate-single", type=Path)
    parser.add_argument("--baseline-revision", default="unspecified")
    parser.add_argument("--scaling", action="store_true", help="measure serial/static/dynamic scheduling across graph sizes and costs")
    parser.add_argument("--baseline-binary", type=Path, help="interleave a prior scaling-capable executable")
    parser.add_argument("--threads", help="comma-separated workers; default 1,2,4,8 up to selected physical cores")
    parser.add_argument("--extended", action="store_true", help="add shared lanes, sparse domains, fan-in, feedback and segmentation")
    parser.add_argument("--scenarios", help="optional comma-separated scaling scenario names")
    parser.add_argument("--trace-modes", default="off")
    parser.add_argument("--lossy", action="store_true")
    parser.add_argument("--trace-capacity", type=int, default=4096)
    parser.add_argument("--profile", action="store_true", help="separate diagnostic run with sparse component timing")
    args = parser.parse_args()
    if args.steps is None:
        args.steps = 20_000 if args.scaling else 500_000
    if args.repetitions < 3 or args.repetitions > 30 or args.steps < 1000:
        parser.error("require 3..30 repetitions and at least 1000 steps")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    if args.scaling:
        scaling(args)
        return
    modes = ["off", "text", "perfetto", "both", "single", "calendar"]
    rng = random.Random(9141326)
    rows, singles = [], []
    expected = None
    for repetition in range(args.repetitions):
        order = modes.copy()
        rng.shuffle(order)
        for mode in order:
            output = args.output_dir / f"trace-{mode}-{repetition}"
            row = execute(args.binary, [mode, args.steps, output], args.cpus)
            row["repetition"] = repetition
            if mode in ("off", "text", "perfetto", "both"):
                state = tuple(row[key] for key in ("sent", "received", "checksum", "unit_ticks"))
                if expected is None:
                    expected = state
                if state != expected or int(row["dropped"]) != 0:
                    raise AssertionError("observation changed hardware results or lost lossless events")
            rows.append(row)
        if args.baseline_single and args.candidate_single:
            variants = [("baseline", args.baseline_single), ("candidate", args.candidate_single)]
            rng.shuffle(variants)
            for variant, binary in variants:
                row = execute(binary, [100_000_000], args.cpus)
                row.update(variant=variant, repetition=repetition)
                singles.append(row)
    with (args.output_dir / "raw.csv").open("w") as file:
        writer = csv.DictWriter(file, list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    summary = []
    off_wall = statistics.median(float(r["wall_s"]) for r in rows if r["mode"] == "off")
    for mode in modes:
        selected = [row for row in rows if row["mode"] == mode]
        wall = statistics.median(float(row["wall_s"]) for row in selected)
        event_count = int(selected[0]["events"])
        item = dict(mode=mode, wall_s=wall,
                    run_s=statistics.median(float(r["run_s"]) for r in selected),
                    close_s=statistics.median(float(r["wall_s"]) - float(r["run_s"]) for r in selected),
                    wall_min_s=min(float(r["wall_s"]) for r in selected),
                    wall_max_s=max(float(r["wall_s"]) for r in selected),
                    events=event_count, events_per_s=event_count / wall,
                    total_ns_per_event=wall * 1e9 / event_count if event_count else 0,
                    incremental_ns_per_event=(wall - off_wall) * 1e9 / event_count if event_count else 0,
                    ns_per_unit_tick=wall * 1e9 / int(selected[0]["unit_ticks"]),
                    allocated_ingress_bytes=int(selected[0]["allocated_ingress_bytes"]),
                    peak_ingress_bytes=max(int(r["peak_ingress_bytes"]) for r in selected),
                    file_bytes_median=statistics.median(int(r["file_bytes"]) for r in selected),
                    maxrss_kib=max(int(r["maxrss_kib"]) for r in selected))
        for field in ("native_buffer_peak_bytes", "native_buffer_peak_records", "temporary_disk_bytes"):
            if field in selected[0]:
                item[field] = max(int(r[field]) for r in selected)
        if "first_output_s" in selected[0]:
            item["first_output_s"] = statistics.median(float(r["first_output_s"]) for r in selected)
        summary.append(item)
        print(json.dumps(item))
    with (args.output_dir / "summary.json").open("w") as file:
        json.dump(summary, file, indent=2)
    if singles:
        if len({row["digest"] for row in singles}) != 1:
            raise AssertionError("single-clock before/after cycle results differ")
        with (args.output_dir / "single-regression.csv").open("w") as file:
            writer = csv.DictWriter(file, list(singles[0]))
            writer.writeheader(); writer.writerows(singles)
        times = {variant: statistics.median(float(r["wall_s"]) for r in singles if r["variant"] == variant)
                 for variant in ("baseline", "candidate")}
        print(json.dumps(dict(single_clock=times, ratio=times["candidate"] / times["baseline"])))
    metadata = dict(platform=platform.platform(), cpus=args.cpus, seed=9141326,
                    steps=args.steps, repetitions=args.repetitions,
                    binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                    baseline_revision=args.baseline_revision,
                    method="fresh processes, interleaved modes, steady_clock wall time includes final recorder drain; no fsync",
                    ingress_peak="sum of per-stream high-water marks; excludes encoder buffers and dictionaries; RSS also reported")
    (args.output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
