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
    result = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    rows = list(csv.DictReader(io.StringIO(result.stdout)))
    if len(rows) != 1:
        raise ValueError(f"unexpected benchmark output: {result.stdout}")
    return rows[0]


def scaling(args):
    """Same hardware work, independent serial oracle, interleaved host variants."""
    scenarios = [(f"p{pairs}-d{domains}-w{work}-s{skew}", pairs, domains, work, skew)
                 for pairs, domains, work, skew in
                 ((4, 2, 0, 1), (32, 2, 0, 1), (32, 8, 0, 1), (32, 32, 0, 1),
                  (4, 2, 4000, 1), (16, 8, 4000, 1), (16, 8, 4000, 16))]
    variants = [("candidate", args.binary)]
    if args.baseline_binary:
        variants.insert(0, ("baseline", args.baseline_binary))
    threads = sorted(set(map(int, args.threads.split(","))))
    if not threads or threads[0] != 1 or any(t < 1 or t > 64 for t in threads):
        raise ValueError("--threads must include 1 and contain values in [1,64]")
    jobs = [(scenario, variant, binary, count, dynamic)
            for scenario in scenarios for variant, binary in variants for count in threads
            for dynamic in ([0] if count == 1 else [0, 1])]
    rng = random.Random(9141326)
    rows, expected = [], {}
    for repetition in range(args.repetitions):
        rng.shuffle(jobs)
        for (name, pairs, domains, work, skew), variant, binary, count, dynamic in jobs:
            row = execute(binary, ["scaling", args.steps, count, pairs, domains, work, dynamic, skew], args.cpus)
            state = tuple(row[key] for key in
                          ("steps", "unit_ticks", "sent", "received", "checksum", "work_digest"))
            if expected.setdefault(name, state) != state or int(row["overflow"]):
                raise AssertionError(f"hardware results differ: {name} {variant} T{count} dynamic={dynamic}")
            if int(row["parallel"]) != (count > 1):
                raise AssertionError("benchmark silently fell back from requested parallel mode")
            row.update(scenario=name, variant=variant, repetition=repetition)
            rows.append(row)
        print(f"scaling repetition {repetition + 1}/{args.repetitions} passed", flush=True)
    with (args.output_dir / "raw.csv").open("w") as file:
        writer = csv.DictWriter(file, list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = []
    for scenario, variant, _, count, dynamic in sorted(jobs, key=lambda j: (j[0][0], j[1], j[3], j[4])):
        name = scenario[0]
        selected = [r for r in rows if (r["scenario"], r["variant"], int(r["threads"]), int(r["dynamic"])) ==
                    (name, variant, count, dynamic)]
        wall = [float(r["run_s"]) for r in selected]
        median = statistics.median(wall)
        serial = statistics.median(float(r["run_s"]) for r in rows
                                   if r["scenario"] == name and r["variant"] == variant and int(r["threads"]) == 1)
        item = dict(scenario=name, variant=variant, threads=count, dynamic=dynamic,
                    run_s=median, run_min_s=min(wall), run_max_s=max(wall), serial_speedup=serial / median,
                    init_s=statistics.median(float(r["init_s"]) for r in selected),
                    cpu_s=statistics.median(float(r["cpu_s"]) for r in selected),
                    unit_ticks_per_s=int(selected[0]["unit_ticks"]) / median,
                    migrations_median=statistics.median(int(r["migrations"]) for r in selected),
                    maxrss_kib=max(int(r["maxrss_kib"]) for r in selected))
        if args.baseline_binary:
            baseline = statistics.median(float(r["run_s"]) for r in rows
                                         if (r["scenario"], r["variant"], int(r["threads"]), int(r["dynamic"])) ==
                                         (name, "baseline", count, dynamic))
            item["baseline_speedup"] = baseline / median
        summary.append(item)
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    metadata = dict(platform=platform.platform(), cpus=args.cpus, seed=9141326, steps=args.steps,
                    repetitions=args.repetitions, baseline_revision=args.baseline_revision,
                    binaries={v: dict(path=str(b.resolve()), sha256=hashlib.sha256(b.read_bytes()).hexdigest())
                              for v, b in variants},
                    method="fresh processes, shuffled scenarios/variants, fixed hardware work; initialization separate; serial digest checked")
    (args.output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    for item in summary:
        print(json.dumps(item))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--steps", type=int)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--cpus", default=",".join(map(str, sorted(os.sched_getaffinity(0))[:2])))
    parser.add_argument("--baseline-single", type=Path)
    parser.add_argument("--candidate-single", type=Path)
    parser.add_argument("--baseline-revision", default="unspecified")
    parser.add_argument("--scaling", action="store_true", help="measure serial/static/dynamic scheduling across graph sizes and costs")
    parser.add_argument("--baseline-binary", type=Path, help="interleave a prior scaling-capable executable")
    parser.add_argument("--threads", default="1,2,4")
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
