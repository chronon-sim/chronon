#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Compare identical scheduler workloads in interleaved fresh processes."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import random
import statistics
import subprocess

STATE_FIELDS = ("predicates", "parallel", "ticks", "sent", "received", "checksum", "digest", "overflow")
METRICS = ("run_s", "init_s", "run_allocations", "init_allocations", "worker_scratch_bytes",
           "rss_kib", "run_cpu_s", "voluntary_switches", "involuntary_switches")


def matrix():
    cases = []
    for clock in (0, 1):
        for dynamic in (0, 1):
            for interval in (0, 1, 64, 4096):
                cases.append((clock, 4, 4, 64, 1, dynamic, interval, 20000))
        for interval in (0, 1):
            cases += [(clock, 1, 4, 64, 1, 0, interval, 20000),
                      (clock, 4, 2, 0, 1, 0, interval, 20000),
                      (clock, 4, 4, 512, 8, 1, interval, 20000),
                      (clock, 8, 32, 64, 4, 1, interval, 10000),
                      (clock, 4, 64, 0, 1, 0, interval, 10000)]
    return [(*c[:-1], 6000) if c[0] == 0 and c[6] == 1 and (c[3] >= 512 or c[1] >= 8)
            else c for c in cases]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="baseline build directory")
    parser.add_argument("candidate", type=Path, help="candidate build directory")
    parser.add_argument("output", type=Path, help="new measurement directory")
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--scale", type=int, default=1, help="multiply work duration, retaining predicate cadence")
    parser.add_argument("--long-focus", action="store_true", help="one-call and interval-64 small/light cases")
    parser.add_argument("--allocations", action="store_true", help="counting executables; timings are diagnostic only")
    parser.add_argument("--pin-workers", action="store_true")
    parser.add_argument("--warm-cpus", action="store_true")
    parser.add_argument("--cpus", default="0,2,4,6,8,10,12,14", help="ordered distinct physical CPUs")
    parser.add_argument("--seed", type=int, default=143)
    parser.add_argument("--cases", type=Path, help="optional JSON list of eight-integer case arrays")
    args = parser.parse_args()
    cpus = args.cpus.split(",")
    if args.repeats < 1 or args.scale < 1:
        parser.error("positive repeats and scale are required")
    if args.warm_cpus and not args.pin_workers:
        parser.error("--warm-cpus requires --pin-workers")
    cases = [tuple(c) for c in json.loads(args.cases.read_text())] if args.cases else matrix()
    if not cases or any(len(c) != 8 or any(not isinstance(x, int) for x in c) for c in cases):
        parser.error("cases must be nonempty eight-integer arrays")
    if args.long_focus:
        cases = [c for c in cases if c[6] in (0, 64) and c[2] in (2, 4, 64) and c[3] <= 64]
    cases = [(*c[:-1], c[-1] * args.scale) for c in cases]
    if len(cpus) < max(c[1] for c in cases) or len(set(cpus)) != len(cpus):
        parser.error("choose enough distinct physical CPUs for the largest worker count")
    name = "chronon_scheduler_invocation_" + ("allocations" if args.allocations else "benchmark")
    binaries = {v: (getattr(args, v).resolve() / "benchmark" / name) for v in ("baseline", "candidate")}
    for binary in binaries.values():
        if not binary.is_file():
            parser.error(f"missing executable: {binary}")
    env = os.environ.copy()
    for variable, enabled in (("CHRONON_BENCH_PIN_WORKERS", args.pin_workers),
                              ("CHRONON_BENCH_WARM_CPUS", args.warm_cpus)):
        env.pop(variable, None)
        if enabled:
            env[variable] = "1"
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {"arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "binaries": {v: {"path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                             for v, p in binaries.items()}, "cases": cases,
                "measurement_mode": "allocation-only" if args.allocations else "uninstrumented timing"}
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rng = random.Random(args.seed)
    jobs = [(case, rep) for case in cases for rep in range(args.repeats)]
    rng.shuffle(jobs)
    rows = []
    for case, rep in jobs:
        order = list(binaries)
        rng.shuffle(order)
        pair = {}
        for variant in order:
            mask = ",".join(cpus[:max(4, case[1])])
            command = ["taskset", "-c", mask, str(binaries[variant]), *map(str, case)]
            output = subprocess.check_output(command, env=env, text=True, timeout=180)
            result = next(csv.DictReader(io.StringIO("\n".join(output.strip().splitlines()[-2:]))))
            if result["overflow"] != "0":
                raise RuntimeError(f"transport overflow: {command}")
            row = {"case": case, "rep": rep, "variant": variant, **result}
            rows.append(row)
            pair[variant] = row
        if any(pair["baseline"][k] != pair["candidate"][k] for k in STATE_FIELDS):
            raise RuntimeError(f"state/predicate mismatch: {case}, repetition {rep}: {pair}")
        (args.output / "runs.json").write_text(json.dumps(rows, indent=2) + "\n")
        if rep == 0:
            print(case, {v: row["run_s"] for v, row in pair.items()}, flush=True)
    summary = []
    for case in cases:
        samples = {v: [r for r in rows if r["case"] == case and r["variant"] == v] for v in binaries}
        item = {"case": case}
        for key in METRICS:
            item[key] = {v: {"median": statistics.median(float(r[key]) for r in group),
                             "min": min(float(r[key]) for r in group),
                             "max": max(float(r[key]) for r in group)} for v, group in samples.items()}
        if not args.allocations:
            item["speedup"] = item["run_s"]["baseline"]["median"] / item["run_s"]["candidate"]["median"]
        summary.append(item)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("completed", len(rows), "validated runs", flush=True)


if __name__ == "__main__":
    main()
