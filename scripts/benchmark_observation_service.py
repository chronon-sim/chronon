#!/usr/bin/env python3
"""Serial, equal-affinity A/B runs of one binary with observation service off/on.

The manifest contains cases with name, cwd, thread_command, service_command,
and optionally scenario (a SAGE YAML template), workers, and expected_crc.
Command arguments may contain {run_dir} and {scenario}. All outputs and exact
commands are retained. Compile and run correctness tests BEFORE this script.
Requires PyYAML only for SAGE scenario templates.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import statistics
import subprocess
import threading
import time


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run_case(case, mode, repetition, output, cpus):
    directory = output / f"{case['name']}-{mode}-{repetition}"
    directory.mkdir()
    scenario = ""
    if case.get("scenario"):
        import yaml
        config = yaml.safe_load(Path(case["scenario"]).read_text())
        config["output_dir"] = str(directory / "model")
        config["threads"] = case["workers"]
        scenario = str(directory / "scenario.yaml")
        Path(scenario).write_text(yaml.safe_dump(config, sort_keys=False))
    command = [part.replace("{run_dir}", str(directory)).replace("{scenario}", scenario)
               for part in case[f"{mode}_command"]]
    command = ["taskset", "-c", cpus, *command]
    environment = dict(os.environ)
    for key in list(environment):
        if key.startswith(("CHRONON_EXPERIMENTAL_", "NUCLEUS_EXPERIMENTAL_")):
            del environment[key]
    (directory / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    begin = time.perf_counter()
    with (directory / "output.log").open("w") as stream:
        # wait(timeout=...) polls in Python and quantizes short wall times. A
        # watchdog keeps the deadline while wait() blocks on the actual exit.
        process = subprocess.Popen(command, cwd=case["cwd"], env=environment,
                                   stdout=stream, stderr=subprocess.STDOUT)
        watchdog = threading.Timer(300, process.kill)
        watchdog.start()
        try:
            process.wait()
        finally:
            watchdog.cancel()
    elapsed = time.perf_counter() - begin
    content = (directory / "output.log").read_text()
    if process.returncode:
        raise RuntimeError(f"run failed ({process.returncode}): {directory}")
    row = dict(case=case["name"], mode=mode, repetition=repetition,
               warmup=repetition < 0, wall_s=elapsed, directory=str(directory))
    if case.get("scenario"):
        metrics = json.loads((directory / "model/metrics.json").read_text())
        if metrics["message"] != "completed":
            raise RuntimeError(f"SAGE did not complete: {directory}")
        policy = metrics["target"]["clock_domains"]["execution_policy"]
        if policy not in ("parallel_clock_scheduler", "epoch_free_lookahead"):
            raise RuntimeError(f"SAGE was not parallel ({policy}): {directory}")
        row["cycles"] = metrics["cycles"]
        row["scheduler"] = policy
        for stats_file in directory.rglob("clock-stats.json"):
            stats = json.loads(stats_file.read_text())
            if stats["dropped_events"] or not stats["allocated_staging_bytes"]:
                raise RuntimeError(f"lossy or nonparallel clock capture: {stats_file}")
            if mode == "service" and not stats["service_records"]:
                raise RuntimeError(f"service did not drain: {stats_file}")
    else:
        if "SYSCON poweroff" not in content or not re.search(r"Exit code:\s*0\b", content):
            raise RuntimeError(f"Nucleus did not complete successfully: {directory}")
        if case.get("expected_crc") and case["expected_crc"] not in content:
            raise RuntimeError(f"CoreMark CRC mismatch: {directory}")
        if mode == "service" and "Scheduler service:" not in content:
            raise RuntimeError(f"service mode was not reported: {directory}")
        row["cycles"] = int(re.search(r"Cycles executed:\s*(\d+)", content)[1])
        row["simulation_wall_s"] = int(re.search(r"Wall time:\s*(\d+) ms", content)[1]) / 1000
    print(f"{case['name']} {mode} {repetition}: {elapsed:.6f}s cycles={row['cycles']}", flush=True)
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpus", default="0,2,4,6,8,10")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--only", nargs="*")
    args = parser.parse_args()
    if args.repetitions < 1 or args.warmups < 0:
        parser.error("invalid repetition count")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(args.manifest.read_text())
    cases = [case for case in manifest["cases"] if not args.only or case["name"] in args.only]
    if not cases:
        parser.error("no matching cases")
    metadata = dict(manifest=manifest, cpus=args.cpus,
                    platform=subprocess.check_output(["lscpu"], text=True),
                    binary_sha256={case["thread_command"][0]: digest(case["thread_command"][0])
                                   for case in cases}, started=time.time())
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rng = random.Random(20260920)
    rows = []
    for case in cases:
        for repetition in range(-args.warmups, args.repetitions):
            modes = ["thread", "service"]
            rng.shuffle(modes)
            for mode in modes:
                rows.append(run_case(case, mode, repetition, output, args.cpus))
                (output / "runs.json").write_text(json.dumps(rows, indent=2) + "\n")
    summary = []
    for case in cases:
        result = {"case": case["name"]}
        for mode in ("thread", "service"):
            times = [row["wall_s"] for row in rows
                     if row["case"] == case["name"] and row["mode"] == mode and not row["warmup"]]
            result[mode] = dict(median_s=statistics.median(times), minimum_s=min(times), maximum_s=max(times))
        result["speedup"] = result["thread"]["median_s"] / result["service"]["median_s"]
        result["wall_reduction_pct"] = 100 * (1 - 1 / result["speedup"])
        summary.append(result)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
