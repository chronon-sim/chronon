#!/usr/bin/env python3
"""Run the counter benchmark matrix and emit one CSV row per configuration."""

import argparse
import csv
import pathlib
import subprocess
import sys
import tempfile


def comma_ints(value: str) -> list[int]:
    return [int(item) for item in value.split(",")]


def comma_strings(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_result(stdout: str) -> dict[str, str]:
    line = next((line for line in stdout.splitlines() if line.startswith("RESULT ")), None)
    if line is None:
        raise RuntimeError(f"benchmark did not emit RESULT: {stdout}")
    return dict(field.split("=", 1) for field in line.split()[1:])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmark", default="build-release/benchmark/chronon_counter_periodic_benchmark"
    )
    parser.add_argument(
        "--modes",
        type=comma_strings,
        default=comma_strings("updates-off,updates-only,snapshot-only,csv,perfetto,full"),
    )
    parser.add_argument("--counters", type=comma_ints, default=comma_ints("1,16,64,256"))
    parser.add_argument("--threads", type=comma_ints, default=comma_ints("1,2,4,8"))
    parser.add_argument("--periods", type=comma_ints, default=comma_ints("100,1000,10000"))
    parser.add_argument("--cycles", type=int, default=1_000_000)
    parser.add_argument("--queue-capacity", type=int, default=8 * 1024 * 1024)
    args = parser.parse_args()

    benchmark = pathlib.Path(args.benchmark).resolve()
    if not benchmark.is_file():
        parser.error(f"benchmark executable not found: {benchmark}")

    writer = None
    with tempfile.TemporaryDirectory(prefix="chronon-counter-profile-") as output_root:
        for mode in args.modes:
            periods = [0] if mode in ("updates-off", "updates-only") else args.periods
            for counters in args.counters:
                for threads in args.threads:
                    for period in periods:
                        output_dir = (
                            pathlib.Path(output_root) / f"{mode}-{counters}-{threads}-{period}"
                        )
                        command = [
                            str(benchmark),
                            "--mode",
                            mode,
                            "--counters",
                            str(counters),
                            "--threads",
                            str(threads),
                            "--cycles",
                            str(args.cycles),
                            "--period",
                            str(period),
                            "--queue-capacity",
                            str(args.queue_capacity),
                            "--output-dir",
                            str(output_dir),
                        ]
                        completed = subprocess.run(
                            command,
                            check=True,
                            text=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                        result = parse_result(completed.stdout)
                        if writer is None:
                            writer = csv.DictWriter(sys.stdout, fieldnames=list(result))
                            writer.writeheader()
                        writer.writerow(result)
                        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
