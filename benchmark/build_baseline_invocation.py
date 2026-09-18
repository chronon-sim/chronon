#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Link the unchanged public-API invocation probe against a baseline Make build."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build", type=Path, help="baseline Unix Makefiles build with benchmarks enabled")
    parser.add_argument("--count-allocations", action="store_true")
    args = parser.parse_args()
    build = args.build.resolve()
    source = Path(__file__).resolve().with_name("scheduler_invocation_benchmark.cpp")
    entries = json.loads((build / "compile_commands.json").read_text())
    entry = next(e for e in entries if e["file"].endswith("/multiclock_benchmark.cpp"))
    command = entry.get("arguments") or shlex.split(entry["command"])
    name = "scheduler_invocation_allocations" if args.count_allocations else "scheduler_invocation_benchmark"
    obj = build / "benchmark" / (name + ".o")
    if not args.count_allocations:
        command = [a for a in command if not a.startswith("-DCHRONON_COUNT_CLOCK_ALLOCATIONS")]
    command[command.index("-o") + 1] = str(obj)
    command[command.index(entry["file"])] = str(source)
    # No CHRONON_BENCH_SCRATCH: baseline headers do not have the private memory
    # measurement seam. Runtime/model code and allocation wrappers are identical.
    subprocess.run(command, cwd=entry["directory"], check=True)
    link = shlex.split(
        (build / "benchmark/CMakeFiles/chronon_multiclock_benchmark.dir/link.txt").read_text()
    )
    link = [str(obj) if a.endswith("multiclock_benchmark.cpp.o") else a for a in link]
    if not args.count_allocations:
        link = [a for a in link if a not in ("-Wl,--wrap=_Znwm", "-Wl,--wrap=_Znam")]
    link[link.index("-o") + 1] = "chronon_" + name
    subprocess.run(link, cwd=build / "benchmark", check=True)


if __name__ == "__main__":
    main()
