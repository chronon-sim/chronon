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
    args = parser.parse_args()
    build = args.build.resolve()
    source = Path(__file__).resolve().with_name("scheduler_invocation_benchmark.cpp")
    entries = json.loads((build / "compile_commands.json").read_text())
    entry = next(e for e in entries if e["file"].endswith("/multiclock_benchmark.cpp"))
    command = entry.get("arguments") or shlex.split(entry["command"])
    obj = build / "benchmark/scheduler_invocation.o"
    command[command.index("-o") + 1] = str(obj)
    command[command.index(entry["file"])] = str(source)
    # No CHRONON_BENCH_SCRATCH: baseline headers do not have the private memory
    # measurement seam. Runtime/model code and allocation wrappers are identical.
    subprocess.run(command, cwd=entry["directory"], check=True)
    link = shlex.split(
        (build / "benchmark/CMakeFiles/chronon_multiclock_benchmark.dir/link.txt").read_text()
    )
    link = [str(obj) if a.endswith("multiclock_benchmark.cpp.o") else a for a in link]
    link[link.index("-o") + 1] = "chronon_scheduler_invocation_benchmark"
    subprocess.run(link, cwd=build / "benchmark", check=True)


if __name__ == "__main__":
    main()
