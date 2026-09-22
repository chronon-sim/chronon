# SPDX-License-Identifier: MPL-2.0
"""Benchmark provenance must identify the executables actually run by each case."""

from contextlib import chdir, redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "observation_benchmark",
    Path(__file__).resolve().parents[2] / "scripts/benchmark_observation_service.py")
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class ExecutableProvenance(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.launcher = self.root / "launcher"
        self.launcher.mkdir()

    def binary(self, relative, marker):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        log = (f"{marker}\nSYSCON poweroff\nExit code: 0\nCycles executed: 7\n"
               "Wall time: 1 ms\nScheduler service: enabled")
        path.write_text(f"#!{sys.executable}\nprint({log!r})\n")
        path.chmod(0o755)
        return path

    def run_manifest(self, cases, binaries, search_path=os.defpath):
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps({"cases": cases}))
        output = self.root / "results"
        argv = ["benchmark_observation_service.py", str(manifest), "--output", str(output),
                "--cpus", "0", "--warmups", "0", "--repetitions", "1"]
        popen = subprocess.Popen

        def launch(command, **kwargs):
            # Exercise real executable lookup and child cwd without measuring
            # CPU affinity or depending on taskset/lscpu on the test machine.
            self.assertEqual(command[:3], ["taskset", "-c", "0"])
            return popen(command[3:], **kwargs)

        with (chdir(self.launcher), patch.object(sys, "argv", argv),
              patch.dict(os.environ, {"PATH": search_path}),
              patch.object(runner.subprocess, "check_output", return_value="test CPU"),
              patch.object(runner.subprocess, "Popen", side_effect=launch),
              redirect_stdout(io.StringIO())):
            runner.main()
        metadata = json.loads((output / "metadata.json").read_text())
        expected = {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path, _ in binaries.values()}
        self.assertEqual(metadata["binary_sha256"], expected)
        rows = json.loads((output / "runs.json").read_text())
        self.assertEqual(len(rows), len(cases) * 2)
        for row in rows:
            _, marker = binaries[row["case"], row["mode"]]
            self.assertIn(marker, (Path(row["directory"]) / "output.log").read_text())

    def test_relative_commands_are_distinct_across_cases_and_modes(self):
        cases, binaries = [], {}
        for name in ("case one", "case two"):
            cases.append(dict(name=name, cwd=f"../{name}",
                              thread_command=["./bin/thread"], service_command=["bin/service"]))
            for mode in ("thread", "service"):
                marker = f"{name} {mode}"
                binaries[name, mode] = (self.binary(f"{name}/bin/{mode}", marker), marker)
        self.run_manifest(cases, binaries)

    def test_absolute_command(self):
        binary = self.binary("case/runner", "absolute executable")
        cases = [dict(name="absolute", cwd=str(binary.parent),
                      thread_command=[str(binary)], service_command=[str(binary)])]
        self.run_manifest(cases, {("absolute", mode): (binary, "absolute executable")
                                  for mode in ("thread", "service")})

    def test_path_search_resolves_relative_entries_from_each_case(self):
        first = self.binary("case one/tools/runner", "relative PATH entry")
        fallback = self.binary("shared/runner", "absolute PATH fallback")
        self.binary("launcher/tools/runner", "wrong launching directory")
        (self.root / "case two").mkdir()
        cases = [dict(name=name, cwd=f"../{name}",
                      thread_command=["runner"], service_command=["runner"])
                 for name in ("case one", "case two")]
        binaries = {(name, mode): (binary, marker)
                    for name, binary, marker in (("case one", first, "relative PATH entry"),
                                                 ("case two", fallback, "absolute PATH fallback"))
                    for mode in ("thread", "service")}
        self.run_manifest(cases, binaries, f"tools{os.pathsep}{fallback.parent}")

    def test_empty_path_entry_uses_case_directory(self):
        binary = self.binary("case/runner", "case directory")
        self.binary("launcher/runner", "wrong launching directory")
        cases = [dict(name="empty-path", cwd="../case",
                      thread_command=["runner"], service_command=["runner"])]
        self.run_manifest(cases, {("empty-path", mode): (binary, "case directory")
                                  for mode in ("thread", "service")}, "")


if __name__ == "__main__":
    unittest.main()
