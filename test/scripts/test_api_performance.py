# SPDX-License-Identifier: MPL-2.0
"""One run per variant, advisory timing, strict state checks and bounded failures."""

from contextlib import redirect_stdout, redirect_stderr
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import check_api_performance as measure


class PerformanceMeasurement(unittest.TestCase):
    def run_matrix(self, ratio=1.0, mismatch=False, shard=(0, 1), cpus="0,2", workers=2):
        matrix = [{"name": f"case-{i}", "kind": "floor", "cycles": 100 + i} for i in range(3)]
        calls = []
        clock = [0.0]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            builds = [root / name for name in ("baseline", "candidate")]
            for build in builds:
                binary = build / "benchmark" / measure.executable(matrix[0])
                binary.parent.mkdir(parents=True)
                binary.write_bytes(b"identical synthetic binary")
                (build / "CMakeCache.txt").write_text("same settings")
            output = root / "results"
            argv = ["check_api_performance.py", *map(str, builds), str(output),
                    "--base-sha", "base", "--head-sha", "head", "--cpus", cpus,
                    "--workers", str(workers), "--shard-index", str(shard[0]),
                    "--shard-count", str(shard[1])]

            def run(argv, **kwargs):
                variant = "candidate" if Path(argv[3]).is_relative_to(builds[1]) else "baseline"
                index = int(argv[-1]) - 100
                calls.append((index, variant))
                elapsed = 1 / ratio if variant == "candidate" else 1.0
                clock[0] += elapsed + 0.2  # Startup/shutdown outside the benchmark's timer.
                state = 43 if mismatch and variant == "candidate" else 42
                return subprocess.CompletedProcess(argv, 0, stdout=f"cycles,wall_s,digest\n{100+index},{elapsed},{state}\n")

            with (patch.object(measure.sys, "argv", argv),
                  patch.object(measure, "cases", return_value=matrix),
                  patch.object(measure.os, "sched_getaffinity", return_value=set(map(int, cpus.split(",")))),
                  patch.object(measure.subprocess, "check_output", return_value="test topology"),
                  patch.object(measure.subprocess, "run", side_effect=run),
                  patch.object(measure.time, "monotonic", side_effect=lambda: clock[0]),
                  redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO())):
                status = measure.main()
            artifacts = {p.name: json.loads(p.read_text()) for p in output.glob("*.json")}
        return status, calls, artifacts

    def test_slowdown_never_fails_or_adds_runs_even_for_identical_binaries(self):
        for ratio in (1.2, 1.0, 0.94, 0.5):
            with self.subTest(ratio=ratio):
                status, calls, artifacts = self.run_matrix(ratio=ratio)
                self.assertEqual(status, 0)
                self.assertEqual(sorted(calls), [(i, v) for i in range(3) for v in ("baseline", "candidate")])
                self.assertTrue(artifacts["verdict.json"]["complete_matrix"])
                self.assertFalse(artifacts["metadata.json"]["performance_enforced"])
                self.assertEqual(artifacts["metadata.json"]["runs_per_variant"], 1)
                for row in artifacts["summary.json"]:
                    self.assertNotIn("confidence_level", row)
                    run = row["runs"]["candidate"]
                    self.assertAlmostEqual(run["wall_seconds"], 1 / ratio + 0.2)
                    self.assertAlmostEqual(run["benchmark_seconds"], 1 / ratio)

    def test_state_mismatch_fails_and_retains_both_runs(self):
        status, calls, artifacts = self.run_matrix(mismatch=True)
        self.assertEqual(status, 1)
        self.assertEqual(len(calls), 2)
        self.assertFalse(artifacts["verdict.json"]["complete_matrix"])
        self.assertIn("determinism mismatch", artifacts["failure.json"]["error"])
        self.assertEqual(set(artifacts["case-0-runs.json"]), {"baseline", "candidate"})

    def test_shards_only_measure_their_assigned_case(self):
        status, calls, artifacts = self.run_matrix(shard=(1, 3))
        self.assertEqual(status, 0)
        self.assertEqual(sorted(calls), [(1, "baseline"), (1, "candidate")])
        self.assertTrue(artifacts["verdict.json"]["complete_shard"])
        self.assertFalse(artifacts["verdict.json"]["complete_matrix"])

    def test_worker_coverage_uses_exactly_two_or_four_cpus(self):
        for workers, cpus in ((2, "0,2"), (4, "0,2,4,6")):
            status, _, artifacts = self.run_matrix(workers=workers, cpus=cpus)
            self.assertEqual(status, 0)
            self.assertEqual(len(artifacts["metadata.json"]["cpus"]), workers)
        with self.assertRaises(SystemExit):
            self.run_matrix(workers=4, cpus="0,2")

    def test_matrix_keeps_29_cases_and_bounded_fixed_work(self):
        matrix = measure.cases(2)
        self.assertEqual(len(matrix), 29)
        self.assertEqual(len({c["name"] for c in matrix}), 29)
        self.assertEqual(sum(c["kind"] == "scheduler" and c["args"][5] for c in matrix), 6)
        self.assertTrue(all(0 < c["cycles"] <= 1_000_000_000 for c in matrix))

    def test_sequential_cases_stay_on_one_cpu(self):
        for case in measure.cases(2):
            sequential = case["kind"] == "floor" or "threads1" in case["name"]
            self.assertEqual(measure.command(Path("build"), case, [0, 2])[2],
                             "0" if sequential else "0,2")

    def test_physical_cpu_selection_excludes_smt_siblings(self):
        topology = "# CPU,CORE,SOCKET\n" + "\n".join(f"{i},{i//2},0" for i in range(16))
        with (patch.object(measure.os, "sched_getaffinity", return_value=set(range(1, 16))),
              patch.object(measure.subprocess, "check_output", return_value=topology)):
            self.assertEqual(measure.physical_cpus(limit=None), [1, 2, 4, 6, 8, 10, 12, 14])

    def test_timeout_retains_partial_output(self):
        for output in (b"partial\n", "partial\n", None):
            with self.subTest(output=output), tempfile.TemporaryDirectory() as root:
                log = Path(root) / "run.log"
                with patch.object(measure.subprocess, "run", side_effect=subprocess.TimeoutExpired([], 1, output=output)):
                    with self.assertRaisesRegex(RuntimeError, "timed out"):
                        measure.run_benchmark([], {}, log, 1)
                self.assertEqual(log.read_text(), "partial\n" if output else "")

    def test_crash_fails_even_when_timing_is_advisory(self):
        with tempfile.TemporaryDirectory() as root:
            log = Path(root) / "run.log"
            with patch.object(measure.subprocess, "run", return_value=subprocess.CompletedProcess([], 7, "crash")):
                with self.assertRaisesRegex(RuntimeError, "exit 7"):
                    measure.run_benchmark([], {}, log, 1)
            self.assertEqual(log.read_text(), "crash")

    def test_deadline_caps_each_process(self):
        with patch.object(measure.time, "monotonic", return_value=100):
            self.assertEqual(measure.remaining_timeout(110, 120), 10)
            with self.assertRaisesRegex(RuntimeError, "wall-time budget exhausted"):
                measure.remaining_timeout(99, 120)

    def test_invalid_output_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "overflow"):
            measure.parse({"kind": "scheduler"}, "overflow\n1\n")
        with self.assertRaises(ValueError):
            measure.parse({"kind": "representative"}, "RESULT median_seconds=1 mode=seq\n")
        for seconds in ("nan", "inf", "0", "-1"):
            with self.assertRaisesRegex(ValueError, "elapsed"):
                measure.parse({"kind": "floor"}, f"cycles,wall_s,digest\n1,{seconds},42\n")


if __name__ == "__main__":
    unittest.main()
