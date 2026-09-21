# SPDX-License-Identifier: MPL-2.0
"""Acceptance-gate failures must never become successful CI results."""

import importlib.util
import io
import json
import math
from pathlib import Path
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "api_performance", Path(__file__).resolve().parents[2] / "scripts/check_api_performance.py")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class PerformanceAcceptance(unittest.TestCase):
    def run_synthetic_matrix(self, speedups, selected=None, shard=None, identity=False, force=False,
                             mismatch=False, defer=False, cpus="0,2,4", workers=None):
        """Exercise CLI sampling, artifacts and exit status without real timing."""
        cases = [{"name": f"case-{i}", "kind": "floor", "cycles": 100 + i}
                 for i in range(len(speedups))]
        calls = [0] * len(cases)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            builds = [root / variant for variant in ("baseline", "candidate")]
            for build in builds:
                binary = build / "benchmark" / gate.executable(cases[0])
                binary.parent.mkdir(parents=True)
                binary.write_bytes(b"synthetic benchmark")
                (build / "CMakeCache.txt").write_text("identical synthetic settings\n")
            output = root / "results"
            argv = ["check_api_performance.py", *map(str, builds), str(output),
                    "--base-sha", "baseline", "--head-sha", "candidate", "--cpus", cpus]
            if workers is not None:
                argv += ["--workers", str(workers)]
            if shard is not None:
                argv += ["--shard-index", str(shard[0]), "--shard-count", str(shard[1])]
            if force:
                argv += ["--force-measurement"]
            if defer:
                argv += ["--defer-measurement"]
            if selected is not None:
                argv += ["--case", f"case-{selected}"]

            def run(command, **kwargs):
                index = int(command[-1]) - 100
                repetition = calls[index] // 2
                calls[index] += 1
                candidate = Path(command[3]).is_relative_to(builds[1])
                seconds = 1 / speedups[index][repetition] if candidate else 1
                digest = 43 if mismatch and candidate else 42
                text = f"cycles,wall_s,digest\n{100 + index},{seconds},{digest}\n"
                return subprocess.CompletedProcess(command, 0, stdout=text)

            with (patch.object(gate.sys, "argv", argv),
                  patch.object(gate.os, "sched_getaffinity", return_value=set(map(int, cpus.split(",")))),
                  patch.object(gate, "cases", return_value=cases),
                  patch.object(gate, "calibrate"),
                  patch.object(gate, "runtime_identity", return_value={"verified": True} if identity else None),
                  patch.object(gate.subprocess, "check_output", return_value="synthetic CPU"),
                  patch.object(gate.subprocess, "run", side_effect=run),
                  redirect_stdout(io.StringIO())):
                status = gate.main()
            artifacts = {path.name: json.loads(path.read_text()) for path in output.glob("*.json")}
        return status, calls, artifacts

    def test_coordinator_cpu_survives_cli_without_changing_worker_coverage(self):
        for workers, cpus in ((2, "0,2,4"), (4, "0,2,4,6,8")):
            with self.subTest(workers=workers):
                status, _, artifacts = self.run_synthetic_matrix(
                    [[1.0] * 2], identity=True, cpus=cpus, workers=workers)
                self.assertEqual(status, 0)
                meta = artifacts["metadata.json"]
                self.assertEqual(meta["workers"], workers)
                self.assertEqual(meta["cpus"], list(map(int, cpus.split(","))))
                case = next(c for c in gate.cases(workers)
                            if c["name"] == f"clock1-threads{workers}-dynamic1-poll1")
                command = gate.command(Path("build"), case, meta["cpus"])
                self.assertEqual(command[2], cpus)
                self.assertEqual(command[5], str(workers))

    def test_workers_cannot_share_the_coordinator_cpu(self):
        with self.assertRaises(SystemExit):
            self.run_synthetic_matrix([[1.0] * 2], identity=True, cpus="0,2,4,6", workers=4)

    def test_pair_order_is_balanced_at_both_looks_and_independent_between_cases(self):
        sequences = []
        for case in gate.cases(2):
            orders = gate.measurement_orders(case["name"], (51, 201))
            self.assertEqual(orders, gate.measurement_orders(case["name"], (51, 201)))
            self.assertEqual(len(orders), 201)
            for batch in (orders[:51], orders[51:], orders):
                self.assertLessEqual(abs(sum(pair[0] == "baseline" for pair in batch) * 2
                                         - len(batch)), 1)
                self.assertTrue(all(set(pair) == {"baseline", "candidate"} for pair in batch))
            sequences.append(tuple(orders[:51]))
        self.assertEqual(len(set(sequences)), len(sequences))

    def test_identical_runtime_checks_state_without_fake_statistics(self):
        with patch.object(gate, "confidence", side_effect=AssertionError("must not time identical code")):
            status, calls, artifacts = self.run_synthetic_matrix([[1.0] * 2], identity=True)
        self.assertEqual((status, calls), (0, [4]))
        self.assertTrue(artifacts["verdict.json"]["complete_matrix"])
        result = artifacts["summary.json"][0]
        self.assertEqual(result["sample_count"], 0)
        self.assertEqual(result["looks"], [])
        self.assertNotIn("lower_speedup", result)
        self.assertEqual(len(artifacts["case-0-identity-checks.json"]), 2)

    def test_identical_runtime_still_rejects_state_changes(self):
        with self.assertRaisesRegex(RuntimeError, "determinism mismatch"):
            self.run_synthetic_matrix([[1.0] * 2], identity=True, mismatch=True)

    def test_forced_measurement_keeps_the_original_sample_budget(self):
        status, calls, artifacts = self.run_synthetic_matrix([[1.0] * 51], identity=True, force=True)
        self.assertEqual((status, calls), (0, [102]))
        self.assertEqual(artifacts["metadata.json"]["acceptance_method"], "paired-bootstrap")

    def test_deferred_timing_produces_no_runs_or_acceptance_artifacts(self):
        for identity, force in ((False, False), (True, True)):
            with self.subTest(identity=identity, force=force):
                status, calls, artifacts = self.run_synthetic_matrix(
                    [[1.0] * 51], identity=identity, force=force, defer=True)
                self.assertEqual((status, calls, artifacts), (3, [0], {}))

    def test_prepare_checks_every_identical_case_and_rejects_state_changes(self):
        status, calls, artifacts = self.run_synthetic_matrix([[1.0] * 2] * 3, identity=True, defer=True)
        self.assertEqual((status, calls), (0, [4, 4, 4]))
        self.assertTrue(artifacts["verdict.json"]["complete_matrix"])
        with self.assertRaisesRegex(RuntimeError, "determinism mismatch"):
            self.run_synthetic_matrix([[1.0] * 2], identity=True, mismatch=True, defer=True)

    def test_shard_only_measures_assigned_cases_and_is_not_full_acceptance(self):
        status, calls, artifacts = self.run_synthetic_matrix([[1.02] * 51] * 7, shard=(2, 3))
        self.assertEqual((status, calls), (0, [0, 0, 102, 0, 0, 102, 0]))
        self.assertTrue(artifacts["verdict.json"]["complete_shard"])
        self.assertFalse(artifacts["verdict.json"]["complete_matrix"])

    def test_binary_difference_never_uses_identity(self):
        with patch.object(gate.subprocess, "check_output") as ldd:
            self.assertIsNone(gate.runtime_identity({}, {"baseline": {"exe": "a"},
                                                        "candidate": {"exe": "b"}}))
            ldd.assert_not_called()

    def test_physical_cpus_respects_affinity_and_excludes_smt_siblings(self):
        topology = "# CPU,CORE,SOCKET\n" + "\n".join(
            f"{cpu},{cpu // 2},0" for cpu in range(16))
        with (patch.object(gate.os, "sched_getaffinity", return_value=set(range(1, 16))),
              patch.object(gate.subprocess, "check_output", return_value=topology)):
            self.assertEqual(gate.physical_cpus(), [1, 2, 4, 6])
            self.assertEqual(gate.physical_cpus(limit=None), [1, 2, 4, 6, 8, 10, 12, 14])

    def test_timeout_retains_partial_output(self):
        for output in (b"partial output\n", "partial output\n", None):
            with self.subTest(output=output), tempfile.TemporaryDirectory() as temporary:
                log = Path(temporary) / "sample.log"
                error = subprocess.TimeoutExpired(["benchmark"], 0.1, output=output)
                with patch.object(gate.subprocess, "run", side_effect=error):
                    with self.assertRaisesRegex(RuntimeError, "timed out.*sample.log"):
                        gate.run_benchmark(["benchmark"], {}, log, 0.1)
                self.assertEqual(log.read_text(), "partial output\n" if output else "")

    def test_scenario_budget_caps_subprocess_timeout(self):
        with patch.object(gate.time, "monotonic", return_value=100):
            self.assertEqual(gate.remaining_timeout(300, 180), 180)
            self.assertEqual(gate.remaining_timeout(110, 180), 10)
            for deadline in (100, 99):
                with self.assertRaisesRegex(RuntimeError, "wall-time budget exhausted"):
                    gate.remaining_timeout(deadline, 180)

    def test_identity_requires_resolved_unchanged_libraries(self):
        builds = {key: Path(key) for key in ("baseline", "candidate")}
        binaries = {key: {"exe": "same"} for key in builds}
        with tempfile.TemporaryDirectory() as temporary:
            library = Path(temporary) / "library.so"
            library.write_bytes(b"library")
            output = f"linux-vdso.so.1 (0x123)\nlib => {library} (0x123)\n"
            with patch.object(gate.subprocess, "check_output", return_value=output):
                self.assertIsNotNone(gate.runtime_identity(builds, binaries))
            with patch.object(gate.subprocess, "check_output", return_value="lib => not found"):
                self.assertIsNone(gate.runtime_identity(builds, binaries))
            def changing_library(*args, **kwargs):
                if "candidate" in str(args[0][1]):
                    library.write_bytes(b"changed")
                return output
            with patch.object(gate.subprocess, "check_output", side_effect=changing_library):
                self.assertIsNone(gate.runtime_identity(builds, binaries))
            with patch.dict(gate.os.environ, {"LD_AUDIT": "hook.so"}):
                self.assertIsNone(gate.runtime_identity(builds, binaries))

    def test_final_failure_stops_and_records_incomplete_matrix(self):
        status, calls, artifacts = self.run_synthetic_matrix([[0.98] * 51, [1.1] * 51])
        self.assertEqual(status, 1)
        self.assertEqual(calls, [102, 0])
        self.assertFalse(artifacts["verdict.json"]["pass"])
        self.assertFalse(artifacts["verdict.json"]["complete_matrix"])
        self.assertEqual(len(artifacts["summary.json"]), 1)
        self.assertEqual(len(artifacts["case-0-samples.json"]), 51)

    def test_uncertainty_extends_before_stopping_at_cap(self):
        samples = [0.97] * 25 + [1.02] * 26 + [0.98] * 150
        status, calls, artifacts = self.run_synthetic_matrix([samples, [1.1] * 51])
        self.assertEqual(status, 1)
        self.assertEqual(calls, [402, 0])
        self.assertEqual(len(artifacts["case-0-samples.json"]), 201)
        self.assertEqual(len(artifacts["case-0-looks.json"]), 2)
        self.assertFalse(artifacts["verdict.json"]["complete_matrix"])

    def test_full_pass_requires_every_requested_case(self):
        status, calls, artifacts = self.run_synthetic_matrix([[1.02] * 51, [1.03] * 51])
        self.assertEqual(status, 0)
        self.assertEqual(calls, [102, 102])
        self.assertTrue(artifacts["verdict.json"]["pass"])
        self.assertTrue(artifacts["verdict.json"]["complete_matrix"])
        self.assertEqual(artifacts["verdict.json"]["completed_cases"], 2)

    def test_successful_diagnostic_is_never_a_complete_matrix(self):
        status, calls, artifacts = self.run_synthetic_matrix([[1.02] * 51, [1.03] * 51], selected=0)
        self.assertEqual(status, 0)
        self.assertEqual(calls, [102, 0])
        self.assertTrue(artifacts["verdict.json"]["pass"])
        self.assertFalse(artifacts["verdict.json"]["complete_matrix"])

    def test_calibration_rechecks_nonlinear_scale_up(self):
        case = {"name": "nonlinear", "cycles": 100}
        calls = []

        def run(variant, current, label):
            cycles = current["cycles"]
            calls.append((variant, cycles, label))
            seconds = cycles / (100 if cycles <= 100 else 1000)
            return seconds, {"cycles": cycles}

        records = gate.calibrate(case, 2.0, run)
        self.assertEqual(len(records), 3)
        self.assertTrue(records[-1]["target_reached"])
        self.assertGreaterEqual(min(records[-1]["seconds"].values()), 2.0)
        for a, b in zip(calls[::2], calls[1::2]):
            self.assertEqual(a[1:], b[1:])

    def test_calibration_records_cycle_cap(self):
        case = {"name": "capped", "cycles": gate.MAX_CYCLES - 1}
        records = gate.calibrate(case, 2.0, lambda *args: (0.1, {"digest": 1}))
        self.assertEqual(case["cycles"], gate.MAX_CYCLES)
        self.assertTrue(records[-1]["capped"])
        self.assertFalse(records[-1]["target_reached"])

    def test_calibration_preserves_failed_state_evidence(self):
        saved = []
        with self.assertRaisesRegex(RuntimeError, "determinism mismatch"):
            gate.calibrate({"name": "bad", "cycles": 1}, 2.0,
                           lambda variant, *args: (2.0, {"digest": variant}),
                           lambda records: saved.extend(records))
        self.assertFalse(saved[0]["state_matches"])

    def test_calibration_is_bounded(self):
        with self.assertRaisesRegex(RuntimeError, "target not reached"):
            gate.calibrate({"name": "never", "cycles": 1}, 2.0,
                           lambda *args: (1.0, {"digest": 1}))

    def test_identical_and_faster(self):
        self.assertTrue(gate.confidence([1.0] * 31)["pass"])
        self.assertTrue(gate.confidence([1.1] * 31)["pass"])

    def test_regression_is_not_hidden(self):
        self.assertFalse(gate.confidence([0.98] * 31)["pass"])
        self.assertFalse(gate.confidence([0.98] * 17 + [1.3] * 14)["pass"])

    def test_uncertainty_fails_closed(self):
        result = gate.confidence([0.97] * 15 + [1.02] * 16)
        self.assertGreater(result["median_speedup"], 1.0)
        self.assertFalse(result["pass"])

    def test_invalid_samples_rejected(self):
        for samples in ([1.0] * 14, [0.0] * 31, [-1.0] * 31,
                        [math.nan] * 31, [math.inf] * 31):
            with self.assertRaises(ValueError):
                gate.confidence(samples)

    def test_only_uncertainty_extends_once(self):
        uncertain = gate.confidence([0.97] * 25 + [1.02] * 26)
        self.assertTrue(gate.needs_extension(uncertain, 201))
        self.assertFalse(gate.needs_extension(uncertain, 51))
        self.assertFalse(gate.needs_extension(gate.confidence([0.98] * 51), 201))
        self.assertFalse(gate.needs_extension(gate.confidence([1.01] * 51), 201))

    def test_second_look_keeps_bad_initial_samples(self):
        # Discarding the initial batch would pass. The cumulative decision must fail.
        initial = [0.97] * 51
        additional = [0.97] * 55 + [1.02] * 95
        self.assertTrue(gate.confidence(additional)["pass"])
        cumulative = gate.confidence(initial + additional)
        self.assertFalse(cumulative["pass"])
        self.assertEqual(cumulative["sample_count"], 201)
        self.assertFalse(gate.needs_extension(cumulative, 201))

    def test_two_looks_share_false_acceptance_budget(self):
        result = gate.confidence([1.0] * 51)
        self.assertGreaterEqual(result["confidence_level"], 0.975)
        self.assertLessEqual(2 * (1 - result["confidence_level"]), 0.05 + 1e-12)

    def test_overflow_rejected(self):
        with self.assertRaises(ValueError):
            gate.parse({"kind": "scheduler"}, "overflow\n1\n")

    def test_missing_state_rejected(self):
        with self.assertRaises(ValueError):
            gate.parse({"kind": "representative"}, "RESULT median_seconds=1 mode=seq\n")

    def test_nonfinite_time_rejected(self):
        with self.assertRaises(ValueError):
            gate.parse({"kind": "floor"}, "cycles,wall_s,digest\n1,nan,42\n")


if __name__ == "__main__":
    unittest.main()
