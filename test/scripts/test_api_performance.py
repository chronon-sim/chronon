# SPDX-License-Identifier: MPL-2.0
"""Acceptance-gate failures must never become successful CI results."""

import importlib.util
import math
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "api_performance", Path(__file__).resolve().parents[2] / "scripts/check_api_performance.py")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class PerformanceAcceptance(unittest.TestCase):
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
