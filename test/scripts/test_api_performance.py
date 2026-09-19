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
