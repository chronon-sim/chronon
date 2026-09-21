# SPDX-License-Identifier: MPL-2.0
"""Complete and consistent evidence is required; a slowdown is still reportable."""
import json
from contextlib import redirect_stdout, redirect_stderr
import io
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import collect_api_performance as collector
import run_api_performance as runner
from check_api_performance import METHOD, cases


class ShardCollection(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for i, case in enumerate(cases(2)):
            folder = self.root / f"api-performance-{i}"
            folder.mkdir()
            common = {"base_sha": "base", "head_sha": "head", "measurement_method": METHOD,
                      "performance_enforced": False}
            meta = {**common, "shard_index": i, "shard_count": 29, "workers": 2,
                    "case_order": [case["name"]], "runs_per_variant": 1}
            verdict = {**common, "pass": True, "complete_shard": True,
                       "completed_cases": 1, "expected_cases": 1}
            runs = {variant: {"wall_seconds": seconds, "benchmark_seconds": seconds / 2,
                              "state": {"digest": "42"}}
                    for variant, seconds in (("baseline", 1.0), ("candidate", 2.0))}
            row = {"case": case, "runs": runs, "state_matches": True}
            for name, value in (("metadata", meta), ("verdict", verdict), ("summary", [row]),
                                (f"{case['name']}-runs", runs)):
                (folder / f"{name}.json").write_text(json.dumps(value))

    def collect(self):
        return collector.collect(self.root, "base", "head", 29, 2)

    def change(self, name, mutate):
        path = self.root / "api-performance-0" / f"{name}.json"
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value))

    def test_all_29_slow_scenarios_are_successful_measurements(self):
        results, meta = self.collect()
        self.assertEqual((len(results), len(meta)), (29, 29))
        self.assertEqual([r["case"] for r in results], cases(2))
        self.assertTrue(all(r["runs"]["candidate"]["wall_seconds"] == 2 for r in results))

    def test_missing_shard_rejected(self):
        (self.root / "api-performance-28" / "verdict.json").unlink()
        with self.assertRaises(FileNotFoundError):
            self.collect()

    def test_partial_collection_excludes_missing_failed_and_invalid_shards(self):
        (self.root / "api-performance-28" / "verdict.json").unlink()
        (self.root / "api-performance-27" / "summary.json").write_text('{"unfinished":')
        self.change("verdict", lambda value: value.update({"pass": False, "complete_shard": False}))
        results, metadata = collector.collect(self.root, "base", "head", 29, 2,
                                              allow_incomplete=True)
        self.assertEqual([row["shard_index"] for row in results], list(range(1, 27)))
        self.assertEqual(len(metadata), 26)
        with self.assertRaisesRegex(ValueError, "failed or incomplete"):
            self.collect()

    def test_failure_timeout_and_cancellation_reports_keep_completed_shards(self):
        (self.root / "api-performance-28" / "verdict.json").unlink()
        for error in (RuntimeError("late task failed"), RuntimeError("wall-time budget exhausted"),
                      KeyboardInterrupt("cancelled")):
            with self.subTest(error=str(error)), tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "results"
                argv = ["run_api_performance.py", "baseline", "candidate", str(output),
                        "--base-sha", "base", "--head-sha", "head"]

                def fail_after_completed_tasks(*args):
                    shutil.copytree(self.root, output / "shards", dirs_exist_ok=True)
                    raise error

                with (patch.object(runner.sys, "argv", argv),
                      patch.object(runner, "physical_cpus", return_value=list(range(16))),
                      patch.object(runner, "run_tasks", side_effect=fail_after_completed_tasks),
                      redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO())):
                    status = runner.main()
                self.assertEqual(status, 1)
                verdict = json.loads((output / "verdict.json").read_text())
                self.assertFalse(verdict["pass"])
                self.assertFalse(verdict["complete_matrix"])
                self.assertEqual(verdict["error"], str(error))
                report = (output / "report.md").read_text()
                self.assertIn("**28/29**", report)
                self.assertIn("FAILED / INCOMPLETE", report)
                self.assertIn("1.000000 | 2.000000 | +100.0%", report)
                self.assertNotIn("**PASS**", report)
                self.assertEqual(verdict["completed_cases"], 28)
                self.assertEqual(len(json.loads((output / "summary.json").read_text())), 28)

    def test_failed_or_incomplete_shard_rejected(self):
        self.change("verdict", lambda v: v.update(complete_shard=False))
        with self.assertRaisesRegex(ValueError, "failed or incomplete"):
            self.collect()

    def test_different_commits_rejected(self):
        self.change("metadata", lambda v: v.update(head_sha="other"))
        with self.assertRaisesRegex(ValueError, "commit mismatch"):
            self.collect()

    def test_changed_work_including_cycle_count_rejected(self):
        self.change("summary", lambda rows: rows[0]["case"].update(cycles=1))
        with self.assertRaisesRegex(ValueError, "changed scenarios"):
            self.collect()

    def test_missing_run_rejected(self):
        path = next((self.root / "api-performance-0").glob("*-runs.json"))
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.collect()

    def test_repeated_measurements_rejected(self):
        self.change("metadata", lambda v: v.update(runs_per_variant=51))
        with self.assertRaisesRegex(ValueError, "one run"):
            self.collect()

    def test_state_mismatch_cannot_hide_behind_success(self):
        self.change("summary", lambda rows: rows[0].update(state_matches=False))
        with self.assertRaisesRegex(ValueError, "state mismatch"):
            self.collect()

    def test_invalid_elapsed_time_rejected(self):
        name = cases(2)[0]["name"]
        for seconds in (0, -1, "1", True):
            with self.subTest(seconds=seconds):
                self.change(f"{name}-runs", lambda runs: runs["candidate"].update(wall_seconds=seconds))
                self.change("summary", lambda rows: rows[0]["runs"]["candidate"].update(wall_seconds=seconds))
                with self.assertRaisesRegex(ValueError, "elapsed"):
                    self.collect()

    def test_report_shows_wall_time_and_slowdown_without_threshold(self):
        results, _ = self.collect()
        verdict = {"base_sha": "base", "head_sha": "head", "pass": True, "expected_cases": 29}
        report = collector.render_report(results, verdict, {"concurrent_scenarios": 8, "workers": 2})
        self.assertIn("29/29", report)
        self.assertIn("**PASS**", report)
        self.assertIn("1.000000 | 2.000000 | +100.0%", report)
        self.assertIn("timing is informational", report)
        self.assertIn("8 concurrent tasks", report)
        self.assertIn("<!-- chronon-api-performance -->", report)
        partial = collector.render_report([], {**verdict, "pass": False, "error": "timeout"},
                                          {"concurrent_scenarios": 8, "workers": 2})
        self.assertIn("FAILED / INCOMPLETE", partial)
        self.assertNotIn("**PASS**", partial)


if __name__ == "__main__":
    unittest.main()
