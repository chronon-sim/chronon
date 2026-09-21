# SPDX-License-Identifier: MPL-2.0
"""Parallel execution must never turn incomplete or weaker evidence into a pass."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import collect_api_performance as collector
from check_api_performance import cases, executable


class ShardAcceptance(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.make_shards()

    def make_shards(self, identity=False, count=6):
        method = "binary-identity" if identity else "paired-bootstrap"
        for i in range(count):
            group = cases(2)[i::count]
            folder = self.root / f"api-performance-{i}"
            folder.mkdir(exist_ok=True)
            binaries = {executable(case): "binary-hash" for case in group}
            libraries = {name: {"/lib/library.so": "library-hash"} for name in binaries}
            meta = {"base_sha": "base", "head_sha": "head", "shard_index": i, "shard_count": count,
                    "workers": 2, "case_order": [case["name"] for case in group],
                    "repeats": 51, "max_repeats": 201, "minimum_speedup": 0.99,
                    "per_look_confidence": 0.975, "maximum_looks": 2,
                    "false_acceptance_budget": 0.05, "target_seconds": 2.0,
                    "acceptance_method": method,
                    "binaries": {v: dict(binaries) for v in ("baseline", "candidate")},
                    "runtime_identity": {v: dict(libraries) for v in ("baseline", "candidate")}}
            verdict = {"base_sha": "base", "head_sha": "head", "pass": True,
                       "complete_shard": True, "completed_cases": len(group),
                       "expected_cases": len(group), "acceptance_method": method}
            rows = [{"case": case, "pass": True, "acceptance_method": method,
                     "lower_speedup": 1.0, "sample_count": 0 if identity else 51,
                     "confidence_level": 0.975, "looks": [], "determinism_pairs": 2}
                    for case in group]
            for row in rows:
                name = row["case"]["name"]
                suffix = "identity-checks" if identity else "samples"
                pairs = [{"baseline": 1.0, "candidate": 1.0}] * (2 if identity else 51)
                (folder / f"{name}-{suffix}.json").write_text(json.dumps(pairs))
                if not identity:
                    row["looks"] = [{"sample_count": 51, "lower_speedup": 1.0, "pass": True}]
                    (folder / f"{name}-looks.json").write_text(json.dumps(row["looks"]))
            for name, value in (("metadata", meta), ("verdict", verdict), ("summary", rows)):
                (folder / f"{name}.json").write_text(json.dumps(value))

    def collect(self):
        return collector.collect(self.root, "base", "head", 6, 2)

    def change(self, name, mutate):
        path = self.root / "api-performance-0" / f"{name}.json"
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value))

    def test_complete_union_accepts_statistics_and_identity(self):
        for identity in (False, True):
            self.make_shards(identity)
            results, metadata = self.collect()
            self.assertEqual(len(results), 29)
            self.assertEqual(len(metadata), 6)
            self.assertEqual([row["case"] for row in results], cases(2))

    def test_missing_artifact_rejected(self):
        (self.root / "api-performance-5" / "verdict.json").unlink()
        with self.assertRaises(FileNotFoundError):
            self.collect()

    def test_prepare_identity_artifact_must_cover_the_entire_matrix(self):
        self.make_shards(identity=True, count=1)
        results, metadata = collector.collect(self.root, "base", "head", 1, 2)
        self.assertEqual(len(results), 29)
        self.assertEqual(len(metadata), 1)
        self.change("summary", lambda rows: rows.pop())
        with self.assertRaisesRegex(ValueError, "missing, duplicate"):
            collector.collect(self.root, "base", "head", 1, 2)

    def test_one_scenario_per_shard_still_requires_all_29_scenarios(self):
        self.make_shards(count=29)
        results, metadata = collector.collect(self.root, "base", "head", 29, 2)
        self.assertEqual(len(results), 29)
        self.assertEqual(len(metadata), 29)
        (self.root / "api-performance-28" / "verdict.json").unlink()
        with self.assertRaises(FileNotFoundError):
            collector.collect(self.root, "base", "head", 29, 2)

    def test_incomplete_or_failed_shard_rejected(self):
        for key in ("pass", "complete_shard"):
            self.make_shards()
            self.change("verdict", lambda value: value.update({key: False}))
            with self.assertRaisesRegex(ValueError, "failed or incomplete"):
                self.collect()

    def test_duplicate_case_rejected(self):
        self.change("summary", lambda rows: rows.__setitem__(1, rows[0]))
        with self.assertRaisesRegex(ValueError, "missing, duplicate"):
            self.collect()

    def test_changed_workload_rejected(self):
        self.change("summary", lambda rows: rows[0]["case"].update(profile="port"))
        with self.assertRaisesRegex(ValueError, "changed workload"):
            self.collect()

    def test_missing_samples_rejected(self):
        path = next((self.root / "api-performance-0").glob("*-samples.json"))
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.collect()

    def test_mixed_commits_rejected(self):
        self.change("metadata", lambda value: value.update(head_sha="other"))
        with self.assertRaisesRegex(ValueError, "commit mismatch"):
            self.collect()

    def test_changed_threshold_rejected(self):
        self.change("metadata", lambda value: value.update(minimum_speedup=0.98))
        with self.assertRaisesRegex(ValueError, "changed acceptance"):
            self.collect()

    def test_failure_cannot_hide_behind_successful_verdict(self):
        self.change("summary", lambda rows: rows[0].update(lower_speedup=0.989))
        with self.assertRaisesRegex(ValueError, "unproven non-regression"):
            self.collect()

    def test_identity_requires_both_executable_and_library_proof(self):
        for key in ("binaries", "runtime_identity"):
            self.make_shards(identity=True)
            self.change("metadata", lambda value: value[key].update(candidate={}))
            with self.assertRaisesRegex(ValueError, "identity not proven"):
                self.collect()

    def test_identity_requires_deterministic_checks(self):
        self.make_shards(identity=True)
        self.change("summary", lambda rows: rows[0].update(determinism_pairs=0))
        with self.assertRaisesRegex(ValueError, "missing identity checks"):
            self.collect()


if __name__ == "__main__":
    unittest.main()
