# SPDX-License-Identifier: MPL-2.0
"""Local parallelism must preserve CPU isolation, cleanup and complete evidence."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_api_performance as runner


class LocalPerformanceRunner(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_groups_never_overlap_and_respect_available_physical_cores(self):
        self.assertEqual(runner.cpu_groups(list(range(16)), 2, 8),
                         [[i, i + 1, i + 2] for i in range(0, 15, 3)])
        self.assertEqual(runner.cpu_groups([0, 2, 4, 6, 8], 2, 8), [[0, 2, 4]])
        self.assertEqual(runner.cpu_groups(list(range(16)), 2, 2), [[0, 1, 2], [3, 4, 5]])
        self.assertEqual(runner.cpu_groups(list(range(16)), 4, 8),
                         [list(range(i, i + 5)) for i in (0, 5, 10)])
        for cpus, workers, jobs in (([0, 2], 2, 8), ([0, 0, 2], 2, 8), ([0, 2, 4], 2, 0)):
            with self.assertRaises(ValueError):
                runner.cpu_groups(cpus, workers, jobs)

    def test_dynamic_queue_reuses_free_cpus_without_waiting_for_slow_case(self):
        cpus = runner.physical_cpus(limit=None)
        if len(cpus) < 6:
            self.skipTest("requires six physical CPUs")
        groups = runner.cpu_groups(cpus, 2, 2)
        worker = self.root / "worker.py"
        worker.write_text('''import json, os, sys, time
from pathlib import Path
root, index = Path(sys.argv[1]), int(sys.argv[2])
result = {"cpus": sorted(os.sched_getaffinity(0)), "start": time.monotonic()}
if index == 0:
    deadline = time.monotonic() + 5
    while not (root / "3.json").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
result["end"] = time.monotonic()
(root / f"{index}.json").write_text(json.dumps(result))
''')
        with redirect_stdout(io.StringIO()):
            statuses = runner.run_tasks(4, groups,
                lambda index, _: [sys.executable, str(worker), str(self.root), str(index)], 10)
        self.assertEqual(statuses, [0] * 4)
        records = [json.loads((self.root / f"{i}.json").read_text()) for i in range(4)]
        self.assertEqual(records[0]["cpus"], groups[0])
        for index in (1, 2, 3):
            self.assertEqual(records[index]["cpus"], groups[1])
        self.assertLess(records[3]["end"], records[0]["end"])
        self.assertLessEqual(records[1]["end"], records[2]["start"])

    def test_timeout_kills_checker_and_benchmark_children(self):
        cpus = runner.physical_cpus(limit=None)
        groups = runner.cpu_groups(cpus, 2, 1)
        worker = self.root / "hang.py"
        worker.write_text('''import os, signal, subprocess, sys, time
from pathlib import Path
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
Path(sys.argv[1]).write_text(f"{os.getpid()} {child.pid}")
time.sleep(60)
''')
        pids = self.root / "pids"
        with redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "wall-time budget exhausted"):
                runner.run_tasks(1, groups,
                    lambda *_: [sys.executable, str(worker), str(pids)], 1)
        for pid in map(int, pids.read_text().split()):
            stat = Path(f"/proc/{pid}/stat")
            if stat.exists():
                self.assertEqual(stat.read_text().split()[2], "Z")

    def test_failure_stops_queue_before_starting_another_scenario(self):
        cpus = runner.physical_cpus(limit=None)
        groups = runner.cpu_groups(cpus, 2, 1)
        commands = []
        def command(index, _):
            commands.append(index)
            return [sys.executable, "-c", "raise SystemExit(7)"]
        with redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "task 0 failed with exit 7"):
                runner.run_tasks(3, groups, command, 10)
        self.assertEqual(commands, [0])

    def invoke_main(self, statuses, collector_result=None):
        output = self.root / "results"
        argv = ["run_api_performance.py", "baseline", "candidate", str(output),
                "--base-sha", "base", "--head-sha", "head"]
        with (patch.object(runner.sys, "argv", argv),
              patch.object(runner, "physical_cpus", return_value=list(range(16))),
              patch.object(runner, "run_tasks", side_effect=statuses) as tasks,
              patch.object(runner, "collect", return_value=collector_result) as collect,
              redirect_stdout(io.StringIO())):
            status = runner.main()
        return status, tasks, collect, json.loads((output / "verdict.json").read_text())

    def test_identity_path_requires_collector_and_skips_timing(self):
        status, tasks, collect, verdict = self.invoke_main([[0]], ([{}] * 29, [{}]))
        self.assertEqual(status, 0)
        self.assertEqual(tasks.call_count, 1)
        self.assertEqual(collect.call_args.args[3:], (1, 2))
        self.assertTrue(verdict["pass"])

    def test_changed_runtimes_schedule_all_cases_then_collect(self):
        status, tasks, collect, verdict = self.invoke_main([[3], [0] * 29], ([{}] * 29, [{}] * 29))
        self.assertEqual(status, 0)
        self.assertEqual(tasks.call_args.args[0], 29)
        self.assertEqual(collect.call_args.args[3:], (29, 2))
        self.assertTrue(verdict["complete_matrix"])

    def test_failed_measurement_cannot_reach_collector_or_pass(self):
        status, _, collect, verdict = self.invoke_main([[3], RuntimeError("failed scenario")])
        self.assertEqual(status, 1)
        collect.assert_not_called()
        self.assertFalse(verdict["pass"])
        self.assertFalse(verdict["complete_matrix"])


if __name__ == "__main__":
    unittest.main()
