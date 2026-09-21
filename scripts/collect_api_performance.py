#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Require complete, state-equivalent measurements without a timing threshold."""

import json
import math

from check_api_performance import METHOD, cases


def require(condition, message):
    if not condition:
        raise ValueError(message)


def collect_shard(shard, base, head, index, count, workers, expected):
    """Validate the entire shard before exposing any of its measurements."""
    def read(name):
        return json.loads((shard / f"{name}.json").read_text())

    meta, verdict, results = read("metadata"), read("verdict"), read("summary")
    for record in (meta, verdict):
        require(record["base_sha"] == base and record["head_sha"] == head, "commit mismatch")
        require(record["measurement_method"] == METHOD
                and record["performance_enforced"] is False, "changed measurement policy")
    require(meta["shard_index"] == index and meta["shard_count"] == count
            and meta["workers"] == workers, "shard identity mismatch")
    require(meta["runs_per_variant"] == 1, "expected one run per variant")
    require(verdict["pass"] is True and verdict["complete_shard"] is True,
            "failed or incomplete shard")
    require([row["case"] for row in results] == expected, "missing, duplicate or changed scenarios")
    require(meta["case_order"] == [case["name"] for case in expected], "changed case order")
    require(verdict["completed_cases"] == verdict["expected_cases"] == len(expected),
            "incomplete scenario count")
    for row in results:
        runs = read(f"{row['case']['name']}-runs")
        require(runs == row["runs"] and set(runs) == {"baseline", "candidate"},
                "missing or inconsistent run evidence")
        for run in runs.values():
            require(all(isinstance(run[key], (int, float)) and not isinstance(run[key], bool)
                        and math.isfinite(run[key]) and run[key] > 0
                        for key in ("wall_seconds", "benchmark_seconds")), "invalid elapsed time")
        require(row["state_matches"] is True
                and runs["baseline"]["state"] == runs["candidate"]["state"], "state mismatch")
    return results, meta


def collect(root, base, head, count, workers, *, allow_incomplete=False):
    matrix = cases(workers)
    require(1 <= count <= len(matrix), "invalid shard count")
    summaries, metadata = {}, []
    for index in range(count):
        try:
            results, meta = collect_shard(root / f"api-performance-{index}", base, head,
                                          index, count, workers, matrix[index::count])
        except (OSError, ValueError, KeyError, TypeError):
            if not allow_incomplete:
                raise
            # Failed, unfinished or malformed shards are not completed evidence.
            continue
        summaries.update((row["case"]["name"], dict(row, shard_index=index)) for row in results)
        metadata.append(meta)
    if not allow_incomplete:
        require(len(summaries) == len(matrix) == 29, "incomplete full matrix")
    return [summaries[case["name"]] for case in matrix if case["name"] in summaries], metadata


def render_report(results, verdict, execution):
    """Use measured rows only; an incomplete run is never described as passing."""
    lines = ["<!-- chronon-api-performance -->", "## Performance wall time", "",
             f"Baseline `{verdict['base_sha']}` → PR `{verdict['head_sha']}`", "",
             "One run per revision and scenario; timing is informational and never blocks the PR.",
             "Wall time includes process startup, initialization, warmup, simulation and shutdown.",
             "Single measurements share host resources; no confidence interval is claimed.", "",
             f"Completed: **{len(results)}/{verdict['expected_cases']}** scenarios. "
             f"Correctness/completeness: **{'PASS' if verdict['pass'] else 'FAILED / INCOMPLETE'}**.",
             f"Up to {execution['concurrent_scenarios']} concurrent tasks, "
             f"{execution['workers']} CPUs per task (caller shares a worker CPU).", ""]
    if results:
        lines += ["| Scenario | Baseline wall (s) | PR wall (s) | Wall-time change | State |",
                  "| --- | ---: | ---: | ---: | --- |"]
        for row in results:
            base = row["runs"]["baseline"]["wall_seconds"]
            head = row["runs"]["candidate"]["wall_seconds"]
            lines.append(f"| {row['case']['name']} | {base:.6f} | {head:.6f} | "
                         f"{(head / base - 1) * 100:+.1f}% | Match |")
        lines += ["", "Positive change means slower. Raw logs, CPU allocation and simulation-only "
                  "timings are in the workflow artifact."]
    if verdict.get("error"):
        lines += ["", "The measurement did not complete; see the workflow logs for the failure."]
    return "\n".join(lines) + "\n"
