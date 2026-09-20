#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Accept only a complete, successful union of independently measured shards."""

import argparse
import json
from pathlib import Path

from check_api_performance import cases, executable, MIN_SPEEDUP, LOOK_CONFIDENCE


def require(condition, message):
    if not condition:
        raise ValueError(message)


def collect(root, base, head, count, workers):
    matrix = cases(workers)
    require(1 <= count <= len(matrix), "invalid shard count")
    summaries, metadata = {}, []
    for index in range(count):
        shard = root / f"api-performance-{index}"
        def read(name):
            return json.loads((shard / f"{name}.json").read_text())
        meta, verdict, results = read("metadata"), read("verdict"), read("summary")
        for record in (meta, verdict):
            require(record["base_sha"] == base and record["head_sha"] == head, "commit mismatch")
        require(meta["shard_index"] == index and meta["shard_count"] == count
                and meta["workers"] == workers, "shard identity mismatch")
        require(verdict["pass"] is True and verdict["complete_shard"] is True,
                "failed or incomplete shard")
        expected = [case["name"] for case in matrix[index::count]]
        require(meta["case_order"] == [row["case"]["name"] for row in results] == expected,
                "missing, duplicate or unexpected scenarios")
        shape = lambda case: {key: value for key, value in case.items() if key != "cycles"}
        require([shape(row["case"]) for row in results] ==
                [shape(case) for case in matrix[index::count]], "changed workload")
        require(verdict["completed_cases"] == verdict["expected_cases"] == len(expected),
                "incomplete scenario count")
        for key, value in {"repeats": 51, "max_repeats": 201, "minimum_speedup": MIN_SPEEDUP,
                           "per_look_confidence": LOOK_CONFIDENCE, "maximum_looks": 2,
                           "false_acceptance_budget": 0.05, "target_seconds": 2.0}.items():
            require(meta[key] == value, f"changed acceptance setting: {key}")
        method = meta["acceptance_method"]
        require(verdict["acceptance_method"] == method, "acceptance method mismatch")
        if method == "binary-identity":
            names = {executable(row["case"]) for row in results}
            binaries, libraries = meta["binaries"], meta["runtime_identity"]
            require(binaries["baseline"] == binaries["candidate"]
                    and set(binaries["baseline"]) == names, "executable identity not proven")
            require(libraries and libraries["baseline"] == libraries["candidate"]
                    and set(libraries["baseline"]) == names
                    and all(libraries["baseline"].values()), "library identity not proven")
        else:
            require(method == "paired-bootstrap", "unknown acceptance method")
        for row in results:
            require(row["pass"] is True and row["acceptance_method"] == method,
                    "failed scenario")
            suffix = "identity-checks" if method == "binary-identity" else "samples"
            pairs = read(f"{row['case']['name']}-{suffix}")
            if method == "binary-identity":
                require(len(pairs) == 2, "missing deterministic pairs")
                require(row["sample_count"] == 0 and row["determinism_pairs"] == 2
                        and row["looks"] == [], "missing identity checks")
            else:
                looks = read(f"{row['case']['name']}-looks")
                require(len(pairs) == row["sample_count"] and looks == row["looks"]
                        and len(looks) == (1 if len(pairs) == 51 else 2), "incomplete timing evidence")
                require(row["lower_speedup"] >= MIN_SPEEDUP and row["sample_count"] in (51, 201)
                        and row["confidence_level"] == LOOK_CONFIDENCE, "unproven non-regression")
            summaries[row["case"]["name"]] = dict(row, shard_index=index)
        metadata.append(meta)
    require(len(summaries) == len(matrix) == 29, "incomplete full matrix")
    return [summaries[case["name"]] for case in matrix], metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--shards", type=int, default=6)
    parser.add_argument("--workers", type=int, choices=(2, 4), default=2)
    args = parser.parse_args()
    results, shards = collect(args.root, args.base_sha, args.head_sha, args.shards, args.workers)
    args.output.mkdir(parents=True, exist_ok=True)
    verdict = {"pass": True, "complete_matrix": True, "completed_cases": len(results),
               "expected_cases": 29, "base_sha": args.base_sha, "head_sha": args.head_sha}
    for name, value in (("summary", results), ("metadata", {**verdict, "shards": shards}),
                        ("verdict", verdict)):
        (args.output / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
    print(f"PASS: all {len(results)} scenarios across {len(shards)} shards")


if __name__ == "__main__":
    main()
