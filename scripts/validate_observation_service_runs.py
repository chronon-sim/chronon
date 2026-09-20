#!/usr/bin/env python3
"""Compare every completed A/B run, including final counters and trace records.

Only SAGE's workload.scenario path and clock-manifest run_id are normalized.
Trace text is compared by domain and complete record, independent of cross-stream
arrival order. This does not replace importing the binary trace with Perfetto.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def counter_rows(directory):
    files = list(directory.rglob("counters.csv"))
    if len(files) != 1:
        raise ValueError(f"expected one counters.csv: {directory}")
    with files[0].open() as stream:
        return list(csv.DictReader(stream))


def fingerprint(directory):
    metrics_path = directory / "model/metrics.json"
    if not metrics_path.exists():
        rows = counter_rows(directory)
        return dict(counters=digest(rows), counter_rows=len(rows),
                    counter_columns=len(rows[0]))
    metrics = json.loads(metrics_path.read_text())
    metrics["workload"].pop("scenario")
    result = dict(metrics=digest(metrics), buffers={})
    for path in sorted((directory / "model").glob("*.bin")):
        with path.open("rb") as stream:
            result["buffers"][path.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    result["clocks"] = {}
    for path in sorted(directory.rglob("clock-manifest.json")):
        manifest = json.loads(path.read_text())
        manifest.pop("run_id", None)
        capture = dict(manifest=digest(manifest), domains={})
        for log in sorted(path.parent.glob("text-domain-*.log")):
            records, previous = [], {}
            with log.open() as stream:
                for line in stream:
                    if line.startswith("#"):
                        continue
                    fields = line.rstrip("\n").split("\t")
                    cycle, unit, ordinal = int(fields[0]), int(fields[1]), int(fields[-1])
                    if unit in previous:
                        before_cycle, before_ordinal = previous[unit]
                        if cycle < before_cycle or ordinal <= before_ordinal:
                            raise ValueError(f"stream order violation: {log}, unit {unit}")
                    previous[unit] = cycle, ordinal
                    records.append(line)
            capture["domains"][log.name] = dict(records=len(records),
                                                sha256=digest(sorted(records)))
        result["clocks"][str(path.parent.relative_to(directory))] = capture
    if not result["clocks"]:
        raise ValueError(f"no clock capture: {directory}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("measurements", type=Path)
    args = parser.parse_args()
    rows = json.loads((args.measurements / "runs.json").read_text())
    reference, reference_paths, results = {}, {}, []
    failures = 0
    for row in rows:
        actual = fingerprint(Path(row["directory"]))
        expected = reference.setdefault(row["case"], actual)
        reference_path = reference_paths.setdefault(row["case"], Path(row["directory"]))
        differences = [key for key in actual if actual[key] != expected.get(key)]
        failures += bool(differences)
        result = dict(case=row["case"], mode=row["mode"],
                      repetition=row["repetition"], differences=differences,
                      reference_directory=str(reference_path), fingerprint=actual)
        if "counters" in differences:
            before = counter_rows(reference_path)
            after = counter_rows(Path(row["directory"]))
            result["counter_differences"] = [
                dict(row=i, cycle=a["cycle"], column=key, reference=a[key], actual=b.get(key),
                     final_row=i == len(before) - 1 and i == len(after) - 1)
                for i, (a, b) in enumerate(zip(before, after))
                for key in a if a[key] != b.get(key)]
        results.append(result)
    output = args.measurements / "correctness.json"
    output.write_text(json.dumps(dict(runs=results, mismatched_runs=failures), indent=2) + "\n")
    print(f"Compared {len(rows)} runs across {len(reference)} cases: {failures} mismatches; {output}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
