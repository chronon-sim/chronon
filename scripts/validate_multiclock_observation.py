#!/usr/bin/env python3
"""Validate unified clock observation with a real Perfetto Trace Processor.

The C++ fixture compares hardware state with observation disabled. This checker
independently checks imported edge/pipeline timestamps, event payloads, text logs,
and every reset-on-snapshot counter interval across execution modes.
"""
import argparse
import csv
from fractions import Fraction
from pathlib import Path
import re
import subprocess
import tempfile

from validate_clock_traces import query

CASES = ("1-whole", "1-segmented", "3-whole", "3-segmented", "stop-resume")
UNIT = re.compile(r"(?:writer|reader)[0-2]$")
LOG = re.compile(
    r"\[domain=(\d+) edge=(\d+) time=(\d+)/(\d+)s\]\s+\[\s*DEBUG\]\s+"
    r"((?:writer|reader)[0-2]): domain edge (\d+) value (\d+)")
SLICE_SQL = """
SELECT id, ts, dur, track_id, name,
       EXTRACT_ARG(arg_set_id, 'debug.domain_id') AS domain_id,
       EXTRACT_ARG(arg_set_id, 'debug.local_cycle') AS local_cycle,
       EXTRACT_ARG(arg_set_id, 'debug.time_num') AS time_num,
       EXTRACT_ARG(arg_set_id, 'debug.time_den') AS time_den,
       EXTRACT_ARG(arg_set_id, 'debug.value') AS value,
       EXTRACT_ARG(arg_set_id, 'debug.label') AS label
FROM slice WHERE EXTRACT_ARG(arg_set_id, 'debug.domain_id') IS NOT NULL
"""


def edge(unit, cycle):
    if unit.startswith("writer"):
        return Fraction(cycle, 3_000_000_000)
    return Fraction(137, 1_000_000_000_000) + Fraction(cycle, 400_000_000)


def unit_for_track(tracks, track_id):
    seen = set()
    while track_id not in seen and track_id in tracks:
        seen.add(track_id)
        track = tracks[track_id]
        name = track["name"].translate(dict.fromkeys(map(ord, "\u200b\u200c\u200d\u2060")))
        if UNIT.fullmatch(name):
            return name
        track_id = int(track["parent_id"]) if track["parent_id"] != "[NULL]" else -1
    raise AssertionError(f"cannot resolve hardware unit for track {track_id}")


def imported(processor, directory):
    trace = directory / "timeline.pftrace"
    for table in ("slice", "track", "flow", "stats"):
        query(processor, trace, f"SELECT * FROM {table} LIMIT 0")
    errors = query(processor, trace,
                   "SELECT name,value FROM stats WHERE severity='error' AND value != 0")
    if errors:
        raise AssertionError(f"{directory}: import errors: {errors}")
    tracks = {int(row["id"]): row for row in
              query(processor, trace, "SELECT id,name,parent_id FROM track")}
    events, pipelines, identities = {}, {}, {}
    for row in query(processor, trace, SLICE_SQL):
        unit = unit_for_track(tracks, int(row["track_id"]))
        cycle = int(row["local_cycle"])
        key = unit, cycle
        expected = edge(unit, cycle)
        exact = Fraction(int(row["time_num"]), int(row["time_den"]))
        domain = 1 if unit.startswith("writer") else 2
        if exact != expected or int(row["domain_id"]) != domain:
            raise AssertionError(f"{directory}: incorrect exact time/domain at {key}: {row}")
        if int(row["ts"]) != int(expected * 1_000_000_000):
            raise AssertionError(f"{directory}: incorrect imported nanoseconds at {key}")
        if row["name"] == "edge":
            if key in events or row["label"] != "payload":
                raise AssertionError(f"{directory}: duplicate edge or damaged owned payload at {key}")
            events[key] = int(row["value"])
            kind = "edge"
        else:
            duration = int(edge(unit, cycle + 1) * 1_000_000_000) - int(expected * 1_000_000_000)
            if key in pipelines or int(row["dur"]) != duration:
                raise AssertionError(f"{directory}: pipeline edge span mismatch at {key}: {row}")
            pipelines[key] = duration
            kind = "pipeline"
        identities[int(row["id"])] = unit, cycle, kind
    expected_keys = {(f"{kind}{i}", n) for kind, count in (("writer", 300), ("reader", 40))
                     for i in range(3) for n in range(count)}
    if events.keys() != expected_keys or pipelines.keys() != expected_keys:
        raise AssertionError(f"{directory}: missing/extra events or pipeline slices")
    flows = set()
    for row in query(processor, trace, "SELECT slice_out,slice_in FROM flow"):
        source, target = int(row["slice_out"]), int(row["slice_in"])
        if source in identities and target in identities:
            flows.add((identities[source], identities[target]))
    if not flows:
        raise AssertionError(f"{directory}: unified event/pipeline flow IDs did not import")
    return events, pipelines, flows


def verify_logs(directory, events):
    seen = {}
    for path in directory.glob("*.log"):
        for line in path.read_text().splitlines():
            match = LOG.search(line)
            if not match:
                continue
            domain, cycle, numerator, denominator, unit, argument_cycle, value = match.groups()
            key = unit, int(cycle)
            if key in seen or int(cycle) != int(argument_cycle):
                raise AssertionError(f"{path}: duplicate/misattributed log {line}")
            if Fraction(int(numerator), int(denominator)) != edge(*key):
                raise AssertionError(f"{path}: incorrect exact log timestamp {line}")
            if int(domain) != (1 if unit.startswith("writer") else 2):
                raise AssertionError(f"{path}: incorrect log domain {line}")
            seen[key] = int(value)
    if seen != events:
        raise AssertionError(f"{directory}: logs differ from imported events")


def verify_counters(directory, events):
    with (directory / "counters.csv").open() as file:
        reader = csv.DictReader(file)
        fields = reader.fieldnames
        rows = list(reader)
    if fields[:3] != ["time_num", "time_den", "sample"]:
        raise AssertionError(f"{directory}: exact counter cutoff columns missing")
    units = sorted({unit for unit, _ in events})
    cursors = {unit: 0 for unit in units}
    previous, unique = Fraction(-1), set()
    periodic, finals = 0, 0
    for row in rows:
        cutoff = Fraction(int(row["time_num"]), int(row["time_den"]))
        kind = row["sample"]
        if (cutoff, kind) in unique or cutoff < previous or cutoff > Fraction(1, 10_000_000):
            raise AssertionError(f"{directory}: repeated/regressing/out-of-run counter cutoff {row}")
        unique.add((cutoff, kind))
        previous = cutoff
        inclusive = kind == "final_after"
        if kind == "periodic":
            periodic += 1
            if cutoff * 3_000_000_000 / 7 != periodic:
                raise AssertionError(f"{directory}: missing/off-grid reference-clock sample {row}")
        elif kind in ("final_before", "final_after"):
            finals += 1
        else:
            raise AssertionError(f"{directory}: ambiguous counter cutoff phase {kind}")
        for unit in units:
            start = cursors[unit]
            end = start
            while (unit, end) in events and (edge(unit, end) < cutoff or
                                            (inclusive and edge(unit, end) == cutoff)):
                end += 1
            previous_value = events[unit, start - 1] if start else 0
            value = events[unit, end - 1] if end else 0
            for name, expected in (("ticks", end - start), ("transfers", value - previous_value)):
                if int(row[f"{unit}.{name}"]) != expected:
                    raise AssertionError(f"{directory}: {unit}.{name} interval mismatch at {cutoff}: "
                                         f"actual={row[f'{unit}.{name}']} expected={expected}")
            cursors[unit] = end
    if periodic != 42 or finals < 1 or sum(cursors.values()) != len(events):
        raise AssertionError(f"{directory}: incomplete periodic/final counter coverage")


def validate(processor, root):
    baseline = None
    for case in CASES:
        traces = [trace for trace in (root / case).glob("*/timeline.pftrace")
                  if (trace.parent / "counters.csv").is_file()]
        # 'latest' is a symlink to the timestamped directory.
        traces = sorted({trace.resolve() for trace in traces})
        if len(traces) != 1:
            raise AssertionError(f"{root / case}: expected one complete trace")
        directory = traces[0].parent
        result = imported(processor, directory)
        if baseline is None:
            baseline = result
        elif result != baseline:
            raise AssertionError(f"{case}: imported semantics differ from sequential execution")
        verify_logs(directory, result[0])
        verify_counters(directory, result[0])
        print(f"PASS {case}: {len(result[0])} exact edge events, pipeline spans, flows, logs, counter intervals")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace-processor", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    if args.directory:
        if args.binary:
            subprocess.run([str(args.binary.resolve()), str(args.directory.resolve())], check=True)
        validate(args.trace_processor, args.directory)
    elif args.binary:
        with tempfile.TemporaryDirectory(prefix="chronon-unified-clock-import-") as scratch:
            root = Path(scratch) / "fixtures"
            subprocess.run([str(args.binary.resolve()), str(root)], check=True)
            validate(args.trace_processor, root)
    else:
        parser.error("provide --binary or an existing --directory")


if __name__ == "__main__":
    main()
