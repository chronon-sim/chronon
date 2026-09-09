#!/usr/bin/env python3
"""Import real multi-clock traces with Trace Processor and check semantic equivalence.

Requires the actual Trace Processor executable, not protobuf-only decoding.
Uses --full-sort because streams may differ by hundreds of simulated seconds.
"""
import argparse
import csv
from fractions import Fraction
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile

from merge_clock_logs import load_events
from recover_clock_trace import recover

FIELDS = ("ts", "unit", "local_cycle", "event", "phase", "transaction_id", "fifo_id", "value", "ordinal")
EVENT_SQL = """
SELECT s.ts, t.name AS unit,
       EXTRACT_ARG(s.arg_set_id, 'debug.local_cycle') AS local_cycle,
       s.name AS event,
       EXTRACT_ARG(s.arg_set_id, 'debug.phase') AS phase,
       EXTRACT_ARG(s.arg_set_id, 'debug.transaction_id') AS transaction_id,
       EXTRACT_ARG(s.arg_set_id, 'debug.fifo_id') AS fifo_id,
       EXTRACT_ARG(s.arg_set_id, 'debug.value') AS value,
       EXTRACT_ARG(s.arg_set_id, 'debug.ordinal') AS ordinal
FROM slice s JOIN track t ON t.id = s.track_id WHERE s.category = 'clock'
"""
FLOW_SQL = """
SELECT ot.name AS source_unit, EXTRACT_ARG(o.arg_set_id, 'debug.ordinal') AS source_ordinal,
       it.name AS target_unit, EXTRACT_ARG(i.arg_set_id, 'debug.ordinal') AS target_ordinal
FROM flow f JOIN slice o ON o.id = f.slice_out JOIN slice i ON i.id = f.slice_in
JOIN track ot ON ot.id = o.track_id JOIN track it ON it.id = i.track_id
WHERE o.category = 'clock' AND i.category = 'clock'
"""


def query(processor, trace, sql):
    result = subprocess.run([str(processor), "--full-sort", "query", str(trace), sql],
                            check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return list(csv.DictReader(io.StringIO(result.stdout)))


def normalize(row):
    # PerfettoSQL represents uint64 debug values using signed SQLite integers.
    return tuple(row[key] if key in ("unit", "event") else int(row[key]) & ((1 << 64) - 1) for key in FIELDS)


def identity(row):
    return row[1], row[-1]


def assert_unique(rows):
    result = {identity(row): row for row in rows}
    if len(result) != len(rows):
        raise AssertionError("duplicated unit/ordinal record identity")
    return result


def verify_import(processor, trace, expected, clocks, stream_domains, prefix=False):
    rows = [normalize(row) for row in query(processor, trace, EVENT_SQL)]
    actual = assert_unique(rows)
    if not prefix and actual != expected:
        missing = next((key for key in expected if actual.get(key) != expected[key]), None)
        raise AssertionError(f"{trace}: semantic mismatch at {missing}: actual={actual.get(missing)}, expected={expected.get(missing)}")
    for key, row in actual.items():
        if expected.get(key) != row:
            raise AssertionError(f"{trace}: incorrect imported time/identity at {key}: {row}")
    errors = query(processor, trace, "SELECT name, value FROM stats WHERE severity = 'error' AND value != 0")
    if errors:
        raise AssertionError(f"Trace Processor errors: {errors}")
    snapshots = query(processor, trace, "SELECT COUNT(*) AS count FROM clock_snapshot WHERE clock_id IN (64,65,11)")
    if rows and int(snapshots[0]["count"]) < 3:
        raise AssertionError("native sequence-clock snapshots missing")

    # Build the expected flow graph independently from stable transaction identities
    # and exact physical times, restricting it to the retained (possibly lossy) events.
    transactions = {}
    for row in rows:
        ts, unit, cycle, kind, phase, transaction, fifo, value, ordinal = row
        clock = clocks[stream_domains[unit]]
        exact = Fraction(clock["phase_num"], clock["phase_den"]) + cycle * Fraction(clock["period_num"], clock["period_den"])
        if not Fraction(ts, 1_000_000_000) <= exact < Fraction(ts + 1, 1_000_000_000):
            raise AssertionError(f"physical-time quantization mismatch: {row}")
        if transaction:
            transactions.setdefault(transaction, []).append((exact, phase, unit, ordinal))
    expected_flows = set()
    for events in transactions.values():
        events.sort()
        for source, target in zip(events, events[1:]):
            expected_flows.add((source[2], source[3], target[2], target[3]))
    flows = {(row["source_unit"], int(row["source_ordinal"]), row["target_unit"], int(row["target_ordinal"]))
             for row in query(processor, trace, FLOW_SQL)}
    if flows != expected_flows:
        raise AssertionError(f"{trace}: flow/causality mismatch: missing={list(expected_flows - flows)[:3]}, extra={list(flows - expected_flows)[:3]}")
    return actual, len(flows)


def verify_directory(processor, directory, check_prefix):
    manifest = json.loads((directory / "clock-manifest.json").read_text())
    clocks = {clock["id"]: clock for clock in manifest["domains"]}
    stream_domains = {stream["unit"]: stream["domain_id"] for stream in manifest["streams"]}
    stats = json.loads((directory / "clock-stats.json").read_text())
    with (directory / "reference.tsv").open() as file:
        reference = assert_unique([normalize(row) for row in csv.DictReader(file, delimiter="\t")])
    text = assert_unique([normalize(row) for row in load_events(directory)])
    for key, row in text.items():
        if reference.get(key) != row:
            raise AssertionError(f"text differs from independent reference at {key}")
    if len(text) + stats["dropped_events"] != len(reference):
        raise AssertionError("loss accounting mismatch")
    if manifest["lossless"] and text != reference:
        raise AssertionError("lossless text lost events")
    trace = directory / "timeline.pftrace"
    actual, flows = verify_import(processor, trace, text, clocks, stream_domains)
    if check_prefix:
        data = trace.read_bytes()
        for length in (len(data) // 2, len(data) - 7):
            with tempfile.TemporaryDirectory(prefix="chronon-trace-recovery-") as scratch:
                prefix = Path(scratch) / "recovered.pftrace"
                prefix.write_bytes(recover(data[:length]))
                retained, _ = verify_import(processor, prefix, actual, clocks, stream_domains, prefix=True)
                # A small lossy file can consist of ONE compressed wrapper;
                # tearing it legitimately leaves no complete events to recover.
                if not len(retained) < len(actual):
                    raise AssertionError("recovery did not retain a proper semantic prefix")
    digest = hashlib.sha256(repr(sorted(reference.values())).encode()).hexdigest()
    print(f"PASS {directory.name}: events={len(actual)} drops={stats['dropped_events']} flows={flows} reference_sha256={digest}")
    return digest


def verify_regressing(processor, path):
    rows = query(processor, path, "SELECT ts, EXTRACT_ARG(arg_set_id, 'debug.local_cycle') AS cycle FROM slice WHERE name = 'edge'")
    actual = {(int(row["cycle"]), int(row["ts"])) for row in rows}
    expected = {(n, int(Fraction(999, 1000) + Fraction(n * 1000, 1001))) for n in (1000, 1, 2000, 0, 3000)}
    if actual != expected:
        raise AssertionError(f"absolute fallback/checkpoint mismatch: {actual} vs {expected}")
    print("PASS regressing input normalized with incremental checkpoints")


def verify_writer_collisions(processor, path):
    rows = query(processor, path, """
        SELECT s.id, s.ts, t.name AS unit, s.name,
               EXTRACT_ARG(s.arg_set_id, 'debug.local_cycle') AS cycle,
               EXTRACT_ARG(s.arg_set_id, 'debug.transaction_id') AS transaction_id,
               EXTRACT_ARG(s.arg_set_id, 'debug.phase') AS phase,
               EXTRACT_ARG(s.arg_set_id, 'debug.label') AS label
        FROM slice s JOIN track t ON t.id = s.track_id WHERE s.category = 'clock'
    """)
    expected = set()
    for n in range(80):
        for source in (True, False):
            cycle = n * 4 + (1 if n & 1 else 0 if source else 3)
            expected.add((n, 'z-source' if source else 'a-target',
                          'write' if source else 'visible', cycle, 42 + n,
                          0 if n & 1 and source else 1,
                          'owned-source' if source else 'owned-target'))
    actual = {(int(r['ts']), r['unit'], r['name'], int(r['cycle']),
               int(r['transaction_id']), int(r['phase']), r['label']) for r in rows}
    if len(rows) != 160 or actual != expected:
        raise AssertionError(f'{path}: writer collision event/ownership mismatch')
    endpoints = {(int(r['transaction_id']), r['unit']): int(r['id']) for r in rows}
    expected_flows = {(endpoints[n + 42, 'z-source'], endpoints[n + 42, 'a-target']) for n in range(80)}
    flows = query(processor, path, 'SELECT slice_out, slice_in FROM flow')
    actual_flows = {(int(r['slice_out']), int(r['slice_in'])) for r in flows}
    errors = query(processor, path, "SELECT name, value FROM stats WHERE severity = 'error' AND value != 0")
    if len(flows) != 80 or actual_flows != expected_flows or errors:
        raise AssertionError(f'{path}: same-ns/same-instant flow direction mismatch: {errors}')
    print(f'PASS {path.name}: 80 same-ns flows, phase ordering, owned metadata, external merge passes')


def run(args, root):
    if args.multiclock_binary:
        subprocess.run([str(args.multiclock_binary), str(root / "fifo")], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if args.recorder_binary:
        subprocess.run([str(args.recorder_binary), str(root / "recorders")], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    directories = sorted({path.parent for path in root.rglob("reference.tsv")})
    if not directories:
        raise ValueError("no generated reference fixtures found")
    # Discover schemas before relying on version-dependent Trace Processor tables.
    first_trace = directories[0] / "timeline.pftrace"
    for table in ("slice", "track", "flow", "clock_snapshot", "stats"):
        query(args.trace_processor, first_trace, f"SELECT * FROM {table} LIMIT 0")
    digests = {}
    for directory in directories:
        digests[directory.name] = verify_directory(args.trace_processor, directory, args.check_prefix)
    for a, b in (("serial", "fallback"), ("serial", "lossy"),
                 ("workers-0", "workers-1"), ("workers-0", "workers-2"), ("workers-0", "workers-3")):
        if a in digests and b in digests and digests[a] != digests[b]:
            raise AssertionError(f"execution/recording configuration changed semantics: {a}, {b}")
    for name in ('collision-reverse', 'collision-batch1', 'collision-batch64', 'collision-lossy'):
        if name in digests and digests[name] != digests.get('collision-forward'):
            raise AssertionError(f'collision fixture changed hardware semantics: {name}')
    for path in root.rglob("regressing.pftrace"):
        verify_regressing(args.trace_processor, path)
    for path in root.rglob('writer-collision-*.pftrace'):
        verify_writer_collisions(args.trace_processor, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace-processor", required=True, type=Path)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--multiclock-binary", type=Path)
    parser.add_argument("--recorder-binary", type=Path)
    parser.add_argument("--check-prefix", action="store_true")
    args = parser.parse_args()
    if args.directory:
        run(args, args.directory)
    else:
        with tempfile.TemporaryDirectory(prefix="chronon-clock-validation-") as scratch:
            run(args, Path(scratch))


if __name__ == "__main__":
    main()
