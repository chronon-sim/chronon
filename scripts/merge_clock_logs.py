#!/usr/bin/env python3
"""Offline semantic merge of Chronon's per-domain logs (stdlib only).

This presentation order is NOT a hardware causality relation. Runtime recording
preserves per-stream order only. Exact Fraction arithmetic reconstructs physical
seconds before sorting; simulation does not pay this conversion/sorting cost.
"""
import argparse
import csv
from fractions import Fraction
import json
from pathlib import Path
import sys


def load_events(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "clock-manifest.json").read_text())
    if manifest["version"] != 1:
        raise ValueError("unsupported clock manifest version")
    clocks = {d["id"]: d for d in manifest["domains"]}
    streams = {s["unit_id"]: s for s in manifest["streams"]}
    events, last = [], {}
    for domain_id, clock in clocks.items():
        phase = Fraction(clock["phase_num"], clock["phase_den"])
        period = Fraction(clock["period_num"], clock["period_den"])
        path = directory / ("text-domain-" + clock["name"] + ".log")
        with path.open() as file:
            for line in file:
                if line.startswith("#"):
                    continue
                cycle, unit, name, event_phase, transaction, fifo, value, ordinal = line.rstrip("\n").split("\t")
                cycle, unit, event_phase, transaction, fifo, value, ordinal = map(
                    int, (cycle, unit, event_phase, transaction, fifo, value, ordinal))
                stream = streams[unit]
                if stream["domain_id"] != domain_id:
                    raise ValueError("record placed in the wrong domain file")
                if unit in last and (cycle < last[unit][0] or ordinal <= last[unit][1]):
                    raise ValueError(f"stream order violation at {path}: unit={unit}, ordinal={ordinal}")
                last[unit] = cycle, ordinal
                time = phase + cycle * period
                events.append(dict(time=time, ts=(time * 1_000_000_000).__floor__(),
                                   domain_id=domain_id, unit=stream["unit"], local_cycle=cycle,
                                   event=name, phase=event_phase, transaction_id=transaction,
                                   fifo_id=fifo, value=value, ordinal=ordinal))
    return events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    events = load_events(args.directory)
    events.sort(key=lambda e: (e["time"], e["phase"], e["domain_id"], e["unit"], e["ordinal"]))
    fields = ["time_num", "time_den", "ts", "domain_id", "unit", "local_cycle", "event",
              "phase", "transaction_id", "fifo_id", "value", "ordinal"]
    output = args.output.open("w") if args.output else sys.stdout
    try:
        writer = csv.DictWriter(output, fields, delimiter="\t")
        writer.writeheader()
        for event in events:
            event = event.copy()
            time = event.pop("time")
            writer.writerow(dict(time_num=time.numerator, time_den=time.denominator, **event))
    finally:
        if args.output:
            output.close()


if __name__ == "__main__":
    main()
