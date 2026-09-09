#!/usr/bin/env python3
"""Retain complete top-level packets from a crash-truncated Perfetto prefix.

Copies bytes verbatim, preserving sequence/clock/track identities, interning and
compression. A partially written compressed wrapper is discarded in full.
Does not repair corrupt middle packets or a trace missing its initial metadata.
"""
import argparse
from pathlib import Path


def varint(data, offset):
    value = 0
    for shift in range(0, 70, 7):
        if offset == len(data):
            return None
        byte = data[offset]
        offset += 1
        if shift == 63 and byte > 1:
            raise ValueError("invalid protobuf varint")
        value |= (byte & 127) << shift
        if not byte & 128:
            return value, offset
    raise ValueError("invalid protobuf varint")


def packet_boundaries(data):
    offset = 0
    boundaries = []
    while offset < len(data):
        tag = varint(data, offset)
        if tag is None:
            break
        if tag[0] != 10:
            raise ValueError("expected Trace.packet field; corrupt or unsupported file")
        length = varint(data, tag[1])
        if length is None or length[1] + length[0] > len(data):
            break
        offset = length[1] + length[0]
        boundaries.append(offset)
    return boundaries


def recover(data):
    boundaries = packet_boundaries(data)
    return data[:boundaries[-1]] if boundaries else b""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.input.resolve() == args.output.resolve():
        parser.error("output must differ from input")
    data = args.input.read_bytes()
    result = recover(data)
    with args.output.open("xb") as output:
        output.write(result)
    print(f"retained_bytes={len(result)} discarded_tail_bytes={len(data) - len(result)}")


if __name__ == "__main__":
    main()
