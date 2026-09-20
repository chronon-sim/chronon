#!/usr/bin/env python3
"""Update or check the shared, compiled quickstart in README and the guide."""

import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if documentation is stale")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    source = (root / "examples/quickstart.cpp").read_text()
    code = source.split("// [quickstart]\n", 1)[1].split("// [/quickstart]", 1)[0].rstrip()
    begin, end = "<!-- quickstart:begin -->", "<!-- quickstart:end -->"
    block = f"{begin}\n```cpp\n{code}\n```\n{end}"
    stale = []
    for name in ("README.md", "website/docs/intro.md"):
        path = root / name
        text = path.read_text()
        if text.count(begin) != 1 or text.count(end) != 1:
            raise ValueError(f"{name}: expected one quickstart block")
        start, stop = text.index(begin), text.index(end) + len(end)
        updated = text[:start] + block + text[stop:]
        if updated != text:
            stale.append(name)
            if not args.check:
                path.write_text(updated)
    if stale and args.check:
        parser.exit(1, "Stale quickstart: " + ", ".join(stale) + "\nRun python3 scripts/sync_quickstart.py\n")


if __name__ == "__main__":
    main()
