# Framework Tools

This directory contains command-line tools built with the Chronon framework.

## trace_reader

**Source:** `trace_reader.cpp`

Reads Chronon binary trace files (`.ctrace`) produced by the observability backend. It can show file metadata, dump events, filter events, and convert traces to text.

### Build

The tool is built by the `src/tools/CMakeLists.txt` target:

```bash
cmake --build build --target trace_reader
```

### Usage

```bash
trace_reader <command> [options] <file.ctrace>
```

Commands:

```text
info     Show trace file information
dump     Dump all events as text
filter   Filter events by criteria
convert  Convert to text format
```

Options:

```text
--cycles START-END  Filter by cycle range
--unit NAME         Filter by unit name
--limit N           Limit output to N events
-o, --output FILE   Output file (default: stdout)
-v, --verbose       Verbose output
-h, --help          Show help
```

### Examples

```bash
# Show metadata and registered formats/units
./build/src/tools/trace_reader info -v output/trace.ctrace

# Dump the first 100 events
./build/src/tools/trace_reader dump --limit 100 output/trace.ctrace

# Filter events from a cycle range and unit name
./build/src/tools/trace_reader filter --cycles 1000-2000 --unit fetch output/trace.ctrace

# Convert the trace to text
./build/src/tools/trace_reader convert output/trace.ctrace -o trace.log
```

### Input Format

`trace_reader` consumes Chronon's FlatBuffers-based binary trace format. See [`website/docs/guides/binary-trace-format.md`](../../website/docs/guides/binary-trace-format.md) for the format documentation.
